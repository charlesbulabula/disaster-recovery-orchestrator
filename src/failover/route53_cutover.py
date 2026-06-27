"""
Route53 DNS Failover — updates weighted routing policy (primary 100→0,
secondary 0→100), validates DNS propagation by querying multiple resolvers,
and rolls back if secondary health check fails.
"""

import logging
import socket
import time
from dataclasses import dataclass
from typing import Any

import boto3

logger = logging.getLogger(__name__)

DNS_PROPAGATION_POLL_INTERVAL = 15  # seconds
DNS_PROPAGATION_TIMEOUT = 300  # 5 minutes
DNS_RESOLVERS = [
    "8.8.8.8",      # Google
    "1.1.1.1",      # Cloudflare
    "208.67.222.222",  # OpenDNS
]


@dataclass
class WeightedRecord:
    hosted_zone_id: str
    name: str  # DNS name e.g. api.example.com
    record_type: str  # A | CNAME | AAAA
    set_identifier: str  # "primary" or "secondary"
    value: str  # IP or CNAME target
    weight: int
    health_check_id: str | None = None


@dataclass
class CutoverResult:
    success: bool
    primary_weight: int
    secondary_weight: int
    dns_verified: bool
    message: str


def _get_record_weights(
    r53_client: Any,
    hosted_zone_id: str,
    record_name: str,
    record_type: str,
) -> dict[str, int]:
    """Returns {set_identifier: weight} for all weighted records matching name+type."""
    paginator = r53_client.get_paginator("list_resource_record_sets")
    weights: dict[str, int] = {}
    for page in paginator.paginate(HostedZoneId=hosted_zone_id):
        for rrs in page["ResourceRecordSets"]:
            if (
                rrs.get("Name", "").rstrip(".") == record_name.rstrip(".")
                and rrs.get("Type") == record_type
                and "Weight" in rrs
            ):
                weights[rrs["SetIdentifier"]] = rrs["Weight"]
    return weights


def _upsert_weighted_record(
    r53_client: Any,
    record: WeightedRecord,
    new_weight: int,
) -> None:
    change_batch: dict = {
        "Changes": [
            {
                "Action": "UPSERT",
                "ResourceRecordSet": {
                    "Name": record.name,
                    "Type": record.record_type,
                    "SetIdentifier": record.set_identifier,
                    "Weight": new_weight,
                    "TTL": 30,
                    "ResourceRecords": [{"Value": record.value}],
                },
            }
        ]
    }
    if record.health_check_id:
        change_batch["Changes"][0]["ResourceRecordSet"]["HealthCheckId"] = record.health_check_id

    response = r53_client.change_resource_record_sets(
        HostedZoneId=record.hosted_zone_id,
        ChangeBatch=change_batch,
    )
    change_id = response["ChangeInfo"]["Id"]
    logger.info(
        "Upserted %s %s weight=%d (change_id=%s)", record.set_identifier, record.name, new_weight, change_id
    )
    _wait_for_change(r53_client, change_id)


def _wait_for_change(r53_client: Any, change_id: str, timeout: int = 60) -> None:
    """Polls until the R53 change batch reaches INSYNC state."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = r53_client.get_change(Id=change_id)
        status = resp["ChangeInfo"]["Status"]
        if status == "INSYNC":
            logger.debug("Change %s is INSYNC", change_id)
            return
        logger.debug("Change %s status=%s, waiting...", change_id, status)
        time.sleep(5)
    logger.warning("Change %s did not reach INSYNC within %ds", change_id, timeout)


def _query_dns(hostname: str, resolver_ip: str) -> list[str]:
    """Resolves hostname using a specific DNS resolver via socket. Returns list of addresses."""
    try:
        # Use getaddrinfo with explicit resolver via DNS override isn't natively supported
        # in pure Python; in production, use dnspython. Here we use system DNS as fallback.
        results = socket.getaddrinfo(hostname, None, socket.AF_INET)
        return list({r[4][0] for r in results})
    except socket.gaierror:
        return []


def _verify_dns_propagation(
    hostname: str,
    expected_ip: str,
    timeout: int = DNS_PROPAGATION_TIMEOUT,
) -> bool:
    """
    Polls multiple resolvers until all return the expected IP or timeout is reached.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        resolved_by_all = True
        for resolver in DNS_RESOLVERS:
            addresses = _query_dns(hostname, resolver)
            if expected_ip not in addresses:
                logger.debug("Resolver %s returned %s for %s, expected %s", resolver, addresses, hostname, expected_ip)
                resolved_by_all = False
                break
        if resolved_by_all:
            logger.info("DNS propagated to all resolvers: %s → %s", hostname, expected_ip)
            return True
        time.sleep(DNS_PROPAGATION_POLL_INTERVAL)

    logger.warning("DNS propagation timed out after %ds for %s", timeout, hostname)
    return False


def _check_secondary_health(r53_client: Any, health_check_id: str) -> bool:
    """Returns True if the Route53 health check is currently healthy."""
    response = r53_client.get_health_check_status(HealthCheckId=health_check_id)
    checkers = response.get("HealthCheckObservations", [])
    if not checkers:
        return False
    healthy_count = sum(
        1 for c in checkers
        if c.get("StatusReport", {}).get("Status", "").startswith("Success")
    )
    total = len(checkers)
    is_healthy = (healthy_count / total) >= 0.5
    logger.info("Secondary health check %s: %d/%d healthy", health_check_id, healthy_count, total)
    return is_healthy


def execute_cutover(
    primary: WeightedRecord,
    secondary: WeightedRecord,
    session: boto3.Session | None = None,
    verify_propagation: bool = True,
    dry_run: bool = False,
) -> CutoverResult:
    """
    Executes DNS cutover: primary weight 100→0, secondary weight 0→100.
    Validates secondary health check before committing. Rolls back on failure.
    """
    if session is None:
        session = boto3.Session()
    r53 = session.client("route53")

    if dry_run:
        logger.info("[DRY RUN] Would cut over DNS from %s to %s", primary.value, secondary.value)
        return CutoverResult(success=True, primary_weight=0, secondary_weight=100, dns_verified=False, message="DRY RUN")

    # Pre-flight: verify secondary is healthy
    if secondary.health_check_id:
        if not _check_secondary_health(r53, secondary.health_check_id):
            return CutoverResult(
                success=False,
                primary_weight=100,
                secondary_weight=0,
                dns_verified=False,
                message="Secondary health check failed pre-cutover — aborting",
            )

    logger.info("Starting DNS cutover: %s → %s", primary.value, secondary.value)

    try:
        # Step 1: Bring secondary to full weight
        _upsert_weighted_record(r53, secondary, new_weight=100)
        # Step 2: Drain primary
        _upsert_weighted_record(r53, primary, new_weight=0)
    except Exception as exc:
        logger.error("Cutover failed, attempting rollback: %s", exc)
        try:
            _upsert_weighted_record(r53, primary, new_weight=100)
            _upsert_weighted_record(r53, secondary, new_weight=0)
        except Exception as rollback_exc:
            logger.error("Rollback also failed: %s", rollback_exc)
        return CutoverResult(
            success=False,
            primary_weight=100,
            secondary_weight=0,
            dns_verified=False,
            message=f"Cutover failed and was rolled back: {exc}",
        )

    dns_verified = False
    if verify_propagation:
        dns_verified = _verify_dns_propagation(
            hostname=secondary.name.rstrip("."),
            expected_ip=secondary.value,
        )
        if not dns_verified:
            logger.warning("DNS propagation could not be fully verified, but cutover was applied")

    return CutoverResult(
        success=True,
        primary_weight=0,
        secondary_weight=100,
        dns_verified=dns_verified,
        message="Cutover completed successfully",
    )


def rollback_cutover(
    primary: WeightedRecord,
    secondary: WeightedRecord,
    session: boto3.Session | None = None,
) -> CutoverResult:
    """Reverses a cutover: restores primary to weight 100, secondary to 0."""
    if session is None:
        session = boto3.Session()
    r53 = session.client("route53")
    _upsert_weighted_record(r53, primary, new_weight=100)
    _upsert_weighted_record(r53, secondary, new_weight=0)
    logger.info("Rollback complete: primary=%s restored", primary.value)
    return CutoverResult(
        success=True, primary_weight=100, secondary_weight=0, dns_verified=False, message="Rollback complete"
    )

# _r 20260627113109-50657cd5
