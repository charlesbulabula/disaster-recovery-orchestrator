"""
RegionHealthProber — probes primary and secondary region health via HTTP endpoints,
RDS replica lag (CloudWatch), and ECS service running counts. Publishes SNS
alerts when the primary region becomes unhealthy.
"""

import logging
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import httpx
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = 5
TCP_TIMEOUT = 5.0
RDS_LAG_THRESHOLD_SECONDS = 30.0
ECS_MIN_RUNNING_TASKS = 1


@dataclass
class ProbeTarget:
    name: str
    http_url: str | None = None
    tcp_host: str | None = None
    tcp_port: int | None = None
    expected_status: int = 200


@dataclass
class ProbeResult:
    target: str
    check_type: str  # HTTP | TCP | RDS_LAG | ECS
    healthy: bool
    latency_ms: float | None = None
    detail: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class HealthResult:
    primary_healthy: bool
    secondary_healthy: bool
    rds_lag_s: float | None
    checks: dict[str, ProbeResult] = field(default_factory=dict)
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def failover_warranted(self) -> bool:
        return not self.primary_healthy and self.secondary_healthy


class RegionHealthProber:
    """
    Probes AWS primary and secondary region health.
    Checks HTTP endpoints, RDS replica lag, and ECS service counts.
    Publishes SNS alert when the primary region is unhealthy.
    """

    def __init__(
        self,
        primary_region: str,
        secondary_region: str,
        config: dict,
    ):
        self.primary_region = primary_region
        self.secondary_region = secondary_region
        self.config = config

        self._primary_session = boto3.Session(region_name=primary_region)
        self._secondary_session = boto3.Session(region_name=secondary_region)
        self._sns = self._primary_session.client("sns")

    def check_endpoint(self, url: str, timeout: int = HTTP_TIMEOUT) -> ProbeResult:
        """HTTP GET check. Returns ProbeResult with passed=True on HTTP 2xx."""
        start = time.monotonic()
        try:
            with httpx.Client(timeout=float(timeout), follow_redirects=True) as client:
                response = client.get(url)
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            healthy = 200 <= response.status_code < 300
            return ProbeResult(
                target=url,
                check_type="HTTP",
                healthy=healthy,
                latency_ms=latency_ms,
                detail=f"HTTP {response.status_code}",
            )
        except httpx.TimeoutException:
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            return ProbeResult(
                target=url, check_type="HTTP", healthy=False,
                latency_ms=latency_ms, detail=f"Timeout after {timeout}s",
            )
        except httpx.RequestError as exc:
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            return ProbeResult(
                target=url, check_type="HTTP", healthy=False,
                latency_ms=latency_ms, detail=str(exc),
            )

    def check_rds_lag(self, db_id: str, region: str) -> ProbeResult:
        """
        Query CloudWatch ReplicaLag metric for an RDS instance.
        Returns unhealthy if lag > RDS_LAG_THRESHOLD_SECONDS.
        """
        session = (
            self._primary_session if region == self.primary_region
            else self._secondary_session
        )
        cw = session.client("cloudwatch")
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=5)

        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="ReplicaLag",
                Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_id}],
                StartTime=start,
                EndTime=end,
                Period=60,
                Statistics=["Maximum"],
            )
        except ClientError as exc:
            return ProbeResult(
                target=db_id, check_type="RDS_LAG", healthy=False, detail=str(exc)
            )

        datapoints = resp.get("Datapoints", [])
        if not datapoints:
            return ProbeResult(
                target=db_id,
                check_type="RDS_LAG",
                healthy=False,
                detail="No ReplicaLag datapoints in last 5 minutes",
            )

        max_lag = max(dp["Maximum"] for dp in datapoints)
        healthy = max_lag <= RDS_LAG_THRESHOLD_SECONDS
        logger.debug("RDS lag for %s: %.1fs (threshold=%.0fs)", db_id, max_lag, RDS_LAG_THRESHOLD_SECONDS)
        return ProbeResult(
            target=db_id,
            check_type="RDS_LAG",
            healthy=healthy,
            detail=f"ReplicaLag={max_lag:.1f}s (threshold={RDS_LAG_THRESHOLD_SECONDS}s)",
        )

    def check_ecs_service(
        self, cluster: str, service: str, region: str
    ) -> ProbeResult:
        """Verify that an ECS service has runningCount >= desiredCount."""
        session = (
            self._primary_session if region == self.primary_region
            else self._secondary_session
        )
        ecs = session.client("ecs")

        try:
            resp = ecs.describe_services(cluster=cluster, services=[service])
        except ClientError as exc:
            return ProbeResult(
                target=f"{cluster}/{service}", check_type="ECS", healthy=False, detail=str(exc)
            )

        services = resp.get("services", [])
        if not services:
            return ProbeResult(
                target=f"{cluster}/{service}",
                check_type="ECS",
                healthy=False,
                detail=f"Service '{service}' not found in cluster '{cluster}'",
            )

        svc = services[0]
        running = svc.get("runningCount", 0)
        desired = svc.get("desiredCount", 0)
        status = svc.get("status", "UNKNOWN")
        healthy = status == "ACTIVE" and running >= max(desired, ECS_MIN_RUNNING_TASKS)

        detail = f"status={status} running={running} desired={desired}"
        logger.debug("ECS check %s/%s: %s", cluster, service, detail)
        return ProbeResult(
            target=f"{cluster}/{service}",
            check_type="ECS",
            healthy=healthy,
            detail=detail,
        )

    def probe_all(self) -> HealthResult:
        """
        Run all configured health checks. Returns HealthResult with per-check
        results. Publishes SNS notification when primary is unhealthy.
        """
        checks: dict[str, ProbeResult] = {}

        # Primary endpoint checks
        for url in self.config.get("primary_endpoints", []):
            result = self.check_endpoint(url)
            checks[f"primary:endpoint:{url}"] = result

        # Secondary endpoint checks
        for url in self.config.get("secondary_endpoints", []):
            result = self.check_endpoint(url)
            checks[f"secondary:endpoint:{url}"] = result

        # RDS lag (primary region replica)
        rds_lag_s: float | None = None
        rds_db_id = self.config.get("rds_db_id")
        if rds_db_id:
            lag_result = self.check_rds_lag(rds_db_id, self.primary_region)
            checks[f"primary:rds_lag:{rds_db_id}"] = lag_result
            if lag_result.healthy or not lag_result.healthy:
                import re
                m = re.search(r"ReplicaLag=(\d+\.?\d*)s", lag_result.detail)
                if m:
                    rds_lag_s = float(m.group(1))

        # ECS service checks
        for ecs_cfg in self.config.get("ecs_services", []):
            region = ecs_cfg.get("region", self.primary_region)
            prefix = "primary" if region == self.primary_region else "secondary"
            result = self.check_ecs_service(
                cluster=ecs_cfg["cluster"],
                service=ecs_cfg["service"],
                region=region,
            )
            checks[f"{prefix}:ecs:{ecs_cfg['cluster']}/{ecs_cfg['service']}"] = result

        # Determine primary vs secondary health
        primary_checks = [v for k, v in checks.items() if k.startswith("primary:")]
        secondary_checks = [v for k, v in checks.items() if k.startswith("secondary:")]

        primary_healthy = all(c.healthy for c in primary_checks) if primary_checks else True
        secondary_healthy = all(c.healthy for c in secondary_checks) if secondary_checks else True

        result = HealthResult(
            primary_healthy=primary_healthy,
            secondary_healthy=secondary_healthy,
            rds_lag_s=rds_lag_s,
            checks=checks,
        )

        failed = [k for k, v in checks.items() if not v.healthy]
        logger.info(
            "Health probe: primary=%s secondary=%s failed_checks=%s",
            "OK" if primary_healthy else "FAIL",
            "OK" if secondary_healthy else "FAIL",
            failed or "none",
        )

        if not primary_healthy:
            self._publish_sns_alert(result)

        return result

    def _publish_sns_alert(self, result: HealthResult) -> None:
        sns_topic_arn = self.config.get("sns_topic_arn")
        if not sns_topic_arn:
            logger.warning("sns_topic_arn not configured; skipping SNS DR alert")
            return

        failed_checks = [
            f"  [{v.check_type}] {k}: {v.detail}"
            for k, v in result.checks.items()
            if not v.healthy
        ]
        message = (
            f"PRIMARY REGION UNHEALTHY: {self.primary_region}\n"
            f"Timestamp: {result.checked_at}\n"
            f"RDS Lag: {result.rds_lag_s}s\n"
            f"Secondary healthy: {result.secondary_healthy}\n"
            f"Failover warranted: {result.failover_warranted}\n\n"
            f"Failed checks:\n" + "\n".join(failed_checks or ["(none)"])
        )

        try:
            self._sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"DR Alert: {self.primary_region} primary region unhealthy",
                Message=message,
            )
            logger.info("SNS DR alert published to %s", sns_topic_arn)
        except ClientError as exc:
            logger.error("Failed to publish SNS DR alert: %s", exc)

# _r 20260604152308-8c6434e7
