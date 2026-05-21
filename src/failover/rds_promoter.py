"""
RDS Read Replica Promoter — stops replication on a read replica, promotes it
to a standalone DB instance, updates Parameter Store with the new endpoint,
and triggers application config reload via SSM Run Command.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

PROMOTE_POLL_INTERVAL = 20  # seconds
PROMOTE_TIMEOUT = 1800  # 30 minutes
AVAILABLE_STATUS = "available"


@dataclass
class PromotionResult:
    db_instance_id: str
    new_endpoint: str
    new_port: int
    ssm_parameter_updated: bool
    ssm_run_command_id: str | None
    success: bool
    message: str


def _wait_for_db_status(
    rds_client: Any,
    db_instance_id: str,
    target_status: str = AVAILABLE_STATUS,
    timeout: int = PROMOTE_TIMEOUT,
) -> dict:
    """Polls DescribeDBInstances until the instance reaches target_status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        instance = response["DBInstances"][0]
        status = instance["DBInstanceStatus"]
        logger.debug("DB %s status: %s", db_instance_id, status)
        if status == target_status:
            return instance
        if status in ("failed", "incompatible-parameters", "incompatible-restore"):
            raise RuntimeError(f"DB {db_instance_id} entered terminal status: {status}")
        time.sleep(PROMOTE_POLL_INTERVAL)
    raise TimeoutError(f"DB {db_instance_id} did not reach '{target_status}' within {timeout}s")


def _get_replica_source(rds_client: Any, db_instance_id: str) -> str | None:
    """Returns the source DB identifier if the instance is a read replica."""
    response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)
    instance = response["DBInstances"][0]
    source = instance.get("ReadReplicaSourceDBInstanceIdentifier")
    return source


def promote_read_replica(
    rds_client: Any,
    db_instance_id: str,
    backup_retention_period: int = 7,
) -> dict:
    """
    Promotes a read replica to a standalone DB instance.
    Returns the updated DB instance description.
    """
    source = _get_replica_source(rds_client, db_instance_id)
    if not source:
        logger.info("DB %s is not a read replica or already promoted", db_instance_id)
    else:
        logger.info("Promoting read replica %s (source: %s)", db_instance_id, source)

    try:
        rds_client.promote_read_replica(
            DBInstanceIdentifier=db_instance_id,
            BackupRetentionPeriod=backup_retention_period,
        )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "InvalidDBInstanceState":
            logger.info("DB %s is already promoted or not a replica: %s", db_instance_id, exc)
        else:
            raise

    logger.info("Waiting for %s to become available after promotion...", db_instance_id)
    return _wait_for_db_status(rds_client, db_instance_id, AVAILABLE_STATUS)


def update_ssm_parameter(
    ssm_client: Any,
    parameter_name: str,
    endpoint: str,
    port: int,
) -> None:
    """Updates the Parameter Store entry with the new DB endpoint."""
    import json
    value = json.dumps({"host": endpoint, "port": port})
    ssm_client.put_parameter(
        Name=parameter_name,
        Value=value,
        Type="SecureString",
        Overwrite=True,
        Description="Auto-updated by DR RDS promoter",
    )
    logger.info("Updated SSM parameter %s with endpoint %s:%d", parameter_name, endpoint, port)


def trigger_config_reload(
    ssm_client: Any,
    instance_ids: list[str],
    reload_command: str,
    region: str,
) -> str | None:
    """
    Sends an SSM Run Command to trigger application config reload on EC2 instances.
    Returns the command invocation ID.
    """
    if not instance_ids:
        logger.warning("No EC2 instance IDs provided for config reload")
        return None

    response = ssm_client.send_command(
        InstanceIds=instance_ids,
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [reload_command]},
        Comment="DR: trigger application config reload after RDS promotion",
        TimeoutSeconds=120,
    )
    command_id = response["Command"]["CommandId"]
    logger.info("SSM Run Command sent (id=%s) to %d instances", command_id, len(instance_ids))
    return command_id


def _wait_for_ssm_command(ssm_client: Any, command_id: str, instance_ids: list[str], timeout: int = 180) -> bool:
    """Waits for SSM command to complete on all instances. Returns True if all succeeded."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_done = True
        all_success = True
        for instance_id in instance_ids:
            try:
                inv = ssm_client.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
                status = inv["Status"]
                if status in ("Pending", "InProgress", "Delayed"):
                    all_done = False
                elif status != "Success":
                    logger.warning("SSM command failed on %s: %s", instance_id, inv.get("StatusDetails"))
                    all_success = False
            except ClientError:
                all_done = False
        if all_done:
            return all_success
        time.sleep(10)
    logger.warning("SSM command %s timed out", command_id)
    return False


def execute_rds_promotion(
    db_instance_id: str,
    ssm_parameter_name: str,
    app_instance_ids: list[str],
    reload_command: str = "sudo systemctl reload myapp || sudo kill -HUP $(pidof myapp)",
    region: str = "us-east-1",
    session: boto3.Session | None = None,
    dry_run: bool = False,
) -> PromotionResult:
    """
    Full RDS promotion workflow:
    1. Promote read replica to standalone
    2. Update SSM Parameter Store with new endpoint
    3. Trigger application config reload via SSM Run Command
    """
    if session is None:
        session = boto3.Session()

    rds = session.client("rds", region_name=region)
    ssm = session.client("ssm", region_name=region)

    if dry_run:
        logger.info("[DRY RUN] Would promote %s and update %s", db_instance_id, ssm_parameter_name)
        return PromotionResult(
            db_instance_id=db_instance_id,
            new_endpoint="dry-run.endpoint.example.com",
            new_port=5432,
            ssm_parameter_updated=False,
            ssm_run_command_id=None,
            success=True,
            message="DRY RUN",
        )

    try:
        db_info = promote_read_replica(rds, db_instance_id)
    except Exception as exc:
        return PromotionResult(
            db_instance_id=db_instance_id,
            new_endpoint="",
            new_port=0,
            ssm_parameter_updated=False,
            ssm_run_command_id=None,
            success=False,
            message=f"Promotion failed: {exc}",
        )

    endpoint = db_info["Endpoint"]["Address"]
    port = db_info["Endpoint"]["Port"]

    update_ssm_parameter(ssm, ssm_parameter_name, endpoint, port)
    ssm_updated = True

    command_id = trigger_config_reload(ssm, app_instance_ids, reload_command, region)
    if command_id and app_instance_ids:
        _wait_for_ssm_command(ssm, command_id, app_instance_ids)

    return PromotionResult(
        db_instance_id=db_instance_id,
        new_endpoint=endpoint,
        new_port=port,
        ssm_parameter_updated=ssm_updated,
        ssm_run_command_id=command_id,
        success=True,
        message=f"Promoted to {endpoint}:{port}",
    )

# _r 20260521101813-a2aadbe0
