"""
ECS Service Scaler for DR — scales up services in the secondary region, waits
for tasks to reach RUNNING state, updates ALB target groups, and verifies
health checks pass.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SCALE_POLL_INTERVAL = 15  # seconds
SCALE_TIMEOUT = 600  # 10 minutes
HEALTH_CHECK_GRACE_PERIOD = 60  # seconds before checking ALB health
ALB_HEALTHY_THRESHOLD = 0.8  # 80% of targets must be healthy


@dataclass
class ServiceScaleTarget:
    cluster: str
    service: str
    desired_count: int
    target_group_arn: str | None = None


@dataclass
class ScaleResult:
    service: str
    cluster: str
    desired_count: int
    running_count: int
    healthy_targets: int | None
    success: bool
    message: str


@dataclass
class ECSScalerReport:
    region: str
    results: list[ScaleResult] = field(default_factory=list)

    @property
    def all_healthy(self) -> bool:
        return all(r.success for r in self.results)


def _update_service_desired_count(
    ecs_client: Any,
    cluster: str,
    service: str,
    desired_count: int,
) -> None:
    ecs_client.update_service(
        cluster=cluster,
        service=service,
        desiredCount=desired_count,
    )
    logger.info("Set %s/%s desiredCount=%d", cluster, service, desired_count)


def _wait_for_running_tasks(
    ecs_client: Any,
    cluster: str,
    service: str,
    desired_count: int,
    timeout: int = SCALE_TIMEOUT,
) -> int:
    """
    Polls DescribeServices until runningCount >= desired_count or timeout.
    Returns the final running count.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = ecs_client.describe_services(cluster=cluster, services=[service])
        services = response.get("services", [])
        if not services:
            raise ValueError(f"Service {cluster}/{service} not found")

        svc = services[0]
        running = svc.get("runningCount", 0)
        pending = svc.get("pendingCount", 0)
        logger.debug("%s/%s: running=%d pending=%d desired=%d", cluster, service, running, pending, desired_count)

        if running >= desired_count:
            logger.info("%s/%s reached %d running tasks", cluster, service, running)
            return running

        # Check for deployment failures
        for deployment in svc.get("deployments", []):
            failed = deployment.get("failedTasks", 0)
            if failed > 0:
                logger.warning("%s/%s has %d failed tasks in deployment", cluster, service, failed)

        time.sleep(SCALE_POLL_INTERVAL)

    svc_info = ecs_client.describe_services(cluster=cluster, services=[service])
    final_running = svc_info["services"][0].get("runningCount", 0) if svc_info["services"] else 0
    logger.warning("%s/%s timed out: running=%d, desired=%d", cluster, service, final_running, desired_count)
    return final_running


def _check_alb_target_health(
    elbv2_client: Any,
    target_group_arn: str,
) -> tuple[int, int]:
    """Returns (healthy_count, total_count) for the target group."""
    response = elbv2_client.describe_target_health(TargetGroupArn=target_group_arn)
    targets = response.get("TargetHealthDescriptions", [])
    healthy = sum(1 for t in targets if t["TargetHealth"]["State"] == "healthy")
    return healthy, len(targets)


def _wait_for_alb_health(
    elbv2_client: Any,
    target_group_arn: str,
    min_healthy_fraction: float = ALB_HEALTHY_THRESHOLD,
    timeout: int = 300,
    grace_period: int = HEALTH_CHECK_GRACE_PERIOD,
) -> tuple[bool, int]:
    """
    Waits for ALB target group to reach min_healthy_fraction healthy targets.
    Returns (success, healthy_count).
    """
    logger.info("Waiting %ds grace period before ALB health check...", grace_period)
    time.sleep(grace_period)

    deadline = time.time() + timeout
    while time.time() < deadline:
        healthy, total = _check_alb_target_health(elbv2_client, target_group_arn)
        if total == 0:
            logger.debug("No targets registered in %s yet", target_group_arn)
            time.sleep(SCALE_POLL_INTERVAL)
            continue

        fraction = healthy / total
        logger.debug("ALB %s: %d/%d healthy (%.0f%%)", target_group_arn, healthy, total, fraction * 100)

        if fraction >= min_healthy_fraction:
            logger.info("ALB target group healthy: %d/%d (%.0f%%)", healthy, total, fraction * 100)
            return True, healthy

        time.sleep(SCALE_POLL_INTERVAL)

    healthy, total = _check_alb_target_health(elbv2_client, target_group_arn)
    logger.warning("ALB health check timed out: %d/%d healthy", healthy, total)
    return False, healthy


def scale_service(
    ecs_client: Any,
    elbv2_client: Any,
    target: ServiceScaleTarget,
) -> ScaleResult:
    """Scales a single ECS service and verifies ALB health if a target group is provided."""
    try:
        _update_service_desired_count(ecs_client, target.cluster, target.service, target.desired_count)
    except ClientError as exc:
        return ScaleResult(
            service=target.service,
            cluster=target.cluster,
            desired_count=target.desired_count,
            running_count=0,
            healthy_targets=None,
            success=False,
            message=f"Failed to update service: {exc}",
        )

    running = _wait_for_running_tasks(ecs_client, target.cluster, target.service, target.desired_count)
    tasks_ok = running >= target.desired_count

    healthy_targets: int | None = None
    alb_ok = True
    if target.target_group_arn:
        alb_ok, healthy_targets = _wait_for_alb_health(elbv2_client, target.target_group_arn)

    success = tasks_ok and alb_ok
    message_parts = [f"running={running}/{target.desired_count}"]
    if healthy_targets is not None:
        message_parts.append(f"alb_healthy={healthy_targets}")

    return ScaleResult(
        service=target.service,
        cluster=target.cluster,
        desired_count=target.desired_count,
        running_count=running,
        healthy_targets=healthy_targets,
        success=success,
        message=", ".join(message_parts),
    )


def scale_dr_services(
    targets: list[ServiceScaleTarget],
    region: str,
    session: boto3.Session | None = None,
    dry_run: bool = False,
) -> ECSScalerReport:
    """Scales all ECS services in the DR region and returns a consolidated report."""
    if session is None:
        session = boto3.Session()

    ecs = session.client("ecs", region_name=region)
    elbv2 = session.client("elbv2", region_name=region)
    report = ECSScalerReport(region=region)

    for target in targets:
        if dry_run:
            logger.info("[DRY RUN] Would scale %s/%s to %d tasks", target.cluster, target.service, target.desired_count)
            report.results.append(ScaleResult(
                service=target.service,
                cluster=target.cluster,
                desired_count=target.desired_count,
                running_count=0,
                healthy_targets=None,
                success=True,
                message="DRY RUN",
            ))
            continue

        result = scale_service(ecs, elbv2, target)
        report.results.append(result)
        log_fn = logger.info if result.success else logger.error
        log_fn("Scale result for %s/%s: %s (success=%s)", target.cluster, target.service, result.message, result.success)

    logger.info("DR scaling complete: %d/%d services healthy", sum(r.success for r in report.results), len(report.results))
    return report

# _r 20260608105903-eedb98b1
