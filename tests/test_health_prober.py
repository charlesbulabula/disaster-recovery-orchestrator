"""
pytest tests for the multi-region health prober.
Tests HTTP check success/failure, RDS lag threshold, SNS notification,
and mock boto3 / httpx.
"""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.health.prober import (
    ECS_MIN_RUNNING_TASKS,
    RDS_LAG_THRESHOLD_SECONDS,
    ProbeResult,
    ProbeTarget,
    RegionHealthStatus,
    probe_ecs_service,
    probe_http,
    probe_rds_lag,
    probe_tcp,
    probe_region,
    publish_failure_to_sns,
    trigger_dr_workflow,
)


# ---------------------------------------------------------------------------
# HTTP Probe Tests
# ---------------------------------------------------------------------------


class TestProbeHTTP:
    def test_http_success_returns_healthy(self):
        target = ProbeTarget(name="api", http_url="https://example.com/health", expected_status=200)
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.health.prober.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = probe_http(target)

        assert result.healthy is True
        assert result.check_type == "HTTP"
        assert result.latency_ms is not None
        assert result.latency_ms >= 0

    def test_http_wrong_status_returns_unhealthy(self):
        target = ProbeTarget(name="api", http_url="https://example.com/health", expected_status=200)
        mock_response = MagicMock()
        mock_response.status_code = 503

        with patch("src.health.prober.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = probe_http(target)

        assert result.healthy is False
        assert "503" in result.detail

    def test_http_connection_error_returns_unhealthy(self):
        import httpx
        target = ProbeTarget(name="api", http_url="https://unreachable.example.com/health")

        with patch("src.health.prober.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.side_effect = httpx.ConnectError("Connection refused")
            mock_client_cls.return_value = mock_client

            result = probe_http(target)

        assert result.healthy is False
        assert result.check_type == "HTTP"

    def test_http_probe_requires_http_url(self):
        target = ProbeTarget(name="no-url")
        with pytest.raises(AssertionError):
            probe_http(target)


# ---------------------------------------------------------------------------
# TCP Probe Tests
# ---------------------------------------------------------------------------


class TestProbeTCP:
    def test_tcp_success(self):
        target = ProbeTarget(name="db", tcp_host="127.0.0.1", tcp_port=5432)
        with patch("src.health.prober.socket.create_connection") as mock_conn:
            mock_sock = MagicMock()
            mock_conn.return_value = mock_sock
            result = probe_tcp(target)

        assert result.healthy is True
        assert result.check_type == "TCP"
        mock_sock.close.assert_called_once()

    def test_tcp_connection_refused(self):
        import socket
        target = ProbeTarget(name="db", tcp_host="127.0.0.1", tcp_port=9999)
        with patch("src.health.prober.socket.create_connection", side_effect=ConnectionRefusedError("refused")):
            result = probe_tcp(target)

        assert result.healthy is False
        assert result.check_type == "TCP"

    def test_tcp_timeout(self):
        import socket
        target = ProbeTarget(name="db", tcp_host="10.0.0.1", tcp_port=5432)
        with patch("src.health.prober.socket.create_connection", side_effect=socket.timeout("timed out")):
            result = probe_tcp(target)

        assert result.healthy is False

    def test_tcp_requires_host_and_port(self):
        target = ProbeTarget(name="no-tcp")
        with pytest.raises(AssertionError):
            probe_tcp(target)


# ---------------------------------------------------------------------------
# RDS Lag Probe Tests
# ---------------------------------------------------------------------------


class TestProbeRDSLag:
    def _mock_cw_client(self, lag_value: float) -> MagicMock:
        cw = MagicMock()
        now = datetime.now(timezone.utc)
        cw.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Maximum": lag_value, "Timestamp": now - timedelta(minutes=2)},
            ]
        }
        return cw

    def test_lag_below_threshold_is_healthy(self):
        cw = self._mock_cw_client(lag_value=5.0)
        result = probe_rds_lag(cw, "my-replica", "us-east-1", threshold_seconds=30.0)
        assert result.healthy is True
        assert result.check_type == "RDS_LAG"

    def test_lag_above_threshold_is_unhealthy(self):
        cw = self._mock_cw_client(lag_value=45.0)
        result = probe_rds_lag(cw, "my-replica", "us-east-1", threshold_seconds=30.0)
        assert result.healthy is False
        assert "45.0" in result.detail

    def test_lag_exactly_at_threshold_is_healthy(self):
        cw = self._mock_cw_client(lag_value=30.0)
        result = probe_rds_lag(cw, "my-replica", "us-east-1", threshold_seconds=30.0)
        assert result.healthy is True

    def test_no_datapoints_returns_unhealthy(self):
        cw = MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        result = probe_rds_lag(cw, "my-replica", "us-east-1")
        assert result.healthy is False
        assert "No ReplicaLag" in result.detail


# ---------------------------------------------------------------------------
# SNS Notification Tests
# ---------------------------------------------------------------------------


class TestPublishFailureToSNS:
    def test_publishes_to_sns_on_failure(self):
        sns = MagicMock()
        failed_result = ProbeResult(target="api", check_type="HTTP", healthy=False, detail="503")
        healthy_result = ProbeResult(target="db", check_type="TCP", healthy=True, detail="OK")
        status = RegionHealthStatus(
            region="us-east-1",
            healthy=False,
            results=[failed_result, healthy_result],
        )

        publish_failure_to_sns(sns, "arn:aws:sns:us-east-1:123:dr-alerts", status)

        sns.publish.assert_called_once()
        call_kwargs = sns.publish.call_args[1]
        assert "us-east-1" in call_kwargs["Subject"]
        assert "HTTP" in call_kwargs["Message"]
        assert "api" in call_kwargs["Message"]

    def test_sns_message_contains_failed_checks(self):
        sns = MagicMock()
        results = [
            ProbeResult(target="svc1", check_type="HTTP", healthy=False, detail="timeout"),
            ProbeResult(target="svc2", check_type="RDS_LAG", healthy=False, detail="lag=60s"),
        ]
        status = RegionHealthStatus(region="eu-west-1", healthy=False, results=results)

        publish_failure_to_sns(sns, "arn:aws:sns:eu-west-1:123:dr-alerts", status)

        message = sns.publish.call_args[1]["Message"]
        assert "svc1" in message
        assert "svc2" in message
        assert "timeout" in message


# ---------------------------------------------------------------------------
# Step Functions Trigger Tests
# ---------------------------------------------------------------------------


class TestTriggerDRWorkflow:
    def test_starts_step_functions_execution(self):
        sfn = MagicMock()
        sfn.start_execution.return_value = {"executionArn": "arn:aws:states:us-east-1:123:execution:DR:abc"}
        status = RegionHealthStatus(
            region="us-east-1",
            healthy=False,
            results=[ProbeResult(target="api", check_type="HTTP", healthy=False, detail="down")],
        )

        arn = trigger_dr_workflow(sfn, "arn:aws:states:us-east-1:123:stateMachine:DR", status)

        assert arn == "arn:aws:states:us-east-1:123:execution:DR:abc"
        call_args = sfn.start_execution.call_args[1]
        assert "failed_region" in json.loads(call_args["input"])
        assert json.loads(call_args["input"])["failed_region"] == "us-east-1"

# _r 20260530150003-bf3c155e
