from __future__ import annotations

"""Tests for Alert Manager security functionality (aligned with implementation)."""

import os
import sys
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from security.alert_manager import (
    Alert,
    AlertChannel,
    AlertLevel,
    AlertManager,
    AlertThrottler,
    FailureMode,
    SystemHealthMonitor,
    alert_manager,
    send_aws_connection_failure_alert,
    send_memory_exhaustion_alert,
    send_pii_detection_failure_alert,
    send_security_breach_alert,
)


class TestAlertManager:
    """Test cases for Alert Manager functionality."""

    @pytest.fixture
    def alert_manager_instance(self):
        """Create AlertManager instance for testing."""
        return AlertManager()

    def test_alert_dataclass(self):
        now = datetime.now()
        alert = Alert(
            id="a1",
            level=AlertLevel.ERROR,
            failure_mode=FailureMode.AWS_CONNECTION_FAILURE,
            message="Test",
            source="unit",
            timestamp=now,
        )
        assert alert.id == "a1"
        assert alert.level == AlertLevel.ERROR
        assert alert.failure_mode == FailureMode.AWS_CONNECTION_FAILURE
        assert alert.source == "unit"
        assert alert.timestamp == now

    def test_channel_config(self):
        ch = AlertChannel(
            name="webhook1",
            type="webhook",
            config={"url": "https://example.test/webhook"},
            enabled=True,
            min_level=AlertLevel.WARNING,
        )
        assert ch.name == "webhook1"
        assert ch.type == "webhook"
        assert ch.config["url"].startswith("https://")

    def test_throttler(self):
        thr = AlertThrottler(window_seconds=1, max_alerts=1)
        key = "k"
        assert thr.should_send_alert(key) is True
        assert thr.should_send_alert(key) is False

    def test_health_monitor_register(self):
        mon = SystemHealthMonitor(AlertManager())
        mon.register_health_check(lambda: True, "ok")
        assert len(mon.health_checks) == 1

    @pytest.mark.asyncio
    async def test_perform_health_checks_sends_alert_on_failure(self, alert_manager_instance):
        # health check returns False
        def bad_check():
            return False

        alert_manager_instance.health_monitor.register_health_check(bad_check, "bad")
        with patch.object(alert_manager_instance, "send_alert", new=AsyncMock()) as mock_send:
            await alert_manager_instance.health_monitor.perform_health_checks()
            assert mock_send.await_count >= 1

    @pytest.mark.asyncio
    async def test_send_alert_with_parameters(self, alert_manager_instance):
        with patch.object(alert_manager_instance, "_send_to_channels", new=AsyncMock()) as mock:
            alert_id = await alert_manager_instance.send_alert(
                level=AlertLevel.ERROR,
                failure_mode=FailureMode.SECURITY_BREACH,
                message="breach",
                source="test",
                metadata={"ip": "1.2.3.4"},
            )
            assert isinstance(alert_id, str)
            assert mock.await_count == 1

    @pytest.mark.asyncio
    async def test_send_alert_with_alert_object(self, alert_manager_instance):
        with patch.object(alert_manager_instance, "_send_to_channels", new=AsyncMock()) as mock:
            alert = Alert(
                id="x",
                level=AlertLevel.WARNING,
                failure_mode=FailureMode.PII_DETECTION_FAILURE,
                message="pii fail",
                source="detector",
                timestamp=datetime.now(),
            )
            alert_id = await alert_manager_instance.send_alert(alert)
            assert isinstance(alert_id, str)
            assert mock.await_count == 1

    @pytest.mark.asyncio
    @patch("security.alert_manager.AIOHTTP_AVAILABLE", True)
    @patch("security.alert_manager.aiohttp")
    async def test_webhook_delivery(self, mock_aiohttp, alert_manager_instance):
        # setup channel
        alert_manager_instance.channels = {
            "wh": AlertChannel(
                name="wh",
                type="webhook",
                config={"url": "https://example.test/webhook"},
                enabled=True,
                min_level=AlertLevel.INFO,
            )
        }
        # mock HTTP
        mock_session = AsyncMock()
        resp = AsyncMock()
        resp.status = 200
        mock_session.post.return_value.__aenter__.return_value = resp
        mock_aiohttp.ClientSession.return_value.__aenter__.return_value = mock_session

        alert = Alert(
            id="wh-1",
            level=AlertLevel.ERROR,
            failure_mode=FailureMode.AWS_CONNECTION_FAILURE,
            message="AWS down",
            source="aws",
            timestamp=datetime.now(),
        )
        await alert_manager_instance._send_to_channels(alert)
        mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_helper_alert_functions(self):
        with patch.object(alert_manager, "send_alert", new=AsyncMock()) as mock:
            await send_pii_detection_failure_alert("init failed", "ctx")
            await send_memory_exhaustion_alert(2048, "ctx2")
            await send_aws_connection_failure_alert("bedrock", "bad key", "ctx3")
            await send_security_breach_alert("unauth", "details", "ctx4")
            assert mock.await_count == 4

    def test_failure_mode_enum_extras(self):
        # Check that important failure modes exist
        assert FailureMode.PII_DETECTION_FAILURE is not None
        assert FailureMode.AWS_CONNECTION_FAILURE is not None
        assert FailureMode.SECURITY_BREACH is not None
        assert FailureMode.MEMORY_EXHAUSTION is not None

    def test_global_instance(self):
        assert alert_manager is not None
        assert isinstance(alert_manager, AlertManager)
