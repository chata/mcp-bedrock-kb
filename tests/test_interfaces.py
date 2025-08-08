from __future__ import annotations

"""
Tests for interface definitions and protocols.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

from src.bedrock_kb_mcp.interfaces import (
    BaseBedrockClient,
    BaseS3Manager,
    PIIFinding,
)


class MockPIIDetector:
    """Mock implementation of PIIDetectorInterface."""

    async def detect_pii(self, text: str) -> list[PIIFinding]:
        """Mock PII detection."""
        if "email@example.com" in text:
            return [
                PIIFinding(
                    entity_type="EMAIL_ADDRESS",
                    start=text.find("email@example.com"),
                    end=text.find("email@example.com") + len("email@example.com"),
                    score=0.95,
                    text="email@example.com",
                )
            ]
        return []

    async def mask_pii(self, text: str, findings: list[PIIFinding] | None = None) -> str:
        """Mock PII masking."""
        if findings is None:
            findings = await self.detect_pii(text)

        masked_text = text
        for finding in findings:
            mask = "*" * len(finding.text)
            masked_text = masked_text.replace(finding.text, mask)

        return masked_text

    def is_ready(self) -> bool:
        """Mock readiness check."""
        return True

    async def ensure_initialized(self) -> None:
        """Mock initialization."""
        pass


class MockAlertManager:
    """Mock implementation of AlertManagerInterface."""

    def __init__(self):
        self.alerts = []

    async def send_alert(
        self,
        level: str,
        failure_mode: str,
        message: str,
        source: str = "system",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Mock alert sending."""
        alert_id = f"alert_{len(self.alerts)}"
        self.alerts.append(
            {
                "id": alert_id,
                "level": level,
                "failure_mode": failure_mode,
                "message": message,
                "source": source,
                "metadata": metadata or {},
            }
        )
        return alert_id

    def get_active_alerts(self) -> list[dict[str, Any]]:
        """Mock get active alerts."""
        return self.alerts.copy()

    async def resolve_alert(self, alert_key: str, resolved_by: str = "system") -> bool:
        """Mock alert resolution."""
        for alert in self.alerts:
            if alert["id"] == alert_key:
                alert["resolved"] = True
                alert["resolved_by"] = resolved_by
                return True
        return False


class MockAuthManager:
    """Mock implementation of AuthManagerInterface."""

    def __init__(self):
        self._authenticated = True

    async def get_session(self) -> Any:
        """Mock session creation."""
        if not self._authenticated:
            raise Exception("Not authenticated")
        return MagicMock()

    async def refresh_credentials(self) -> None:
        """Mock credential refresh."""
        pass

    def is_authenticated(self) -> bool:
        """Mock authentication check."""
        return self._authenticated


class MockConfigManager:
    """Mock implementation of ConfigManagerInterface."""

    def __init__(self):
        self._config = {
            "aws.region": "us-east-1",
            "aws.profile": None,
            "s3.default_bucket": "test-bucket",
            "s3.max_concurrent_uploads": 5,
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Mock config get."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Mock config set."""
        self._config[key] = value

    def load_from_file(self, file_path: str) -> None:
        """Mock config loading."""
        pass

    def get_aws_region(self) -> str:
        """Mock AWS region."""
        return self._config.get("aws.region", "us-east-1")

    def get_aws_profile(self) -> str | None:
        """Mock AWS profile."""
        return self._config.get("aws.profile")


class MockConnectionPool:
    """Mock implementation of ConnectionPoolInterface."""

    def __init__(self):
        self._connections = {}
        self._stats = {"created": 0, "reused": 0}

    async def get_connection(self, service_name: str) -> Any:
        """Mock connection retrieval."""
        if service_name in self._connections:
            self._stats["reused"] += 1
        else:
            self._connections[service_name] = MagicMock()
            self._stats["created"] += 1
        return self._connections[service_name]

    async def return_connection(self, service_name: str, connection: Any) -> None:
        """Mock connection return."""
        pass

    async def close_all(self) -> None:
        """Mock pool closure."""
        self._connections.clear()

    def get_stats(self) -> dict[str, Any]:
        """Mock statistics."""
        return self._stats.copy()


class TestPIIDetectorInterface:
    """Test PII detector interface implementation."""

    @pytest.fixture
    def pii_detector(self):
        """Create mock PII detector."""
        return MockPIIDetector()

    @pytest.mark.asyncio
    async def test_detect_pii(self, pii_detector):
        """Test PII detection."""
        text = "Contact me at email@example.com"
        findings = await pii_detector.detect_pii(text)

        assert len(findings) == 1
        assert findings[0].entity_type == "EMAIL_ADDRESS"
        assert findings[0].text == "email@example.com"
        assert findings[0].score == 0.95

    @pytest.mark.asyncio
    async def test_detect_pii_no_findings(self, pii_detector):
        """Test PII detection with no findings."""
        text = "This is clean text"
        findings = await pii_detector.detect_pii(text)

        assert len(findings) == 0

    @pytest.mark.asyncio
    async def test_mask_pii(self, pii_detector):
        """Test PII masking."""
        text = "Contact me at email@example.com"
        masked = await pii_detector.mask_pii(text)

        assert "email@example.com" not in masked
        assert "*" * len("email@example.com") in masked

    @pytest.mark.asyncio
    async def test_mask_pii_with_findings(self, pii_detector):
        """Test PII masking with pre-detected findings."""
        text = "Contact me at email@example.com"
        findings = [
            PIIFinding(
                entity_type="EMAIL_ADDRESS", start=14, end=31, score=0.95, text="email@example.com"
            )
        ]

        masked = await pii_detector.mask_pii(text, findings)
        assert "email@example.com" not in masked

    def test_is_ready(self, pii_detector):
        """Test readiness check."""
        assert pii_detector.is_ready() is True

    @pytest.mark.asyncio
    async def test_ensure_initialized(self, pii_detector):
        """Test initialization."""
        await pii_detector.ensure_initialized()


class TestAlertManagerInterface:
    """Test alert manager interface implementation."""

    @pytest.fixture
    def alert_manager(self):
        """Create mock alert manager."""
        return MockAlertManager()

    @pytest.mark.asyncio
    async def test_send_alert(self, alert_manager):
        """Test alert sending."""
        alert_id = await alert_manager.send_alert(
            level="ERROR",
            failure_mode="test_failure",
            message="Test alert",
            source="test",
            metadata={"key": "value"},
        )

        assert alert_id.startswith("alert_")

        alerts = alert_manager.get_active_alerts()
        assert len(alerts) == 1
        assert alerts[0]["level"] == "ERROR"
        assert alerts[0]["message"] == "Test alert"

    @pytest.mark.asyncio
    async def test_resolve_alert(self, alert_manager):
        """Test alert resolution."""
        alert_id = await alert_manager.send_alert("INFO", "test", "Test")

        resolved = await alert_manager.resolve_alert(alert_id, "admin")
        assert resolved is True

        alerts = alert_manager.get_active_alerts()
        assert alerts[0]["resolved"] is True
        assert alerts[0]["resolved_by"] == "admin"

    @pytest.mark.asyncio
    async def test_resolve_nonexistent_alert(self, alert_manager):
        """Test resolving non-existent alert."""
        resolved = await alert_manager.resolve_alert("nonexistent")
        assert resolved is False


class TestAuthManagerInterface:
    """Test auth manager interface implementation."""

    @pytest.fixture
    def auth_manager(self):
        """Create mock auth manager."""
        return MockAuthManager()

    @pytest.mark.asyncio
    async def test_get_session_authenticated(self, auth_manager):
        """Test getting session when authenticated."""
        session = await auth_manager.get_session()
        assert session is not None

    @pytest.mark.asyncio
    async def test_get_session_not_authenticated(self, auth_manager):
        """Test getting session when not authenticated."""
        auth_manager._authenticated = False

        with pytest.raises(Exception, match="Not authenticated"):
            await auth_manager.get_session()

    def test_is_authenticated(self, auth_manager):
        """Test authentication check."""
        assert auth_manager.is_authenticated() is True

        auth_manager._authenticated = False
        assert auth_manager.is_authenticated() is False

    @pytest.mark.asyncio
    async def test_refresh_credentials(self, auth_manager):
        """Test credential refresh."""
        await auth_manager.refresh_credentials()  # Should not raise


class TestConfigManagerInterface:
    """Test config manager interface implementation."""

    @pytest.fixture
    def config_manager(self):
        """Create mock config manager."""
        return MockConfigManager()

    def test_get_config(self, config_manager):
        """Test config retrieval."""
        assert config_manager.get("aws.region") == "us-east-1"
        assert config_manager.get("nonexistent", "default") == "default"

    def test_set_config(self, config_manager):
        """Test config setting."""
        config_manager.set("test.key", "test_value")
        assert config_manager.get("test.key") == "test_value"

    def test_get_aws_region(self, config_manager):
        """Test AWS region retrieval."""
        assert config_manager.get_aws_region() == "us-east-1"

    def test_get_aws_profile(self, config_manager):
        """Test AWS profile retrieval."""
        assert config_manager.get_aws_profile() is None

    def test_load_from_file(self, config_manager):
        """Test loading config from file."""
        config_manager.load_from_file("test.yaml")  # Should not raise


class TestConnectionPoolInterface:
    """Test connection pool interface implementation."""

    @pytest.fixture
    def connection_pool(self):
        """Create mock connection pool."""
        return MockConnectionPool()

    @pytest.mark.asyncio
    async def test_get_connection_new(self, connection_pool):
        """Test getting new connection."""
        conn = await connection_pool.get_connection("s3")
        assert conn is not None

        stats = connection_pool.get_stats()
        assert stats["created"] == 1
        assert stats["reused"] == 0

    @pytest.mark.asyncio
    async def test_get_connection_reused(self, connection_pool):
        """Test getting reused connection."""
        # First connection
        conn1 = await connection_pool.get_connection("s3")
        await connection_pool.return_connection("s3", conn1)

        # Second connection (should be reused)
        await connection_pool.get_connection("s3")

        stats = connection_pool.get_stats()
        assert stats["created"] == 1
        assert stats["reused"] == 1

    @pytest.mark.asyncio
    async def test_return_connection(self, connection_pool):
        """Test returning connection."""
        conn = await connection_pool.get_connection("s3")
        await connection_pool.return_connection("s3", conn)  # Should not raise

    @pytest.mark.asyncio
    async def test_close_all(self, connection_pool):
        """Test closing all connections."""
        await connection_pool.get_connection("s3")
        await connection_pool.get_connection("bedrock")

        await connection_pool.close_all()

        # Connections should be cleared
        connection_pool.get_stats()
        assert len(connection_pool._connections) == 0


class TestBaseClasses:
    """Test abstract base classes."""

    def test_base_s3_manager_initialization(self):
        """Test BaseS3Manager initialization."""
        auth_manager = MockAuthManager()
        config_manager = MockConfigManager()
        pii_detector = MockPIIDetector()
        alert_manager = MockAlertManager()
        connection_pool = MockConnectionPool()

        # Cannot instantiate abstract class directly
        with pytest.raises(TypeError):
            BaseS3Manager(
                auth_manager, config_manager, pii_detector, alert_manager, connection_pool
            )

    def test_base_bedrock_client_initialization(self):
        """Test BaseBedrockClient initialization."""
        auth_manager = MockAuthManager()
        config_manager = MockConfigManager()
        connection_pool = MockConnectionPool()

        # Cannot instantiate abstract class directly
        with pytest.raises(TypeError):
            BaseBedrockClient(auth_manager, config_manager, connection_pool)
