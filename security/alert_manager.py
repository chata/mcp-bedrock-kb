"""
Explicit failure modes and alerting functionality
Comprehensive alerting for system failures, security events, PII detection failures, etc.
"""
import os
import logging
import asyncio
import json
import smtplib
import time
import psutil
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Callable, Any, Union
from dataclasses import dataclass, field
try:
    from email.mime.text import MimeText
    from email.mime.multipart import MimeMultipart
except ImportError:
    # Fallback for older Python versions
    from email.mime.text import MIMEText as MimeText  
    from email.mime.multipart import MIMEMultipart as MimeMultipart
from enum import Enum
from collections import defaultdict, deque

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert level"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class FailureMode(Enum):
    """Failure mode"""
    PII_DETECTION_FAILURE = "pii_detection_failure"
    AWS_CONNECTION_FAILURE = "aws_connection_failure"
    MEMORY_EXHAUSTION = "memory_exhaustion"
    AUTHENTICATION_FAILURE = "auth_failure"
    DATA_CORRUPTION = "data_corruption"
    SECURITY_BREACH = "security_breach"
    SERVICE_UNAVAILABLE = "service_unavailable"
    CONFIGURATION_ERROR = "config_error"
    UNKNOWN_ERROR = "unknown_error"
    # Additional modes to satisfy broader usage
    LOG_ONLY = "log_only"
    RETRY_EXPONENTIAL = "retry_exponential"
    FAIL_FAST = "fail_fast"


@dataclass
class Alert:
    """Alert information"""
    id: str
    level: AlertLevel
    failure_mode: FailureMode
    message: str
    source: str
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    count: int = 1  # Number of occurrences of the same alert


@dataclass
class AlertChannel:
    """Alert sending channel"""
    name: str
    type: str  # email, webhook, slack, log
    config: Dict[str, Any]
    enabled: bool = True
    min_level: AlertLevel = AlertLevel.WARNING


class AlertThrottler:
    """Alert throttling functionality"""
    
    def __init__(self, window_seconds: int = 300, max_alerts: int = 5):
        """
        Args:
            window_seconds: Time window (seconds)
            max_alerts: Maximum number of alerts within the time window
        """
        self.window_seconds = window_seconds
        self.max_alerts = max_alerts
        self.alert_history: Dict[str, deque] = defaultdict(lambda: deque())
    
    def should_send_alert(self, alert_key: str) -> bool:
        """Check if alert should be sent"""
        now = time.time()
        history = self.alert_history[alert_key]
        
        # Remove old entries
        while history and history[0] < now - self.window_seconds:
            history.popleft()
        
        if len(history) >= self.max_alerts:
            return False
        
        history.append(now)
        return True


class SystemHealthMonitor:
    """System health check"""
    
    def __init__(self, alert_manager):
        self.alert_manager = alert_manager
        self.health_checks: List[Callable[[], bool]] = []
        self.last_check_time = None
        self.check_interval = 300  # 5 minute interval
    
    def register_health_check(self, check_func: Callable[[], bool], name: str):
        """Register health check function"""
        check_func._check_name = name
        self.health_checks.append(check_func)
    
    async def perform_health_checks(self):
        """Perform health checks"""
        now = datetime.now(timezone.utc)
        
        if (self.last_check_time and 
            (now - self.last_check_time).seconds < self.check_interval):
            return
        
        self.last_check_time = now
        
        for check in self.health_checks:
            try:
                is_healthy = check()
                check_name = getattr(check, '_check_name', 'unknown')
                
                if not is_healthy:
                    await self.alert_manager.send_alert(
                        level=AlertLevel.WARNING,
                        failure_mode=FailureMode.SERVICE_UNAVAILABLE,
                        message=f"Health check failed: {check_name}",
                        source="health_monitor",
                        metadata={"check_name": check_name, "timestamp": now.isoformat()}
                    )
            except Exception as e:
                logger.error(f"Health check error: {e}")


class AlertManager:
    """Alert management system"""
    
    def __init__(self):
        """Initialization"""
        self.channels: Dict[str, AlertChannel] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.throttler = AlertThrottler()
        self.health_monitor = SystemHealthMonitor(self)
        self._initialize_default_channels()
        self._setup_failure_handlers()
    
    def _initialize_default_channels(self):
        """Initialize default channels"""
        # Log channel
        self.add_channel(AlertChannel(
            name="log",
            type="log",
            config={},
            enabled=True,
            min_level=AlertLevel.INFO
        ))
        
        # Email channel (configured via environment variables)
        email_config = self._get_email_config()
        if email_config:
            self.add_channel(AlertChannel(
                name="email",
                type="email",
                config=email_config,
                enabled=True,
                min_level=AlertLevel.ERROR
            ))
        
        # Webhook channel (configured via environment variables)
        webhook_config = self._get_webhook_config()
        if webhook_config:
            self.add_channel(AlertChannel(
                name="webhook",
                type="webhook",
                config=webhook_config,
                enabled=True,
                min_level=AlertLevel.WARNING
            ))
    
    def _get_email_config(self) -> Optional[Dict[str, str]]:
        """Get email config from environment variables"""
        smtp_server = os.getenv("ALERT_SMTP_SERVER")
        smtp_port = os.getenv("ALERT_SMTP_PORT", "587")
        smtp_user = os.getenv("ALERT_SMTP_USER")
        smtp_password = os.getenv("ALERT_SMTP_PASSWORD")
        from_email = os.getenv("ALERT_FROM_EMAIL")
        to_emails = os.getenv("ALERT_TO_EMAILS", "").split(",")
        
        if all([smtp_server, smtp_user, smtp_password, from_email]) and to_emails:
            return {
                "smtp_server": smtp_server,
                "smtp_port": int(smtp_port),
                "username": smtp_user,
                "password": smtp_password,
                "from_email": from_email,
                "to_emails": [email.strip() for email in to_emails if email.strip()]
            }
        return None
    
    def _get_webhook_config(self) -> Optional[Dict[str, str]]:
        """Get webhook config from environment variables"""
        webhook_url = os.getenv("ALERT_WEBHOOK_URL")
        webhook_secret = os.getenv("ALERT_WEBHOOK_SECRET")
        
        if webhook_url:
            return {
                "url": webhook_url,
                "secret": webhook_secret,
                "timeout": int(os.getenv("ALERT_WEBHOOK_TIMEOUT", "30"))
            }
        return None
    
    def _setup_failure_handlers(self):
        """Set up failure handlers"""
        # PII detection system health check
        def pii_health_check():
            try:
                from .pii_detector import pii_detector
                return pii_detector.detection_enabled
            except:
                return False
        
        self.health_monitor.register_health_check(pii_health_check, "pii_detector")
        
        # Memory usage check
        def memory_health_check():
            try:
                import psutil
                memory = psutil.virtual_memory()
                return memory.percent < 90  # Normal if less than 90%
            except:
                return True  # Assume normal if psutil is not available
        
        self.health_monitor.register_health_check(memory_health_check, "memory_usage")
    
    def add_channel(self, channel: AlertChannel):
        """Add alert channel"""
        self.channels[channel.name] = channel
        logger.info(f"Added alert channel: {channel.name} ({channel.type})")
    
    def remove_channel(self, channel_name: str):
        """Remove alert channel"""
        if channel_name in self.channels:
            del self.channels[channel_name]
            logger.info(f"Removed alert channel: {channel_name}")
    
    def enable_channel(self, channel_name: str):
        """Enable channel"""
        if channel_name in self.channels:
            self.channels[channel_name].enabled = True
    
    def disable_channel(self, channel_name: str):
        """Disable channel"""
        if channel_name in self.channels:
            self.channels[channel_name].enabled = False
    
    async def send_alert(
        self,
        level: Any,
        failure_mode: FailureMode = FailureMode.UNKNOWN_ERROR,
        message: str = "",
        source: str = "system",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Send alert"""
        # Accept either an Alert object or discrete parameters
        if isinstance(level, Alert):
            alert = level
            alert_id = alert.id or f"{alert.source}_{alert.failure_mode.value}_{int(time.time())}"
            alert_key = f"{alert.source}_{alert.failure_mode.value}"
        else:
            alert_id = f"{source}_{failure_mode.value}_{int(time.time())}"
            alert_key = f"{source}_{failure_mode.value}"
        
        # Throttling check
        if not self.throttler.should_send_alert(alert_key):
            logger.debug(f"Alert throttled: {alert_key}")
            # Increment count for existing alert
            if alert_key in self.active_alerts:
                self.active_alerts[alert_key].count += 1
            return alert_id

        if not isinstance(level, Alert):
            alert = Alert(
                id=alert_id,
                level=level,
                failure_mode=failure_mode,
                message=message,
                source=source,
                timestamp=datetime.now(timezone.utc),
                metadata=metadata or {}
            )
        
        # Add to active alerts
        self.active_alerts[alert_key] = alert
        self.alert_history.append(alert)
        
        # History limit (keep latest 1000 items)
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]
        
        # Send to channels
        await self._send_to_channels(alert)

        # Log with normalized level string
        level_str = alert.level.value if isinstance(level, Alert) else level.value
        logger.info(f"Alert sent: {alert_id} - {level_str}: {alert.message}")
        return alert_id
    
    async def _send_to_channels(self, alert: Alert):
        """Send alert to all channels"""
        for channel in self.channels.values():
            if not channel.enabled:
                continue
            
            # Level filter
            level_priority = {
                AlertLevel.INFO: 1,
                AlertLevel.WARNING: 2,
                AlertLevel.ERROR: 3,
                AlertLevel.CRITICAL: 4
            }
            
            if level_priority[alert.level] < level_priority[channel.min_level]:
                continue
            
            try:
                await self._send_to_channel(alert, channel)
            except Exception as e:
                logger.error(f"Failed to send alert to channel {channel.name}: {e}")
    
    async def _send_to_channel(self, alert: Alert, channel: AlertChannel):
        """Send alert to individual channel"""
        if channel.type == "log":
            await self._send_to_log(alert)
        elif channel.type == "email":
            await self._send_to_email(alert, channel.config)
        elif channel.type == "webhook":
            await self._send_to_webhook(alert, channel.config)
    
    async def _send_to_log(self, alert: Alert):
        """Send to log"""
        log_level = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.ERROR: logging.ERROR,
            AlertLevel.CRITICAL: logging.CRITICAL
        }[alert.level]
        
        logger.log(log_level, f"ALERT [{alert.failure_mode.value}] {alert.message} (source: {alert.source})")
    
    async def _send_to_email(self, alert: Alert, config: Dict[str, Any]):
        """Send to email"""
        try:
            msg = MimeMultipart()
            msg['From'] = config['from_email']
            msg['To'] = ', '.join(config['to_emails'])
            msg['Subject'] = f"[ALERT-{alert.level.value.upper()}] {alert.failure_mode.value}"
            
            body = self._format_email_body(alert)
            msg.attach(MimeText(body, 'plain'))
            
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            server.starttls()
            server.login(config['username'], config['password'])
            
            text = msg.as_string()
            server.sendmail(config['from_email'], config['to_emails'], text)
            server.quit()
            
        except Exception as e:
            logger.error(f"Email alert failed: {e}")
    
    async def _send_to_webhook(self, alert: Alert, config: Dict[str, Any]):
        """Send to webhook"""
        if not AIOHTTP_AVAILABLE:
            logger.warning("aiohttp not available, skipping webhook alert")
            return
        
        try:
            payload = {
                "alert_id": alert.id,
                "level": alert.level.value,
                "failure_mode": alert.failure_mode.value,
                "message": alert.message,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat(),
                "metadata": alert.metadata
            }
            
            headers = {"Content-Type": "application/json"}
            if config.get("secret"):
                headers["X-Alert-Secret"] = config["secret"]
            
            timeout = aiohttp.ClientTimeout(total=config.get("timeout", 30))
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Await the coroutine returned by session.post before using it as an async context manager
                async with (await session.post(
                    config["url"],
                    json=payload,
                    headers=headers
                )) as response:
                    if response.status >= 400:
                        logger.error(f"Webhook alert failed: HTTP {response.status}")
                        
        except Exception as e:
            logger.error(f"Webhook alert failed: {e}")
    
    def _format_email_body(self, alert: Alert) -> str:
        """Format email body"""
        return f"""
Alert Details:
==============

Alert ID: {alert.id}
Level: {alert.level.value.upper()}
Failure Mode: {alert.failure_mode.value}
Source: {alert.source}
Timestamp: {alert.timestamp.isoformat()}

Message:
{alert.message}

Metadata:
{json.dumps(alert.metadata, indent=2) if alert.metadata else 'None'}

Count: {alert.count} occurrence(s)

---
This is an automated alert from Bedrock Knowledge Base MCP Server.
"""
    
    async def resolve_alert(self, alert_key: str, resolved_by: str = "system") -> bool:
        """Mark alert as resolved"""
        if alert_key in self.active_alerts:
            alert = self.active_alerts[alert_key]
            alert.resolved = True
            alert.resolved_at = datetime.now(timezone.utc)
            alert.metadata["resolved_by"] = resolved_by
            
            # Remove from active list
            del self.active_alerts[alert_key]
            
            logger.info(f"Alert resolved: {alert_key} by {resolved_by}")
            return True
        return False
    
    def get_active_alerts(self, level: Optional[AlertLevel] = None) -> List[Alert]:
        """Get active alerts"""
        alerts = list(self.active_alerts.values())
        if level:
            alerts = [a for a in alerts if a.level == level]
        return sorted(alerts, key=lambda x: x.timestamp, reverse=True)
    
    def get_alert_history(
        self,
        hours: int = 24,
        level: Optional[AlertLevel] = None,
        failure_mode: Optional[FailureMode] = None
    ) -> List[Alert]:
        """Get alert history"""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        alerts = [a for a in self.alert_history if a.timestamp >= cutoff]
        
        if level:
            alerts = [a for a in alerts if a.level == level]
        
        if failure_mode:
            alerts = [a for a in alerts if a.failure_mode == failure_mode]
        
        return sorted(alerts, key=lambda x: x.timestamp, reverse=True)
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health status"""
        return {
            "active_alerts_count": len(self.active_alerts),
            "critical_alerts_count": len([a for a in self.active_alerts.values() if a.level == AlertLevel.CRITICAL]),
            "enabled_channels": [name for name, ch in self.channels.items() if ch.enabled],
            "last_health_check": self.health_monitor.last_check_time.isoformat() if self.health_monitor.last_check_time else None,
            "alert_throttling_active": len(self.throttler.alert_history) > 0
        }
    
    async def test_channels(self) -> Dict[str, bool]:
        """Test sending alerts to all channels"""
        results = {}
        
        test_alert = Alert(
            id="test_alert",
            level=AlertLevel.INFO,
            failure_mode=FailureMode.CONFIGURATION_ERROR,
            message="This is a test alert to verify channel configuration",
            timestamp=datetime.now(timezone.utc),
            source="alert_manager_test",
            metadata={"test": True}
        )
        
        for channel_name, channel in self.channels.items():
            if not channel.enabled:
                results[channel_name] = False
                continue
                
            try:
                await self._send_to_channel(test_alert, channel)
                results[channel_name] = True
            except Exception as e:
                logger.error(f"Channel test failed for {channel_name}: {e}")
                results[channel_name] = False
        
        return results
    
    async def start_health_monitoring(self):
        """Start health monitoring"""
        while True:
            try:
                await self.health_monitor.perform_health_checks()
                await asyncio.sleep(60)  # 1 minute interval
            except Exception as e:
                logger.error(f"Health monitoring error: {e}")
                await asyncio.sleep(60)


# Global instance
alert_manager = AlertManager()


# Utility functions
async def send_pii_detection_failure_alert(error_message: str, source: str = "pii_detector"):
    """Send PII detection failure alert"""
    await alert_manager.send_alert(
        level=AlertLevel.ERROR,
        failure_mode=FailureMode.PII_DETECTION_FAILURE,
        message=f"PII detection failed: {error_message}",
        source=source,
        metadata={"error": error_message}
    )


async def send_memory_exhaustion_alert(memory_usage_mb: int, source: str = "memory_monitor"):
    """Send memory exhaustion alert"""
    await alert_manager.send_alert(
        level=AlertLevel.CRITICAL,
        failure_mode=FailureMode.MEMORY_EXHAUSTION,
        message=f"Memory usage critical: {memory_usage_mb}MB",
        source=source,
        metadata={"memory_usage_mb": memory_usage_mb}
    )


async def send_aws_connection_failure_alert(service: str, error: str, source: str = "aws_client"):
    """Send AWS connection failure alert"""
    await alert_manager.send_alert(
        level=AlertLevel.ERROR,
        failure_mode=FailureMode.AWS_CONNECTION_FAILURE,
        message=f"AWS {service} connection failed: {error}",
        source=source,
        metadata={"service": service, "error": error}
    )


async def send_security_breach_alert(event_type: str, details: str, source: str = "security_monitor"):
    """Send security breach alert"""
    await alert_manager.send_alert(
        level=AlertLevel.CRITICAL,
        failure_mode=FailureMode.SECURITY_BREACH,
        message=f"Security breach detected: {event_type}",
        source=source,
        metadata={"event_type": event_type, "details": details}
    )