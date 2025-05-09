from __future__ import annotations

import os
import platform
from importlib.metadata import version
from typing import Any, Dict

import mixpanel


class Tracking:
    """Mixpanel tracking implementation for GlassFlow Clickhouse ETL."""

    def __init__(self):
        """Initialize the tracking client"""
        self.enabled = os.getenv("GF_TRACKING_ENABLED", "true").lower() == "true"
        self._project_token = "209670ec9b352915013a5dfdb169dd25"
        self.client = mixpanel.Mixpanel(self._project_token)

    def track_event(
        self, event_name: str, properties: Dict[str, Any] | None = None
    ) -> None:
        """Track an event in Mixpanel.

        Args:
            event_name: Name of the event to track
            properties: Additional properties to include with the event
        """
        if not self.enabled:
            return

        base_properties = {
            "sdk_version": version("glassflow-clickhouse-etl"),
            "platform": platform.system(),
            "python_version": platform.python_version()
        }
        if properties is None:
            properties = {}
        properties = {**base_properties, **properties}

        try:
            self.client.track(
                distinct_id="glassflow-clickhouse-etl",
                event_name=event_name,
                properties=properties,
            )
        except Exception:
            pass
