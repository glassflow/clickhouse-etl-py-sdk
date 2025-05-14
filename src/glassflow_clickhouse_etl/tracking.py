from __future__ import annotations

import configparser
import os
import platform
import uuid
from importlib.metadata import version
from typing import Any, Dict

import mixpanel


def _get_distinct_id() -> str:
    """Get or create a distinct ID for tracking.

    Returns:
        str: The distinct ID for tracking
    """
    home_dir = os.path.expanduser("~")
    config_dir = os.path.join(home_dir, ".glassflow")
    config_file = os.path.join(config_dir, "clickhouse.conf")

    # Create directory if it doesn't exist
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)

    config = configparser.ConfigParser()

    # Read existing config if it exists
    if os.path.exists(config_file):
        config.read(config_file)
        if "analytics" in config and "distinct_id" in config["analytics"]:
            return config["analytics"]["distinct_id"]

    # Generate new ID and save it
    distinct_id = str(uuid.uuid4())

    # Create or update config
    if "analytics" not in config:
        config["analytics"] = {}
    config["analytics"]["distinct_id"] = distinct_id

    # Write config to file
    with open(config_file, "w") as f:
        config.write(f)

    return distinct_id


DISTINCT_ID = _get_distinct_id()


class Tracking:
    """Mixpanel tracking implementation for GlassFlow Clickhouse ETL."""

    def __init__(self):
        """Initialize the tracking client"""
        self.enabled = os.getenv("GF_TRACKING_ENABLED", "true").lower() == "true"
        self._project_token = "209670ec9b352915013a5dfdb169dd25"
        self._distinct_id = DISTINCT_ID
        self.client = mixpanel.Mixpanel(self._project_token)

        self.sdk_version = version("glassflow-clickhouse-etl")
        self.platform = platform.system()
        self.python_version = platform.python_version()

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
            "sdk_version": self.sdk_version,
            "platform": self.platform,
            "python_version": self.python_version,
        }
        if properties is None:
            properties = {}
        properties = {**base_properties, **properties}

        try:
            self.client.track(
                distinct_id=self._distinct_id,
                event_name=event_name,
                properties=properties,
            )
        except Exception:
            pass
