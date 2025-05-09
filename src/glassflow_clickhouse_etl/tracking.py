from __future__ import annotations

import os
from typing import Any, Dict

import mixpanel

class Tracking:
    """Mixpanel tracking implementation for GlassFlow Clickhouse ETL."""
    
    def __init__(self, project_token: str | None = None):
        """Initialize the tracking client.
        
        Args:
            project_token: Mixpanel project token. If not provided, will try to get from MIXPANEL_TOKEN env var.
        """
        self.project_token = project_token or os.getenv("MIXPANEL_TOKEN")
        if not self.project_token:
            raise ValueError("Mixpanel project token is required. Set MIXPANEL_TOKEN env var or provide project_token.")
        
        self.client = mixpanel.Mixpanel(self.project_token)
    
    def track_event(self, event_name: str, properties: Dict[str, Any] | None = None) -> None:
        """Track an event in Mixpanel.
        
        Args:
            event_name: Name of the event to track
            properties: Additional properties to include with the event
        """
        if properties is None:
            properties = {}
            
        self.client.track(
            distinct_id="glassflow-clickhouse-etl",
            event_name=event_name,
            properties=properties
        ) 