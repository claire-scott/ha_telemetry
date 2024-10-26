from datetime import datetime
import json
from .base import EventHandler

class ConsoleEventHandler(EventHandler):
    """Simple event handler that prints events to console"""
    def __init__(self):
        self.events_handled = 0
        self.last_event_time = None

    async def handle_event(self, event: dict) -> None:
        print(f"Event received at {datetime.utcnow().isoformat()}:")
        print(json.dumps(event, indent=2))
        self.events_handled += 1
        self.last_event_time = datetime.utcnow()

    async def cleanup(self) -> None:
        pass

    def get_stats(self) -> dict:
        return {
            "events_handled": self.events_handled,
            "last_event_time": self.last_event_time
        }