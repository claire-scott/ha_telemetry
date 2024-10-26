from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, Optional, Deque

@dataclass
class EventMetrics:
    """Detailed metrics for event monitoring"""
    event_type: str
    count: int = 0
    last_seen: Optional[datetime] = None
    avg_size_bytes: float = 0
    min_size_bytes: int = float('inf')
    max_size_bytes: int = 0
    error_count: int = 0
    processing_times: Deque[float] = field(default_factory=lambda: deque(maxlen=100))

    def update(self, event_size: int, processing_time: float, error: bool = False):
        self.count += 1
        self.last_seen = datetime.utcnow()
        self.avg_size_bytes = (self.avg_size_bytes * (self.count - 1) + event_size) / self.count
        self.min_size_bytes = min(self.min_size_bytes, event_size)
        self.max_size_bytes = max(self.max_size_bytes, event_size)
        self.processing_times.append(processing_time)
        if error:
            self.error_count += 1

@dataclass
class EnhancedEventStats:
    """Enhanced statistics tracking"""
    total_events: int = 0
    events_per_minute: float = 0
    last_event_time: Optional[datetime] = None
    connection_status: str = "Disconnected"
    last_error: Optional[str] = None
    last_heartbeat: Optional[datetime] = None
    event_metrics: Dict[str, EventMetrics] = field(default_factory=dict)
    recent_events: Deque[dict] = field(default_factory=lambda: deque(maxlen=100))
    error_history: Deque[tuple] = field(default_factory=lambda: deque(maxlen=50))
    connection_history: Deque[tuple] = field(default_factory=lambda: deque(maxlen=100))
    minute_counts: Deque[datetime] = field(default_factory=lambda: deque(maxlen=60))
    hour_counts: Deque[datetime] = field(default_factory=lambda: deque(maxlen=3600))

    def update_rates(self):
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        hour_ago = now - timedelta(hours=1)
        
        while self.minute_counts and self.minute_counts[0] < minute_ago:
            self.minute_counts.popleft()
        while self.hour_counts and self.hour_counts[0] < hour_ago:
            self.hour_counts.popleft()
            
        self.events_per_minute = len(self.minute_counts)

    def get_hourly_rate(self) -> float:
        return len(self.hour_counts)

    def get_event_type_distribution(self) -> Dict[str, int]:
        return {k: v.count for k, v in self.event_metrics.items()}