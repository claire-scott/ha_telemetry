import json
import asyncio
import websockets
import time
from datetime import datetime, timedelta
from typing import AsyncGenerator
from ..models.metrics import EnhancedEventStats, EventMetrics

class HomeAssistantClient:
    """Enhanced Home Assistant WebSocket client"""
    def __init__(self, websocket_url: str, access_token: str):
        self.websocket_url = websocket_url
        self.access_token = access_token
        self.message_id = 1
        self.stats = EnhancedEventStats()
        self._start_time = datetime.utcnow()

    async def authenticate(self, websocket) -> None:
        """Authenticate with Home Assistant"""
        auth_message = {
            "type": "auth",
            "access_token": self.access_token
        }
        await websocket.send(json.dumps(auth_message))
        auth_response = await websocket.recv()
        if json.loads(auth_response)["type"] != "auth_ok":
            raise Exception("Authentication failed")

    async def subscribe_to_events(self, websocket) -> None:
        """Subscribe to all events"""
        subscribe_message = {
            "id": self.message_id,
            "type": "subscribe_events"
        }
        self.message_id += 1
        await websocket.send(json.dumps(subscribe_message))

    def update_stats(self, event_data: dict, processing_time: float, error: bool = False) -> None:
        now = datetime.utcnow()
        event_type = event_data['event']['event_type']
        event_size = len(json.dumps(event_data))

        self.stats.total_events += 1
        self.stats.last_event_time = now
        self.stats.minute_counts.append(now)
        self.stats.hour_counts.append(now)
        self.stats.update_rates()

        if event_type not in self.stats.event_metrics:
            self.stats.event_metrics[event_type] = EventMetrics(event_type=event_type)
        
        self.stats.event_metrics[event_type].update(
            event_size=event_size,
            processing_time=processing_time,
            error=error
        )

        self.stats.recent_events.append({
            'timestamp': now.isoformat(),
            'type': event_type,
            'size': event_size,
            'processing_time': processing_time
        })

        if error:
            self.stats.error_history.append((now, str(error)))

    async def event_stream(self) -> AsyncGenerator[dict, None]:
        while True:
            try:
                self.stats.connection_status = "Connecting..."
                self.stats.connection_history.append(
                    (datetime.utcnow(), "Connecting")
                )
                
                async with websockets.connect(self.websocket_url) as websocket:
                    await self.authenticate(websocket)
                    await self.subscribe_to_events(websocket)
                    
                    self.stats.connection_status = "Connected"
                    self.stats.connection_history.append(
                        (datetime.utcnow(), "Connected")
                    )
                    self.stats.last_error = None
                    
                    while True:
                        message = await websocket.recv()
                        event_data = json.loads(message)
                        
                        if event_data.get("type") == "event":
                            start_time = time.time()
                            try:
                                yield event_data
                                processing_time = time.time() - start_time
                                self.update_stats(event_data, processing_time)
                            except Exception as e:
                                processing_time = time.time() - start_time
                                self.update_stats(event_data, processing_time, error=str(e))
                                raise

            except Exception as e:
                self.stats.connection_status = "Error"
                self.stats.last_error = str(e)
                self.stats.connection_history.append(
                    (datetime.utcnow(), f"Error: {str(e)}")
                )
                await asyncio.sleep(5)

    def get_uptime(self) -> timedelta:
        return datetime.utcnow() - self._start_time

    def get_performance_metrics(self) -> dict:
        metrics = {
            'uptime_seconds': self.get_uptime().total_seconds(),
            'total_events': self.stats.total_events,
            'events_per_minute': self.stats.events_per_minute,
            'events_per_hour': self.stats.get_hourly_rate(),
            'event_type_distribution': self.stats.get_event_type_distribution(),
            'error_rate': len(self.stats.error_history) / max(1, self.stats.total_events),
            'connection_stability': len(self.stats.connection_history) / max(1, self.get_uptime().total_seconds() / 3600)
        }
        
        all_processing_times = []
        for event_metric in self.stats.event_metrics.values():
            all_processing_times.extend(event_metric.processing_times)
            
        if all_processing_times:
            metrics.update({
                'avg_processing_time': sum(all_processing_times) / len(all_processing_times),
                'max_processing_time': max(all_processing_times),
                'min_processing_time': min(all_processing_times)
            })
            
        return metrics