import json
import asyncio
import threading
import websockets
import time
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template_string
from azure.eventhub import EventHubProducerClient, EventData
from aiokafka import AIOKafkaProducer
from collections import deque, Counter
from dataclasses import dataclass, field
from typing import Dict, List, Deque, AsyncGenerator, Callable, Any, Optional
from abc import ABC, abstractmethod

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

@dataclass
class EventStats:
    total_events: int = 0
    events_per_minute: float = 0
    last_event_time: Optional[datetime] = None
    recent_event_types: List[str] = field(default_factory=list)
    connection_status: str = "Disconnected"
    last_error: Optional[str] = None
    last_heartbeat: Optional[datetime] = None
    event_metrics: Dict[str, EventMetrics] = field(default_factory=dict)
    processing_times: Deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    errors: Deque[Dict] = field(default_factory=lambda: deque(maxlen=100))
    throughput_history: Deque[Dict] = field(default_factory=lambda: deque(maxlen=60))  # Last hour
    
    def update_event_metrics(self, event_type: str, event_size: int, processing_time: float):
        if event_type not in self.event_metrics:
            self.event_metrics[event_type] = EventMetrics(event_type=event_type)
        
        metrics = self.event_metrics[event_type]
        metrics.count += 1
        metrics.last_seen = datetime.utcnow()
        metrics.min_size_bytes = min(metrics.min_size_bytes, event_size)
        metrics.max_size_bytes = max(metrics.max_size_bytes, event_size)
        metrics.avg_size_bytes = (metrics.avg_size_bytes * (metrics.count - 1) + event_size) / metrics.count
        
        self.processing_times.append(processing_time)
    
    def update_throughput(self):
        current_time = datetime.utcnow()
        self.throughput_history.append({
            'timestamp': current_time,
            'events_per_minute': self.events_per_minute
        })
    
    def add_error(self, error: str, event_type: Optional[str] = None):
        self.errors.append({
            'timestamp': datetime.utcnow(),
            'error': str(error),
            'event_type': event_type
        })
        if event_type and event_type in self.event_metrics:
            self.event_metrics[event_type].error_count += 1

class EventHandler(ABC):
    """Abstract base class for event handlers"""
    @abstractmethod
    async def handle_event(self, event: dict) -> None:
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        pass

class ConsoleEventHandler(EventHandler):
    """Simple event handler that prints events to console"""
    async def handle_event(self, event: dict) -> None:
        print(f"Event received: {json.dumps(event, indent=2)}")

    async def cleanup(self) -> None:
        pass

class EventHubHandler(EventHandler):
    """Handler for sending events to Azure Event Hub"""
    def __init__(self, connection_str: str, eventhub_name: str):
        self.connection_str = connection_str
        self.eventhub_name = eventhub_name
        self.producer = None

    async def handle_event(self, event: dict) -> None:
        if not self.producer:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_str,
                eventhub_name=self.eventhub_name
            )

        async with self.producer:
            event_data_batch = await self.producer.create_batch()
            event = EventData(json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "event": event
            }))
            event_data_batch.add(event)
            await self.producer.send_batch(event_data_batch)

    async def cleanup(self) -> None:
        if self.producer:
            await self.producer.close()

class KafkaEventHandler(EventHandler):
    """Handler for sending events to Kafka"""
    def __init__(self, bootstrap_servers: str, topic: str, **kafka_config):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.kafka_config = kafka_config
        self.producer = None

    async def initialize(self):
        """Initialize Kafka producer"""
        if not self.producer:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                **self.kafka_config
            )
            await self.producer.start()

    async def handle_event(self, event: dict) -> None:
        if not self.producer:
            await self.initialize()
        
        event_data = json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "event": event
        }).encode('utf-8')
        
        await self.producer.send_and_wait(
            topic=self.topic,
            value=event_data,
            key=str(event.get('event', {}).get('event_type', '')).encode('utf-8')
        )

    async def cleanup(self) -> None:
        if self.producer:
            await self.producer.stop()

class HomeAssistantClient:
    """Home Assistant WebSocket client that yields events"""
    def __init__(self, websocket_url: str, access_token: str):
        self.websocket_url = websocket_url
        self.access_token = access_token
        self.message_id = 1
        self.stats = EventStats()
        self._start_time = datetime.utcnow()

    async def authenticate(self, websocket) -> None:
        """Authenticate with Home Assistant"""
        auth_message = {
            "type": "auth",
            "access_token": self.access_token
        }
        auth_response = await websocket.recv() # Wait for auth request
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

    def update_stats(self, event_data: dict, processing_time: float) -> None:
        """Update monitoring statistics"""
        self.stats.total_events += 1
        self.stats.last_event_time = datetime.utcnow()
        
        event_type = event_data['event']['event_type']
        if event_type not in self.stats.recent_event_types:
            self.stats.recent_event_types.append(event_type)
        
        # Calculate event size
        event_size = len(json.dumps(event_data).encode('utf-8'))
        
        # Update detailed metrics
        self.stats.update_event_metrics(event_type, event_size, processing_time)
        
        # Update events per minute
        current_time = time.time()
        if not hasattr(self, '_event_times'):
            self._event_times = deque(maxlen=60)
        self._event_times.append(current_time)
        one_minute_ago = current_time - 60
        
        while self._event_times and self._event_times[0] < one_minute_ago:
            self._event_times.popleft()
            
        self.stats.events_per_minute = len(self._event_times)
        self.stats.update_throughput()

    async def event_stream(self) -> AsyncGenerator[dict, None]:
        """Generator that yields Home Assistant events"""
        while True:
            try:
                self.stats.connection_status = "Connecting..."
                async with websockets.connect(self.websocket_url) as websocket:
                    await self.authenticate(websocket)
                    await self.subscribe_to_events(websocket)
                    self.stats.connection_status = "Connected"
                    self.stats.last_error = None
                    
                    while True:
                        start_time = time.time()
                        message = await websocket.recv()
                        event_data = json.loads(message)
                        
                        if event_data.get("type") == "event":
                            processing_time = time.time() - start_time
                            self.update_stats(event_data, processing_time)
                            yield event_data

            except Exception as e:
                self.stats.connection_status = "Error"
                self.stats.last_error = str(e)
                self.stats.add_error(str(e))
                await asyncio.sleep(5)

# Enhanced Flask application with detailed monitoring
app = Flask(__name__)
processor = None

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Event Forwarder Dashboard</title>
    <meta http-equiv="refresh" content="10">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.7.0/chart.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
        .connected { background-color: #90EE90; }
        .error { background-color: #FFB6C1; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .chart-container { height: 300px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
        .metric-value { font-size: 24px; font-weight: bold; }
        .metric-label { color: #666; }
    </style>
</head>
<body>
    <h1>Event Forwarder Dashboard</h1>
    
    <div class="grid">
        <div class="card">
            <h2>Status</h2>
            <div class="status {{ 'connected' if stats.connection_status == 'Connected' else 'error' }}">
                Connection Status: {{ stats.connection_status }}
            </div>
            <div class="metric-value">{{ stats.total_events }}</div>
            <div class="metric-label">Total Events</div>
        </div>
        
        <div class="card">
            <h2>Performance</h2>
            <div class="metric-value">{{ "%.2f"|format(stats.events_per_minute) }}</div>
            <div class="metric-label">Events/Minute</div>
            <div class="metric-value">{{ "%.2f"|format(stats.processing_times|list|mean if stats.processing_times else 0) }}ms</div>
            <div class="metric-label">Avg Processing Time</div>
        </div>
    </div>

    <div class="card">
        <h2>Throughput History</h2>
        <canvas id="throughputChart"></canvas>
    </div>

    <div class="card">
        <h2>Event Type Distribution</h2>
        <table>
            <tr>
                <th>Type</th>
                <th>Count</th>
                <th>Last Seen</th>
                <th>Avg Size</th>
                <th>Errors</th>
            </tr>
            {% for metrics in stats.event_metrics.values()|sort(attribute='count', reverse=True) %}
            <tr>
                <td>{{ metrics.event_type }}</td>
                <td>{{ metrics.count }}</td>
                <td>{{ metrics.last_seen.strftime('%Y-%m-%d %H:%M:%S') if metrics.last_seen else 'N/A' }}</td>
                <td>{{ "%.2f"|format(metrics.avg_size_bytes/1024) }} KB</td>
                <td>{{ metrics.error_count }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>

    <div class="card">
        <h2>Recent Errors</h2>
        <table>
            <tr>
                <th>Time</th>
                <th>Error</th>
                <th>Event Type</th>
            </tr>
            {% for error in stats.errors|reverse %}
            <tr>
                <td>{{ error.timestamp.strftime('%Y-%m-%d %H:%M:%S') }}</td>
                <td>{{ error.error }}</td>
                <td>{{ error.event_type or 'N/A' }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>

    <script>
        const throughputData = {{ throughput_data|tojson }};
        new Chart(document.getElementById('throughputChart'), {
            type: 'line',
            data: {
                labels: throughputData.map(d => d.timestamp),
                datasets: [{
                    label: 'Events per Minute',
                    data: throughputData.map(d => d.events_per_minute),
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    </script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    throughput_data = [
        {
            'timestamp': entry['timestamp'].strftime('%H:%M:%S'),
            'events_per_minute': entry['events_per_minute']
        }
        for entry in processor.ha_client.stats.throughput_history
    ]
    
    return render_template_string(
        DASHBOARD_TEMPLATE,
        stats=processor.ha_client.stats,
        throughput_data=throughput_data
    )

# Enhanced Flask routes
@app.route('/api/metrics')
def get_metrics():
    """Get detailed metrics"""
    return jsonify(processor.ha_client.get_performance_metrics())

@app.route('/api/events/recent')
def get_recent_events():
    """Get recent events"""
    return jsonify(list(processor.ha_client.stats.recent_events))

@app.route('/api/events/types')
def get_event_types():
    """Get event type distribution"""
    return jsonify(processor.ha_client.stats.get_event_type_distribution())

@app.route('/api/errors')
def get_errors():
    """Get error history"""
    return jsonify(list(processor.ha_client.stats.error_history))

def main():
    global processor
    
    # Create Home Assistant client
    ha_client = HomeAssistantClient(
        websocket_url="ws://your-ha-instance:8123/api/websocket",
        access_token="your_long_lived_access_token"
    )
    
    # Choose your event handler
    # For Kafka:
    event_handler = KafkaEventHandler(
        bootstrap_servers='localhost:9092',
        topic='home-assistant-events'
    )
    
    # Create processor
    processor = EventProcessor(ha_client, event_handler)
    
    # Start event processing in separate thread
    event_thread = threading.Thread(target=run_event_loop, args=(processor,))
    event_thread.daemon = True
    event_thread.start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)

if __name__ == "__main__":
    # Install required packages:
    # pip install flask websockets azure-eventhub aiokafka
    main()


@app.route('/api/metrics')
def metrics():
    stats = processor.ha_client.stats
    return jsonify({
        'general': {
            'total_events': stats.total_events,
            'events_per_minute': stats.events_per_minute,
            'uptime_seconds': (datetime.utcnow() - processor.ha_client._start_time).total_seconds(),
            '