import asyncio
import threading
from flask import Flask, jsonify, render_template_string
from typing import Optional
from datetime import datetime

from config.config import Config
from clients.home_assistant import HomeAssistantClient
from handlers.console import ConsoleEventHandler
from handlers.kafka import KafkaEventHandler
from handlers.azure import EventHubHandler
from handlers.base import EventHandler

class EventProcessor:
    """Coordinates event processing between Home Assistant client and event handler"""
    def __init__(self, ha_client: HomeAssistantClient, event_handler: EventHandler):
        self.ha_client = ha_client
        self.event_handler = event_handler

    async def process_events(self):
        """Main processing loop"""
        async for event in self.ha_client.event_stream():
            await self.event_handler.handle_event(event)

class Application:
    def __init__(self, config: Config):
        self.config = config
        self.flask_app = Flask(__name__)
        self.processor: Optional[EventProcessor] = None
        self.setup_routes()
        
    def create_handler(self) -> EventHandler:
        """Create the appropriate event handler based on configuration"""
        if self.config.handler_type == 'console':
            return ConsoleEventHandler()
        elif self.config.handler_type == 'kafka':
            if not (self.config.kafka_bootstrap_servers and self.config.kafka_topic):
                raise ValueError("Kafka configuration missing")
            return KafkaEventHandler(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                topic=self.config.kafka_topic
            )
        elif self.config.handler_type == 'azure':
            if not (self.config.eventhub_connection_str and self.config.eventhub_name):
                raise ValueError("Azure Event Hub configuration missing")
            return EventHubHandler(
                connection_str=self.config.eventhub_connection_str,
                eventhub_name=self.config.eventhub_name
            )
        else:
            raise ValueError(f"Unknown handler type: {self.config.handler_type}")

    def setup_routes(self):
        """Set up Flask routes"""
        
        @self.flask_app.route('/api/status')
        def get_status():
            if not self.processor:
                return jsonify({'status': 'not_initialized'}), 503
                
            return jsonify({
                'status': self.processor.ha_client.stats.connection_status,
                'uptime_seconds': self.processor.ha_client.get_uptime().total_seconds(),
                'total_events': self.processor.ha_client.stats.total_events,
                'events_per_minute': self.processor.ha_client.stats.events_per_minute,
                'last_event_time': self.processor.ha_client.stats.last_event_time,
                'last_error': self.processor.ha_client.stats.last_error
            })

        @self.flask_app.route('/api/metrics')
        def get_metrics():
            if not self.processor:
                return jsonify({'error': 'not_initialized'}), 503
            
            # Combine metrics from both client and handler
            metrics = self.processor.ha_client.get_performance_metrics()
            metrics['handler_stats'] = self.processor.event_handler.get_stats()
            return jsonify(metrics)

        @self.flask_app.route('/api/events/recent')
        def get_recent_events():
            if not self.processor:
                return jsonify({'error': 'not_initialized'}), 503
            return jsonify(list(self.processor.ha_client.stats.recent_events))

        @self.flask_app.route('/api/events/types')
        def get_event_types():
            if not self.processor:
                return jsonify({'error': 'not_initialized'}), 503
            return jsonify(self.processor.ha_client.stats.get_event_type_distribution())

        @self.flask_app.route('/api/errors')
        def get_errors():
            if not self.processor:
                return jsonify({'error': 'not_initialized'}), 503
            return jsonify(list(self.processor.ha_client.stats.error_history))

        @self.flask_app.route('/api/heartbeat')
        def heartbeat():
            if not self.processor:
                return jsonify({'status': 'not_initialized'}), 503
            
            timestamp = datetime.utcnow().isoformat()
            self.processor.ha_client.stats.last_heartbeat = timestamp
            return jsonify({
                'status': 'alive',
                'timestamp': timestamp
            })

    def run_event_loop(self):
        """Run the event processing loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.processor.process_events())

    def start(self):
        """Start the application"""
        # Create client and handler
        ha_client = HomeAssistantClient(
            websocket_url=self.config.ha_url,
            access_token=self.config.ha_token
        )
        event_handler = self.create_handler()
        
        # Create processor
        self.processor = EventProcessor(ha_client, event_handler)
        
        # Start event processing in separate thread
        event_thread = threading.Thread(target=self.run_event_loop)
        event_thread.daemon = True
        event_thread.start()
        
        # Start Flask app
        self.flask_app.run(
            host=self.config.flask_host,
            port=self.config.flask_port,
            debug=self.config.debug
        )

def create_app(config: Config) -> Application:
    """Application factory function"""
    return Application(config)

if __name__ == "__main__":
    # Example usage
    config = Config.from_yaml('config.yml')
    
    app = create_app(config)
    app.start()