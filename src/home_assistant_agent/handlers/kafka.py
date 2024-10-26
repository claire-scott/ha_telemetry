import json
from datetime import datetime
from aiokafka import AIOKafkaProducer
from .base import EventHandler

class KafkaEventHandler(EventHandler):
    """Handler for sending events to Kafka"""
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.stats = {
            'messages_sent': 0,
            'bytes_sent': 0,
            'errors': 0,
            'last_error': None,
            'last_success': None
        }

    async def _ensure_producer(self):
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            await self.producer.start()

    async def handle_event(self, event: dict) -> None:
        try:
            await self._ensure_producer()
            
            event_with_metadata = {
                'timestamp': datetime.utcnow().isoformat(),
                'event': event
            }
            
            await self.producer.send_and_wait(self.topic, event_with_metadata)
            
            message_size = len(json.dumps(event_with_metadata))
            self.stats['messages_sent'] += 1
            self.stats['bytes_sent'] += message_size
            self.stats['last_success'] = datetime.utcnow().isoformat()
            
        except Exception as e:
            self.stats['errors'] += 1
            self.stats['last_error'] = str(e)
            print(f"Kafka: Error: {str(e)}")
            raise

    async def cleanup(self) -> None:
        if self.producer:
            await self.producer.stop()

    def get_stats(self) -> dict:
        return self.stats