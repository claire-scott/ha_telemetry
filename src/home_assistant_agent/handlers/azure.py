from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import json
from .base import EventHandler

class EventHubHandler(EventHandler):
    """Handler for sending events to Azure Event Hub"""
    def __init__(self, connection_str: str, eventhub_name: str):
        self.connection_str = connection_str
        self.eventhub_name = eventhub_name
        self.producer = None
        self.stats = {
            'messages_sent': 0,
            'bytes_sent': 0,
            'errors': 0,
            'last_error': None,
            'last_success': None
        }

    async def handle_event(self, event: dict) -> None:
        try:
            if not self.producer:
                self.producer = EventHubProducerClient.from_connection_string(
                    conn_str=self.connection_str,
                    eventhub_name=self.eventhub_name
                )

            async with self.producer:
                event_data_batch = await self.producer.create_batch()
                event_with_metadata = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "event": event
                }
                event_data = EventData(json.dumps(event_with_metadata))
                event_data_batch.add(event_data)
                await self.producer.send_batch(event_data_batch)

            message_size = len(json.dumps(event_with_metadata))
            self.stats['messages_sent'] += 1
            self.stats['bytes_sent'] += message_size
            self.stats['last_success'] = datetime.utcnow().isoformat()

        except Exception as e:
            self.stats['errors'] += 1
            self.stats['last_error'] = str(e)
            raise

    async def cleanup(self) -> None:
        if self.producer:
            await self.producer.close()

    def get_stats(self) -> dict:
        return self.stats