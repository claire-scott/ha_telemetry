# ha_event_forwarder/config.py

import os
import yaml
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

@dataclass
class Config:
    # Home Assistant configuration
    ha_url: str
    ha_token: str
    
    # Handler configuration
    handler_type: str  # 'console', 'kafka', or 'azure'
    
    # Kafka configuration
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
    
    # Azure configuration
    eventhub_connection_str: Optional[str] = None
    eventhub_name: Optional[str] = None
    
    # Flask configuration
    flask_host: str = '0.0.0.0'
    flask_port: int = 5000
    debug: bool = True

    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables"""
        return cls(
            # Required configurations
            ha_url=os.environ.get('HA_URL', ''),
            ha_token=os.environ.get('HA_TOKEN', ''),
            handler_type=os.environ.get('HANDLER_TYPE', 'console'),
            
            # Optional configurations
            kafka_bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
            kafka_topic=os.environ.get('KAFKA_TOPIC'),
            eventhub_connection_str=os.environ.get('EVENTHUB_CONNECTION_STRING'),
            eventhub_name=os.environ.get('EVENTHUB_NAME'),
            
            # Flask configurations
            flask_host=os.environ.get('FLASK_HOST', '0.0.0.0'),
            flask_port=int(os.environ.get('FLASK_PORT', '5000')),
            debug=os.environ.get('FLASK_DEBUG', '').lower() == 'true'
        )

    @classmethod
    def from_yaml(cls, path: str) -> 'Config':
        """Create configuration from YAML file"""
        with open(path, 'r') as f:
            config_data = yaml.safe_load(f)
            
        return cls(
            ha_url=config_data.get('home_assistant', {}).get('url', ''),
            ha_token=config_data.get('home_assistant', {}).get('token', ''),
            handler_type=config_data.get('handler', {}).get('type', 'console'),
            kafka_bootstrap_servers=config_data.get('kafka', {}).get('bootstrap_servers'),
            kafka_topic=config_data.get('kafka', {}).get('topic'),
            eventhub_connection_str=config_data.get('azure', {}).get('connection_string'),
            eventhub_name=config_data.get('azure', {}).get('eventhub_name'),
            flask_host=config_data.get('flask', {}).get('host', '0.0.0.0'),
            flask_port=int(config_data.get('flask', {}).get('port', 5000)),
            debug=config_data.get('flask', {}).get('debug', False)
        )

    def validate(self) -> None:
        """Validate the configuration"""
        if not self.ha_url:
            raise ValueError("Home Assistant URL is required")
        if not self.ha_token:
            raise ValueError("Home Assistant token is required")
        
        if self.handler_type not in ['console', 'kafka', 'azure']:
            raise ValueError(f"Invalid handler type: {self.handler_type}")
            
        if self.handler_type == 'kafka':
            if not self.kafka_bootstrap_servers:
                raise ValueError("Kafka bootstrap servers required for kafka handler")
            if not self.kafka_topic:
                raise ValueError("Kafka topic required for kafka handler")
                
        if self.handler_type == 'azure':
            if not self.eventhub_connection_str:
                raise ValueError("Event Hub connection string required for azure handler")
            if not self.eventhub_name:
                raise ValueError("Event Hub name required for azure handler")

def load_config() -> Config:
    """
    Load configuration from multiple sources in order of precedence:
    1. Environment variables
    2. YAML configuration file (if specified)
    3. Default values
    """
    # Try to load from YAML if config path is specified
    config_path = os.environ.get('CONFIG_PATH')
    if config_path and Path(config_path).exists():
        config = Config.from_yaml(config_path)
    else:
        # Fall back to environment variables
        config = Config.from_env()
    
    # Validate the configuration
    config.validate()
    return config