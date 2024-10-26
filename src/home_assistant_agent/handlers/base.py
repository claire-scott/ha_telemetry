from abc import ABC, abstractmethod
from typing import Dict, Any

class EventHandler(ABC):
    """Abstract base class for event handlers"""
    @abstractmethod
    async def handle_event(self, event: dict) -> None:
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get handler-specific statistics"""
        pass