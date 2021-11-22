from abc import abstractmethod, abstractstaticmethod
from typing import Any, Dict, List

from models.event import Event


class BaseSource:
    """
    All sources inherit from this base source.
    """

    def __init__(self, dataset: Dict[str, Any]) -> None:
        self.dataset = dataset
        self.events: List[Event] = []

    @abstractmethod
    def get_events(self) -> List[Event]:
        raise NotImplementedError()