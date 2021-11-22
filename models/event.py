from datetime import datetime
from time import time
from typing import Any, Dict, Optional, Set, Tuple, cast

import pytz


class Event:
    """
    An Event is a piece of information from a third-party data source that occurred at a particular timestamp.
    """

    def __init__(self, company: str, source: str, key: str, timestamp: Any,
                 title: str, content: Optional[Any] = None, metadata: Optional[Dict] = None,
                 names: Optional[Set[str]] = None, location: Optional[Dict[str, float]] = None) -> None:
        """
        Args:
            company (str): The third party that this event originates from (e.g. Google, Facebook, etc.)
            source (str): The product/data source that this event originates from (e.g. Messenger, Gmail, etc.)
            key (str): A key to identify this event within the scope of the source (e.g. email_sent, message_received)
            timestamp (Any): The timestamp at which this event occurred. May be passed in as any timestamp format.
            title (Any): A title to represent this event (e.g. "Shomil sent you a message.").
            content (Optional[Any]): A blob-like field to represent this event's primary content (e.g. email body, etc.)
            metadata (Optional[Dict]): A variable-format dictionary containing metadata attributes.
        """
        self.company = company
        self.source = source
        self.key = key
        self.timestamp = Event.parse_timestamp(timestamp)
        self.title = title
        self.content = content
        self.metadata = metadata

        # Two things that we can build an index off of
        self.names = names
        self.location = location

    @staticmethod
    def parse_timestamp(timestamp):
        """
        TODO: Build a general-purpose timestamp parser here. All timestamps should be represented in GMT and as
        epoch time (seconds since 1970). The input may be a string, an integer, or a double.
        """
        if type(timestamp) == datetime:
            timestamp = cast(datetime, timestamp)
            seconds = timestamp.replace(tzinfo=pytz.UTC).timestamp()
        else:
            seconds = timestamp

        return seconds
