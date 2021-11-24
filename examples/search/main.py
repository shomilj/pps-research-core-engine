"""
This search engine is an example application built on top of the Lens framework. It builds an index over documents,
events, people, and locations in a user's data archives, and exposes the results in a simple sort-filter-limit-aggregate
query language.

Like all applications built in Lens, this application has a preprocessing stage (in which it builds an index), and
serves client requests from a well-defined endpoint. Lens entirely handles the execution of this application. It runs
in a confidential computing node on Microsoft Azure, where the confidentiality and integrity of user data and this
application code are protected during processing-time due to the secure hardware provided by Intel SGX enclaves.
"""


from typing import Any, Dict, List, Optional
from examples.search.indexers.google import GoogleIndex

from lens.sources.FacebookSource import FacebookSource
from examples.search.indexers.facebook import FacebookIndex
from examples.search.models.event import Event
from lens.sources.GoogleSource import GoogleSource


class SearchEngine:

    def __init__(self, config: Dict[str, Any] = {}) -> None:
        """
        Creates an instance of this application for a particular user.

        Args:
            config (Dict[str, Any]): A dictionary of configuration parameters. Defaults to {}.
        """
        self.config = config
        self.events: List[Event] = []

    def query(self, args: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
        """
        The search engine exposes a /query endpoint. All endpoints accept an `args` variable. It's up to the application
        logic to define argument types and validate arguments within application logic.

        Args:
            args (Dict[str, Any], optional): Endpoint arguments. Defaults to {}.
        """
        events = self.events
        return [e.to_json() for e in events]

    def help(self) -> None:
        print(f'This index contains {len(self.events)} events.')
        event_types = set()
        for event in self.events:
            event_types.add(event.key)
        print('The possible event types are:')
        print('\n- '.join(sorted(event_types)))

    def preprocess(self, facebook_source: FacebookSource, google_source: GoogleSource):
        """
        Lens allows modules to do some preprocessing of personal data (e.g. index-building). This can then be saved to
        either the module's in-memory state (e.g. instance variables). The underlying engine takes care of serializing
        the in-memory state efficiently, and it's re-built when a user makes a query.
        """

        self.events.extend(FacebookIndex(
            source=facebook_source,
            full_name=self.config.get('full_name')
        ).get_events())

        self.events.extend(GoogleIndex(
            source=google_source
        ).get_events())
