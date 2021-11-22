from sources.facebook import FacebookSource
from typing import Any, Dict, List, Optional, Type
from pathlib import Path
from models.event import Event
from datetime import datetime

from sources.google import GoogleSource
import pytz


class Preprocesser:
    """
    This is something that would run on the client (e.g. the CLI).
    """

    def build_dataset(self, path: str):
        extensions = ['.json', '.vcf', '.ics', '.mbox']
        dataset: Dict[str, Any] = {}
        for extension in extensions:
            full_paths = list(Path(path).glob("**/*" + extension))
            for row in full_paths:
                node = str(row).replace(path, "")
                dataset[node] = open(row).read()
        return dataset


if __name__ == '__main__':
    driver = Preprocesser()
    dataset = driver.build_dataset(
        path='/Users/shomil/Documents/github/school/research/engine/test_data/'
    )

    events: List[Event] = []
    sources: List[Type] = {
        'google': GoogleSource,
        'facebook': FacebookSource
    }
    for company, Source in sources.items():
        filtered = {k.replace(company + '/', ''): v for k,
                    v in dataset.items() if k.startswith(company)}
        events.extend(Source(dataset=filtered).get_events())

    for event in sorted(events, key=lambda e: e.timestamp):
        print(event.timestamp, event.title)
