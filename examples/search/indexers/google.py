import os
from typing import Any, Dict, List, Set
import vobject
import re
from ics import Calendar
import mailbox
import tempfile
from tqdm import tqdm

from typing import Any, Dict, List, Set
from examples.search.indexers.helpers.gmail import GmailMboxMessage

from examples.search.models.event import Event
from lens.sources.GoogleSource import GoogleSource

COMPANY = 'Google'


def clean_names(name):
    return re.sub(' +', ' ', name).strip()


class GoogleIndex:

    def __init__(self, source: GoogleSource) -> None:
        self.source = source
        self.events: List[Event] = []
        self.contacts: List[Dict] = []
        self.email_map: Dict[str, str] = {}

    def get_events(self) -> List[Event]:
        self.parse_contacts()
        self.parse_calendar()
        self.parse_email()
        return self.events

    def extract_names(self, content: str) -> Set[str]:
        mentioned: Set[str] = set()
        for contact in self.contacts:
            name, email = contact['name'], contact['email']
            if name in content or email in content:
                mentioned.add(name)
        return mentioned

    def parse_contacts(self):
        paths = self.source.get_contact_paths()
        for path in paths:
            print(path)
            for contact in vobject.readComponents(open(path).read()):
                names = contact.contents.get('n')
                if names and len(names) > 0:
                    name = clean_names(str(names[0].value))
                    email = contact.email.value
                    self.contacts.append({
                        'name': name,
                        'email': email
                    })
                    self.email_map[email] = name

    def parse_calendar(self):
        for path in self.source.get_calendar_paths():
            print(path)
            c = Calendar(open(path).read())
            for event in c.events:
                names = set()
                for attendee in event.attendees:
                    if attendee.email in self.email_map:
                        names.add(self.email_map[attendee.email])
                self.events.append(Event(
                    company=COMPANY,
                    source='Calendar',
                    key='event',
                    timestamp=event.begin.float_timestamp,
                    title='Calendar Event: ' + event.name,
                    metadata=event.__dict__,
                    names=names,
                ))

    def parse_email(self):
        for path in self.source.get_mailbox_paths():
            print(path)
            m = mailbox.mbox(path)
            for email_data in tqdm(m):
                email = GmailMboxMessage(email_data).parse_email()
                if email['subject'].startswith('?'):
                    continue
                self.events.append(Event(
                    company=COMPANY,
                    source='Email',
                    key='email',
                    timestamp=email['timestamp'],
                    title='Email: ' + email['subject'],
                    content=email['text'],
                    metadata=email,
                    names=self.extract_names(str(email))
                ))
