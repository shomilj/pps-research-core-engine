from datetime import datetime
from io import StringIO
from os import times
import os
from typing import Any, Dict, List, Optional, Set
from models.event import Event
from sources.base import BaseSource
import json
import vobject
import re
from ics import Calendar
import mailbox
import tempfile
from tqdm import tqdm
from pprint import pprint

from sources.utils.gmail import GmailMboxMessage

COMPANY = 'Google'


def clean_names(name): return re.sub(' +', ' ', name).strip()


class GoogleSource(BaseSource):

    def __init__(self, dataset: Dict[str, Any]) -> None:
        self.contacts: List[Dict] = []
        self.email_map: Dict[str, str] = {}
        super().__init__(dataset)

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
        for contact in vobject.readComponents(self.dataset['Contacts/All Contacts/All Contacts.vcf']):
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
        calendars = {k: v for k, v in self.dataset.items() if '.ics' in k}
        for path in calendars:
            c = Calendar(self.dataset[path])
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
        inboxes = {k: v for k, v in self.dataset.items() if '.mbox' in k}
        for inbox in inboxes.values():
            fd, path = tempfile.mkstemp()
            try:
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(inbox)
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

            finally:
                os.remove(path)
