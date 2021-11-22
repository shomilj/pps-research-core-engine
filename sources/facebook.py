from os import times
from typing import Any, Dict, List, Optional, Set
from models.event import Event
from sources.base import BaseSource
import json

COMPANY = 'Facebook'


class FacebookSource(BaseSource):

    def __init__(self, dataset: Dict[str, Any]) -> None:
        self.friends: List[str] = {}
        super().__init__(dataset)

    def get_events(self) -> List[Event]:
        self.parse_friends()
        self.parse_about_you_notifications()
        self.parse_about_you_preferences()
        self.parse_about_you_visited_pages()
        self.parse_ads_and_businesses_advertisers_who_youve_interacted_with()
        self.parse_ads_and_businesses_your_off_facebook_activity()
        self.parse_comments()
        self.parse_events_event_invitations()
        self.parse_events_event_responses()
        self.parse_groups_your_group_membership_activity()
        self.parse_groups_your_posts_and_comments_in_groups()
        self.parse_location_history()
        self.parse_search_history_your_search_history()
        self.parse_security_and_login_account_activity()
        self.parse_security_and_login_authorized_logins()
        return self.events

    def extract_names(self, content: str) -> Set[str]:
        mentioned: Set[str] = set()
        for friend in self.friends:
            if friend in content:
                mentioned.add(friend)
        return mentioned

    def parse_friends(self):
        rows = self.safe_load_data(
            path='friends/friends.json',
            node='friends'
        )
        self.friends = set([row['name'] for row in rows])
        self.events.extend([
            Event(
                company=COMPANY,
                source='Friends',
                key='friend_added',
                timestamp=row['timestamp'],
                title=f'You became friends with {row["name"]}.',
                names={row['name']}
            ) for row in rows
        ])

    def parse_about_you_notifications(self):
        rows = self.safe_load_data(
            path='about_you/notifications.json',
            node='notifications'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='About You',
                key='notification_received',
                timestamp=row['timestamp'],
                title='You received a notification: ' + row['text'],
                content=row['href'],
                names=self.extract_names(row['text'])
            ) for row in rows
        ])

    def parse_about_you_preferences(self):
        rows = self.safe_load_data(
            path='about_you/preferences.json',
            node='preferences'
        )
        self.parse_things(
            rows=rows,
            key='prioritized_page',
            name='Favorites',
            title_prefix='You prioritized a page in your news feed: '
        )
        self.parse_things(
            rows=rows,
            key='blocked_contact',
            name='Messenger Contacts You\'ve Blocked',
            title_prefix='You blocked a Messenger contact: '
        )
        self.parse_things(
            rows=rows,
            key='dismissed_chat_notification',
            name='Language ',
            title_prefix='You dismissed a chat notification from a page: '
        )

    def parse_about_you_visited_pages(self):
        rows = self.safe_load_data(
            path='about_you/visited.json',
            node='visited_things'
        )
        self.parse_things(
            rows=rows,
            key='profile_visit',
            name='Profile visits',
            title_prefix='You visited a profile: ',
        )
        self.parse_things(
            rows=rows,
            key='page_visit',
            name='Page visits',
            title_prefix='You visited a page: '
        )
        self.parse_things(
            rows=rows,
            key='event_visit',
            name='Events visited',
            title_prefix='You visited an event page: '
        )
        self.parse_things(
            rows=rows,
            key='group_visit',
            name='Groups visited',
            title_prefix='You visited a group: '
        )

    def parse_ads_and_businesses_advertisers_who_youve_interacted_with(self):
        rows = self.safe_load_data(
            path='ads_and_businesses/advertisers_who_you\'ve_interacted_with.json',
            node='history'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Ads and Businesses',
                key='ad_interaction',
                timestamp=row['timestamp'],
                title=f'You interacted with an advertisement: {row["title"]}.',
            ) for row in rows
        ])

    def parse_ads_and_businesses_your_off_facebook_activity(self):
        categories = self.safe_load_data(
            path='ads_and_businesses/your_off-facebook_activity.json',
            node='off_facebook_activity'
        )
        for category in categories:
            advertiser_name = category['name']
            self.events.extend([
                Event(
                    company=COMPANY,
                    source='Ads and Businesses',
                    key='off_facebook_activity_record',
                    timestamp=event['timestamp'],
                    title=f'Facebook logged off-Facebook activity on: {advertiser_name} (type: {event["type"]})',
                    metadata=event
                ) for event in category.get('events', [])
            ])

    def parse_comments(self):
        rows = self.safe_load_data(
            path='comments/comments.json',
            node='comments'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Comments',
                key='comment_added',
                timestamp=row['timestamp'],
                title=row['title'],
                metadata=row['data'],
                names=self.extract_names(str(row))
            ) for row in rows
        ])

    def parse_events_event_invitations(self):
        rows = self.safe_load_data(
            path='events/event_invitations.json',
            node='events_invited'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Events',
                key='event_started',
                timestamp=row['start_timestamp'],
                title=f'An event that you were invited to began: {row["name"]}',
                metadata=row
            ) for row in rows
        ])

    def parse_events_event_responses(self):
        categories = self.safe_load_data(
            path='events/your_event_responses.json',
            node='event_responses'
        )
        options = {
            'events_joined': "An event you went to began: ",
            'events_declined': "An event you declined began: ",
            'events_interested': "An event you were interested in began: "
        }
        for option, title_prefix in options.items():
            if option in categories:
                self.events.extend([
                    Event(
                        company=COMPANY,
                        source='Events',
                        key='event_started',
                        timestamp=row['start_timestamp'],
                        title=title_prefix + row['name'],
                        metadata=row
                    ) for row in categories[option]
                ])

    def parse_groups_your_group_membership_activity(self):
        rows = self.safe_load_data(
            path='groups/your_group_membership_activity.json',
            node='groups_joined'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Groups',
                key='group_joined',
                timestamp=row['timestamp'],
                title=row['title']
            ) for row in rows
        ])

    def parse_groups_your_posts_and_comments_in_groups(self):
        rows = self.safe_load_data(
            path='groups/your_posts_and_comments_in_groups.json',
            node='group_posts'
        )
        rows = rows.get('activity_log_data', [])
        self.events.extend([
            Event(
                company=COMPANY,
                source='Groups',
                key='group_post',
                timestamp=row['timestamp'],
                title=row['title'],
                metadata=row['data'],
                names=self.extract_names(str(row))
            ) for row in rows
        ])

    def parse_likes_and_reactions_posts_and_comments(self):
        rows = self.safe_load_data(
            path='likes_and_reactions/posts_and_comments.json',
            node='reactions'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Likes and Reactions',
                key='post_reaction',
                timestamp=row['timestamp'],
                title=row['title'],
                metadata=row['data'],
                names=self.extract_names(str(row))
            ) for row in rows
        ])

    def parse_location_history(self):
        rows = self.safe_load_data(
            path='location/location_history.json',
            node='location_history'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Location History',
                key='location_logged',
                timestamp=row['creation_timestamp'],
                title=f'Your location was recorded in {row["name"]}',
                location=row['coordinate']
            ) for row in rows
        ])

    def parse_search_history_your_search_history(self):
        rows = self.safe_load_data(
            path='search_history/your_search_history.json',
            node='searches'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Search',
                key='search_recorded',
                timestamp=row['timestamp'],
                title='You searched Facebook.',
                metadata=row['data'],
                names=self.extract_names(str(row))
            ) for row in rows
        ])

    def parse_security_and_login_account_activity(self):
        rows = self.safe_load_data(
            path='security_and_login_information/account_activity.json',
            node='account_activity'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login Information',
                key='account_activity',
                timestamp=row['timestamp'],
                title=f'Account activity recorded: {row["action"]}',
                metadata=row
            ) for row in rows
        ])

    def parse_security_and_login_authorized_logins(self):
        rows = self.safe_load_data(
            path='security_and_login_information/authorized_logins.json',
            node='recognized_devices'
        )
        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login Information',
                key='account_activity',
                timestamp=row['created_timestamp'],
                title=f'Authorized login recorded: {row["name"]}',
                metadata=row
            ) for row in rows
        ])

    def parse_things(self, rows: List[Any], key: str, name: str, title_prefix: str):
        for row in rows:
            if row['name'] == name:
                self.events.extend([
                    Event(
                        company=COMPANY,
                        source='About You',
                        key=key,
                        timestamp=entry['timestamp'],
                        title=title_prefix + entry['data']['name'],
                        names=self.extract_names(entry['data']['name']),
                        metadata=entry['data']
                    ) for entry in row['entries']
                ])

    def safe_load_data(self, path: str, node: str) -> Optional[List[Dict[str, Any]]]:
        if path in self.dataset:
            data = self.dataset[path]
            formatted = json.loads(data)
            if node in formatted:
                return formatted[node]
        return []
