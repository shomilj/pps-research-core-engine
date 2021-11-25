from typing import Any, Dict, List, Optional, Set

from examples.search.models.event import Event
from lens.sources.FacebookSource import FacebookSource

COMPANY = 'Facebook'


class FacebookIndex:

    def __init__(self, source: FacebookSource, full_name: str) -> None:
        self.source = source
        self.full_name = full_name
        self.events: List[Event] = []
        self.friends: Set[str] = set()

    def get_events(self) -> List[Event]:
        self.parse_friends()
        self.parse_ads_information()
        self.parse_apps_and_websites_off_of_facebook()
        self.parse_comments_and_reactions()
        self.parse_events()
        self.parse_groups()
        self.parse_location()
        self.parse_messages()
        self.parse_notifications()
        self.parse_polls()
        self.parse_search()
        self.parse_security_and_login_information()
        return self.events

    def extract_names(self, content: str) -> Set[str]:
        mentioned: Set[str] = set()
        for friend in self.friends:
            if friend.lower() in content.lower() or friend.split(' ')[0] in content.lower():
                mentioned.add(friend)
        return mentioned

    def parse_friends(self):
        categories = [{
            'path': 'friend_requests_sent.json',
            'title_prefix': 'You sent a friend request to: ',
            'key': 'friend_request_sent',
            'node': 'sent_requests_v2'
        }, {
            'path': 'friends.json',
            'title_prefix': 'You became friends with: ',
            'key': 'friend_added',
            'node': 'friends_v2'
        }, {
            'path': 'rejected_friend_requests.json',
            'title_prefix': 'You rejected a friend request from: ',
            'key': 'rejected_friend_request',
            'node': 'rejected_requests_v2'
        }, {
            'path': 'removed_friends.json',
            'title_prefix': 'You unfriended a friend: ',
            'key': 'friend_removed',
            'node': 'deleted_friends_v2'
        }]

        for category in categories:
            rows = self.safe_load_data(
                path='friends_and_followers/' + category['path'],
                node=category['node']
            )
            self.friends.update(set([row['name'] for row in rows]))
            self.events.extend([
                Event(
                    company=COMPANY,
                    source='Friends',
                    key='friend_added',
                    timestamp=row['timestamp'],
                    title=category['title_prefix'] + row["name"],
                    names={row['name']}
                ) for row in rows
            ])

    def parse_ads_information(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Ads',
                key='ad_interaction',
                timestamp=row['timestamp'],
                title=f'You interacted with an advertiser on Facebook: {row["title"]}'
            ) for row in self.safe_load_data(
                path='ads_information/advertisers_you\'ve_interacted_with.json',
                node='history_v2'
            )
        ])

    def parse_apps_and_websites_off_of_facebook(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Apps and Websites',
                key='installed_app',
                timestamp=row['added_timestamp'],
                title=f'You connected an app to Facebook: {row["name"]}',
                metadata=row
            ) for row in self.safe_load_data(
                path='apps_and_websites_off_of_facebook/apps_and_websites.json',
                node='installed_apps_v2'
            )
        ])

        categories = self.safe_load_data(
            path='apps_and_websites_off_of_facebook/your_off-facebook_activity.json',
            node='off_facebook_activity_v2'
        )
        for category in categories:
            advertiser_name = category['name']
            self.events.extend([
                Event(
                    company=COMPANY,
                    source='Apps and Websites Off of Facebook',
                    key='off_facebook_activity_record',
                    timestamp=event['timestamp'],
                    title=f'Facebook logged off-Facebook activity on: {advertiser_name} (type: {event["type"]})',
                    metadata=event
                ) for event in category.get('events', [])
            ])

    def parse_comments_and_reactions(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Comments and Reactions',
                key='comment_added',
                timestamp=row['timestamp'],
                title=row["title"],
                metadata=row["data"]
            ) for row in self.safe_load_data(
                path='comments_and_reactions/comments.json',
                node='comments_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Comments and Reactions',
                key='reaction_added',
                timestamp=row['timestamp'],
                title=row["title"],
                metadata=row["data"]
            ) for row in self.safe_load_data(
                path='comments_and_reactions/posts_and_comments.json',
                node='reactions_v2'
            )
        ])

    def parse_events(self):
        rows = self.safe_load_data(
            path='events/your_event_responses.json',
            node='event_responses_v2'
        )
        if rows:
            rows = rows.get('events_joined', [])
            self.events.extend([
                Event(
                    company=COMPANY,
                    source='Events',
                    key='event_started',
                    timestamp=row['start_timestamp'],
                    title="You RSVP'd to an event: " + row["name"],
                    metadata=row
                ) for row in rows
            ])

    def parse_groups(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Groups',
                key='group_comment',
                timestamp=row['timestamp'],
                title=row["title"],
                metadata=row.get("data"),
                names=self.extract_names(str(row))
            ) for row in self.safe_load_data(
                path='groups/your_comments_in_groups.json',
                node='group_comments_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Groups',
                key='group_joined',
                timestamp=row['timestamp'],
                title=row["title"],
            ) for row in self.safe_load_data(
                path='groups/your_group_membership_activity.json',
                node='groups_joined_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Groups',
                key='group_post',
                timestamp=row['timestamp'],
                title=row["title"],
                metadata=row["data"],
                names=self.extract_names(str(row))
            ) for row in self.safe_load_data(
                path='groups/your_posts_in_groups.json',
                node='group_posts_v2'
            )
        ])

    def parse_location(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Location',
                key='location_logged',
                timestamp=row['creation_timestamp'],
                title=f'Facebook recorded your location in {row["name"]}.',
                location=row['coordinate']
            ) for row in self.safe_load_data(
                path='location/location_history.json',
                node='location_history_v2'
            )
        ])

    def parse_messages(self):
        for thread in self.source.glob('*.json'):
            if not thread.endswith('message_1.json'):
                continue
            thread = self.source.read_json(thread)
            for message in thread.get('messages'):
                thread_metadata = {k: v for k,
                                   v in thread.items() if k != 'messages'}
                message_metadata = {
                    'thread_details': thread_metadata,
                    'message_details': message
                }

                participants = [row['name'] for row in thread['participants']]
                is_sender = message.get('sender_name') == self.full_name
                num_participants = len(participants)
                group_name = thread.get('title')
                content = message.get('content', '[empty body]')

                if num_participants == 1:
                    title = 'You sent a message to yourself: ' + content
                if num_participants == 2:
                    dm_target = [
                        p for p in participants if p != self.full_name
                    ][0]
                    if is_sender:
                        title = f'You sent a DM to {dm_target}: {content}'
                    else:
                        title = f'You received a DM from {dm_target}: {content}'

                elif num_participants >= 2:
                    if is_sender:
                        title = f'You sent a message to in the group "{group_name}": {content}'
                    else:
                        title = f'You received a message in the group "{group_name}": {content}'

                else:
                    continue

                self.events.append(Event(
                    company=COMPANY,
                    source='Messenger',
                    key='messenger_event',
                    timestamp=message['timestamp_ms'] / 1000,
                    content=message.get('content', None),
                    metadata=message_metadata,
                    title=title
                ))

    def parse_notifications(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Notifications',
                key='notification_sent',
                timestamp=row['timestamp'],
                title=f'Facebook sent you a notification: {row["text"]}',
                names=self.extract_names(str(row)),
                content=row['href'],
            ) for row in self.safe_load_data(
                path='notifications/notifications.json',
                node='notifications_v2'
            )
        ])

    def parse_polls(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Polls',
                key='poll_vote',
                timestamp=row['timestamp'],
                title=row["title"],
                names=self.extract_names(str(row)),
                metadata=row.get('attachments')
            ) for row in self.safe_load_data(
                path='polls/polls_you_voted_on.json',
                node='poll_votes_v2'
            )
        ])

    def parse_search(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Search',
                key='search',
                timestamp=row['timestamp'],
                title="You searched Facebook for: " +
                    row.get('data', [{}])[0].get('text'),
                names=self.extract_names(str(row)),
                metadata=row.get('attachments')
            ) for row in self.safe_load_data(
                path='search/your_search_history.json',
                node='searches_v2'
            )
        ])

    def parse_security_and_login_information(self):
        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login',
                key='account_event',
                timestamp=row['timestamp'],
                title='Facebook Account Event: ' +
                    row['action'] + ' near ' + row['city'],
                metadata=row
            ) for row in self.safe_load_data(
                path='security_and_login_information/account_activity.json',
                node='account_activity_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login',
                key='account_event',
                timestamp=row['created_timestamp'],
                title='Signed into Facebook from a new device: ' + row['name'],
                metadata=row
            ) for row in self.safe_load_data(
                path='security_and_login_information/authorized_logins.json',
                node='recognized_devices_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login',
                key='account_event',
                timestamp=row['timestamp'],
                title='IP Address Activity Record: ' + row['action'],
                metadata=row
            ) for row in self.safe_load_data(
                path='security_and_login_information/ip_address_activity.json',
                node='used_ip_address_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login',
                key='account_event',
                timestamp=row['timestamp'],
                title='Login/Logout Event: ' + row['action'],
                metadata=row
            ) for row in self.safe_load_data(
                path='security_and_login_information/logins_and_logouts.json',
                node='account_accesses_v2'
            )
        ])

        self.events.extend([
            Event(
                company=COMPANY,
                source='Security and Login',
                key='account_event',
                timestamp=row['created_timestamp'],
                title='Active Session near: ' + row['location'],
                metadata=row
            ) for row in self.safe_load_data(
                path='security_and_login_information/where_you\'re_logged_in.json',
                node='active_sessions_v2'
            )
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
        return self.source.read_node(path=path, node=node)
