import json
import logging
import os
from asyncio import Event, sleep
from random import randint
import types
from typing import Optional

from tornado.web import Application, RequestHandler, authenticated
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop

import graphene
from graphene_tornado.tornado_graphql_handler import TornadoGraphQLHandler


FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)

MAX = 999999
DELAY_FOR_CHECK_USERS = 5
STATUS_PATH = '/api/v1/status'
MESSAGES_PATH = '/api/v1/messages'
STATUS_ONLINE = 'ok'
DATA_FILE = 'data.json'
ONLINE_MAP = {
    None: None,
    'true': True,
    'false': False
}
GLOBAL_STATE = []

class PinGenerator:
    def __init__(self, items: Optional[list] = None):
        items = items or []
        self._items = set(items)

    def get(self) -> str:
        num = randint(1, MAX)
        while num in self._items:
            num = randint(1, MAX)
        self._items.add(num)
        return '{:0>6}'.format(num)


class State:
    def __init__(self):
        self._next_message_id = 1
        self._next_user_id = 1
        self._users_pins = {}
        self._users = {}
        self._messages = {}
        self._pin_generator = PinGenerator()

    def dump(self):
        with open(DATA_FILE, 'w') as f:
            json.dump({
                'message_id': self._next_message_id,
                'user_id': self._next_user_id,
                'messages': self._messages,
                'users': self._users,
                'pins': self._users_pins
            }, f, indent=2)

    @classmethod
    def load(cls):
        instance = cls()
        if os.path.isfile(DATA_FILE):
            with open(DATA_FILE) as f:
                data = json.load(f)
                instance._next_message_id = data['message_id']
                instance._next_user_id = data['user_id']
                instance._messages = {int(k): v for k, v in data['messages'].items()}
                instance._users = {int(k): v for k, v in data['users'].items()}
                instance._users_pins = data['pins']
                pins = [int(pin) for pin in data['pins'].keys()]
                instance._pin_generator = PinGenerator(items=pins)
        return instance

    def add_user(self, endpoint: str, name: str) -> str:
        pin = self._pin_generator.get()
        user_id = self._next_user_id
        self._next_user_id += 1
        data = {'name': name, 'online': False, 'endpoint': endpoint}
        self._users[user_id] = data
        self._users_pins[pin] = user_id
        logger.info('Add user %s, %s', user_id, data)
        self.dump()
        return pin

    def get_users(self, online: bool = None):
        if online is not None:
            users = filter(lambda x: x[1]['online'] == online, self._users.items())
        else:
            users = self._users.items()
        return [{'id': k, **v} for k, v in users]

    def get_user_by_pin(self, pin: str) -> Optional[dict]:
        user_id = self._users_pins.get(pin)
        return self.get_user_by_id(user_id)

    def get_user_by_id(self, user_id: int) -> Optional[dict]:
        if user_id in self._users:
            return {'id': user_id, **self._users.get(user_id)}

    def add_message(self, user_from: dict, user_to: dict, text: str) -> dict:
        message_id = self._next_message_id
        self._next_message_id += 1
        data = {
            'from': user_from['id'],
            'to': user_to['id'],
            'text': text,
            'delivered': False,
        }
        self._messages[message_id] = data
        logger.info('Add message %s, %s', message_id, data)
        self.dump()
        return {'id': message_id, **data}

    def get_messages(self, user: dict) -> list:
        user_id = user['id']
        messages = filter(
            lambda x: user_id in [x[1]['from'], x[1]['to']],
            self._messages.items()
        )
        return list(map(lambda x: {'id': x[0], **x[1]}, messages))

    def set_user_online(self, user_id: int, online: bool):
        self._users[user_id]['online'] = online

    def set_message_delivered(self, message_id: int, delivered: bool):
        self._messages[message_id]['delivered'] = delivered


class BaseHandler(RequestHandler):
    user: Optional[dict]

    @property
    def state(self):
        return self.application.settings['state']

    def json(self):
        return json.loads(self.request.body.decode())

    def get_current_user(self) -> Optional[dict]:
        pin = self.get_query_argument('pin', None)
        return self.state.get_user_by_pin(pin)


class UsersHandler(BaseHandler):
    async def get(self):
        online = ONLINE_MAP.get(self.get_query_argument('online', None))
        await self.finish({
            'count': len(users),
            'items': [{'id': u['id'], 'online': u['online'], 'name': u['name']} for u in users]
        })

    async def post(self):
        try:
            data = self.json()
            user_pin = self.state.add_user(data['endpoint'], data['name'])
            self.set_status(201)
            await self.finish({'pin': user_pin})
        except (json.JSONDecodeError, KeyError):
            self.send_error(400)


class MessagesHandler(BaseHandler):

    @authenticated
    async def get(self):
        user = self.get_current_user()
        messages = self.state.get_messages(user)
        await self.finish({'count': len(messages), 'items': messages})


async def _send_message(message: dict, endpoint: str):
    client = AsyncHTTPClient()
    try:
        response = await client.fetch(
            ''.join([endpoint, MESSAGES_PATH]),
            method='POST', body=json.dumps(message), headers={'Content-Type': 'application/json'})
        data = json.loads(response.body.decode())
        return data['delivered']
    except Exception as e:
        logger.error('Error when send message %s, %s', message['id'], e)


class CreateMessageHandler(BaseHandler):

    @authenticated
    async def post(self, user_to_id):
        user_from = self.get_current_user()
        user_to = self.state.get_user_by_id(int(user_to_id))
        if not user_to:
            self.send_error(400)

        try:
            text = self.json()['text']
            message = self.state.add_message(user_from, user_to, text)
            delivered = await _send_message(message, user_to['endpoint'])
            self.state.set_message_delivered(message['id'], delivered)
            self.state.dump()
            self.set_status(201)
            await self.finish({'delivered': bool(delivered)})
        except (json.JSONDecodeError, KeyError):
            self.send_error(400)


async def check_endpoint(state: State, event: Event):
    client = AsyncHTTPClient()
    while not event.is_set():
        for user in state.get_users():
            user_id = user['id']
            online = False
            try:
                response = await client.fetch(''.join([user['endpoint'], STATUS_PATH]))
                data = json.loads(response.body.decode())
                online = data.get('status') == STATUS_ONLINE
            except Exception as e:
                logger.error('Error when fetch status for user %s, %s', user_id, e)

            if user['online'] != online:
                logger.info('User change status %s, to %s', user_id, online)

            state.set_user_online(user_id, online)
        await sleep(5)


class User(graphene.ObjectType):
    id = graphene.Int()
    online = graphene.Boolean()
    name = graphene.String()


class Message(graphene.ObjectType):
    id = graphene.Int()
    to = graphene.Int()
    from_ = graphene.Int(name='from')
    text = graphene.String()
    delivered = graphene.Boolean()


class Messages(graphene.ObjectType):
    count = graphene.Int()
    items = graphene.List(Message)


class CreateMessage(graphene.Mutation):
    class Arguments:
        pin = graphene.String()
        text = graphene.String()
        user_id = graphene.Int()

    delivered = graphene.Boolean()

    async def mutate(root, info, pin, text, user_id):
        state = GLOBAL_STATE[0]
        user_from = state.get_user_by_pin(pin)
        user_to = state.get_user_by_id(int(user_id))
        if not user_to:
            raise ValueError('User not exists')

        message = state.add_message(user_from, user_to, text)
        delivered = await _send_message(message, user_to['endpoint'])
        state.set_message_delivered(message['id'], delivered)
        state.dump()
        return {'delivered': bool(delivered)}


class Mutations(graphene.ObjectType):
    create_message = CreateMessage.Field()


class Query(graphene.ObjectType):
    users = graphene.List(User)
    messages = graphene.Field(Messages, pin=graphene.String(required=True))

    def resolve_users(root, info):
        return GLOBAL_STATE[0].get_users()

    def resolve_messages(root, info, pin):
        user = GLOBAL_STATE[0].get_user_by_pin(pin)
        if user:
            messages = GLOBAL_STATE[0].get_messages(user)
            return {'count': len(messages), 'items': messages}
        else:
            raise ValueError('User not exists')



my_schema = graphene.Schema(
    query=Query,
    mutation=Mutations
)

if __name__ == '__main__':
    current_state = State.load()
    GLOBAL_STATE.append(current_state)
    app = Application([
        (r'/api/v1/users', UsersHandler),
        (r'/api/v1/messages', MessagesHandler),
        (r'/api/v1/messages/user/(\d+)', CreateMessageHandler),
        (r'/graphql', TornadoGraphQLHandler, dict(graphiql=True, schema=my_schema)),
        (r'/graphql/batch', TornadoGraphQLHandler, dict(graphiql=True, schema=my_schema, batch=True)),
        (r'/graphql/graphiql', TornadoGraphQLHandler, dict(graphiql=True, schema=my_schema))
    ], debug=True, state=current_state)
    check_event = Event()
    loop = IOLoop.current()
    loop.add_callback(check_endpoint, current_state, check_event)
    try:
        app.listen(8000)
        loop.start()
    except KeyboardInterrupt:
        check_event.set()
        loop.stop()
