# -*- test-case-name: vumi.tests.test_worker -*-

import json

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import protocol

import txamqp
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient

from vumi import log
from vumi.message import Message
from vumi.utils import vumi_resource_path


SPECS = {}


def get_spec(specfile):
    """
    Cache the generated part of txamqp, because generating it is expensive.

    This is important for tests, which create lots of txamqp clients,
    and therefore generate lots of specs. Just doing this results in a
    decidedly happy test run time reduction.
    """
    if specfile not in SPECS:
        SPECS[specfile] = txamqp.spec.load(specfile)
    return SPECS[specfile]


class WorkerAmqpFactory(protocol.ReconnectingClientFactory):
    def __init__(self, worker_amqp_client, options):
        self.worker_amqp_client = worker_amqp_client
        self.amqp_vhost = options['vhost']
        self.amqp_spec = get_spec(vumi_resource_path(options['specfile']))
        self.amqp_username = options['username']
        self.amqp_password = options['password']
        self.amqp_heartbeat = options.get('heartbeat', 0)

    def buildProtocol(self, addr):
        protocol = AMQClient(TwistedDelegate(), self.amqp_vhost,
                             self.amqp_spec, self.amqp_heartbeat)
        protocol.factory = self
        self.resetDelay()
        d = protocol.authenticate(self.amqp_username, self.amqp_password)
        d.addCallback(
            lambda r: self.worker_amqp_client.amqp_connection_made(protocol))
        return protocol

    def clientConnectionFailed(self, connector, reason):
        log.err("Connection failed: %r" % (reason,))
        protocol.ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        self.worker_amqp_client.amqp_connection_lost()
        protocol.ReconnectingClientFactory.clientConnectionLost(
            self, connector, reason)


class WorkerAmqpClient(object):
    def __init__(self, worker):
        self.worker = worker
        self._closed = False
        self.protocol = None

    @property
    def is_connected(self):
        return self.protocol is not None

    def amqp_connection_made(self, protocol):
        log.msg("Got an authenticated connection")
        self.protocol = protocol
        self.worker.amqp_connection_made()

    def amqp_connection_lost(self, reason):
        if not self._closed:
            # This disconnect wasn't explicitly requested.
            log.err("Client connection lost: %r" % (reason,))

    def close(self):
        self.stopTrying()
        self._closed = True
        if self.protocol is not None:
            return self.protocol.close()

    @inlineCallbacks
    def get_channel(self, channel_id=None):
        """If channel_id is None a new channel is created"""
        if channel_id:
            channel = self.protocol.channels[channel_id]
        else:
            channel_id = self.protocol.get_new_channel_id()
            channel = yield self.protocol.channel(channel_id)
            yield channel.channel_open()
            self.protocol.channels[channel_id] = channel
        returnValue(channel)

    def get_new_channel_id(self):
        # AMQClient keeps track of channels in a dictionary. The channel ids
        # are the keys, so we get the highest number we already have and
        # increment it for the new id or return 0 if we don't yet have any
        # channels yet.
        if not self.protocol.channels:
            return 0
        return max(self.protocol.channels) + 1

    def _declare_exchange(self, source, channel):
        # get the details for AMQP
        exchange_name = source.exchange_name
        exchange_type = source.exchange_type
        durable = source.durable
        return channel.exchange_declare(exchange=exchange_name,
                                        type=exchange_type, durable=durable)

    @inlineCallbacks
    def start_publisher(self, publisher):
        channel = yield self.get_channel()
        yield self._declare_exchange(publisher, channel)
        yield publisher.start(channel)
        returnValue(publisher)

    @inlineCallbacks
    def start_consumer(self, consumer):
        channel = yield self.get_channel()
        yield self._declare_exchange(consumer, channel)

        # Set up the queue.
        exchange_name = consumer.exchange_name
        durable = consumer.durable
        queue_name = consumer.queue_name
        routing_key = consumer.routing_key
        yield channel.queue_declare(queue=queue_name, durable=durable)
        yield channel.queue_bind(queue=queue_name, exchange=exchange_name,
                                 routing_key=routing_key)

        # register the consumer
        reply = yield channel.basic_consume(queue=queue_name)
        queue = yield self.queue(reply.consumer_tag)
        # start consuming! nom nom nom
        consumer.start(channel, queue)
        returnValue(consumer)


class RoutingKeyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Publisher(object):
    def __init__(self, routing_key, exchange="vumi", exchange_type="direct",
                 durable=True, auto_delete=False, delivery_mode=2):
        self.routing_key = routing_key
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.durable = durable
        self.auto_delete = auto_delete
        self.delivery_mode = delivery_mode

    def start(self, channel):
        log.msg("Started the publisher")
        self.channel = channel

    @inlineCallbacks
    def check_routing_key(self, routing_key):
        if(routing_key != routing_key.lower()):
            raise RoutingKeyError("The routing_key: %s is not all lower case!"
                                  % (routing_key))

    def publish_message(self, message, routing_key=None):
        d = self.publish_raw(message.to_json(), routing_key=routing_key)
        d.addCallback(lambda r: message)
        return d

    def publish_json(self, data, routing_key=None):
        json_data = json.dumps(data, cls=json.JSONEncoder)
        return self.publish_raw(json_data, routing_key=routing_key)

    def publish_raw(self, data, routing_key=None):
        if routing_key is None:
            routing_key = self.routing_key
        self.check_routing_key(routing_key)

        content = Content(data)
        content['delivery mode'] = self.delivery_mode
        return self.channel.basic_publish(
            exchange=self.exchange, content=content, routing_key=routing_key)


class QueueCloseMarker(object):
    "This is a marker for closing consumer queues."


class Consumer(object):
    def __init__(self, routing_key, handler_func, queue_name=None,
                 exchange="vumi", exchange_type="direct", durable=True,
                 message_class=Message):
        self.routing_key = routing_key
        self.handler_func = handler_func
        if queue_name is None:
            queue_name = routing_key
        self.queue_name = queue_name
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.durable = durable
        self.message_class = message_class
        self.paused = None

    def start(self, channel, queue):
        self.channel = channel
        self.queue = queue
        self.keep_consuming = True
        self._testing = hasattr(channel, 'message_processed')
        self.pause()

        @inlineCallbacks
        def read_messages():
            log.msg("Consumer starting...")
            try:
                while self.keep_consuming:
                    message = yield self.queue.get()
                    if isinstance(message, QueueCloseMarker):
                        log.msg("Queue closed.")
                        return
                    yield self.consume(message)
            except txamqp.queue.Closed, e:
                log.err("Queue has closed", e)

        read_messages()
        return self

    def pause(self):
        self.paused = True
        return self.channel.channel_flow(active=False)

    def unpause(self):
        self.paused = False
        return self.channel.channel_flow(active=True)

    @inlineCallbacks
    def consume(self, message):
        result = yield self.consume_message(self.message_class.from_json(
                                            message.content.body))
        if self._testing:
            self.channel.message_processed()
        if result is not False:
            returnValue(self.ack(message))
        else:
            log.msg('Received %s as a return value consume_message. '
                    'Not acknowledging AMQ message' % result)

    def consume_message(self, message):
        return self.handler_func(message)

    def ack(self, message):
        self.channel.basic_ack(message.delivery_tag, True)

    @inlineCallbacks
    def stop(self):
        log.msg("Consumer stopping...")
        self.keep_consuming = False
        # This actually closes the channel on the server
        yield self.channel.channel_close()
        # This just marks the channel as closed on the client
        self.channel.close(None)
        self.queue.put(QueueCloseMarker())
        returnValue(self.keep_consuming)
