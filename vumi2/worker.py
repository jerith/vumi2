# -*- test-case-name: vumi.tests.test_worker -*-

import json
from copy import deepcopy

from zope.interface import implements

from twisted.application.service import MultiService
from twisted.application.internet import TCPClient
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import protocol, reactor
from twisted.web.resource import Resource

from vumi import log
from vumi.message import Message
from vumi.utils import (load_class_by_string, vumi_resource_path, http_request,
                        basic_auth_string, LogFilterSite)

from vumi.connectors import ReceiveInboundConnector, ReceiveOutboundConnector
from vumi.errors import DuplicateConnectorError

from vumi2.vumi_amqp import WorkerAmqpClient, WorkerAmqpFactory
from vumi2.interface import IVumiService, IVumiWorker


class VumiWorkerService(MultiService):
    """Base implementation of IVumiService.
    """
    implements(IVumiService)

    def __init__(self, worker_class, config):
        MultiService.__init__(self)
        self.worker = worker_class(config)
        self.config = config
        self.amqp_client = WorkerAmqpClient(self.worker)
        self.connectors = {}

    # Interface methods and attrs.

    def get_amqp_client_factory(self, amqp_options):
        return WorkerAmqpFactory(self.amqp_client, amqp_options)

    @property
    def connectors_paused(self):
        raise NotImplementedError()

    def get_connector(connector_name):
        raise NotImplementedError()

    def pause_connectors(self):
        for connector in self.connectors.itervalues():
            connector.pause()

    def unpause_connectors(self):
        for connector in self.connectors.itervalues():
            connector.unpause()

    # Things that Worker instances can call, but probably shouldn't.

    def setup_connector(self, connector_name, connector_cls, middlewares=None):
        if connector_name in self.connectors:
            raise DuplicateConnectorError("Attempt to add duplicate connector"
                                          " with name %r" % (connector_name,))
        prefetch_count = self.get_static_config().amqp_prefetch_count

        connector = connector_cls(self, connector_name,
                                  prefetch_count=prefetch_count,
                                  middlewares=middlewares)
        self.connectors[connector_name] = connector

        d = connector.setup()
        d.addCallback(lambda r: connector)
        return d

    def teardown_connector(self, connector_name):
        connector = self.connectors.pop(connector_name)
        d = connector.teardown()
        d.addCallback(lambda r: connector)
        return d

    def setup_ri_connector(self, connector_name, middlewares=None):
        return self.setup_connector(ReceiveInboundConnector, connector_name,
                                    middlewares=middlewares)

    def setup_ro_connector(self, connector_name, middlewares=None):
        return self.setup_connector(ReceiveOutboundConnector, connector_name,
                                    middlewares=middlewares)


class VumiWorker(object):
    """Base implementation of IVumiWorker.
    """
    implements(IVumiWorker)

    # Interface methods and attrs.

    def amqp_connection_made():
        pass

    def amqp_connection_lost():
        pass

    def pre_setup():
        raise NotImplementedError()


# class WorkerService(MultiService):
#     """
#     The Worker is responsible for starting consumers & publishers
#     as needed.
#     """

#     def startWorker(self):
#         # I hate camelCasing method but since Twisted has it as a
#         # standard I voting to stick with it
#         raise VumiError("You need to subclass Worker and its "
#                         "startWorker method")

#     def stopWorker(self):
#         pass

#     @inlineCallbacks
#     def stopService(self):
#         if self.running:
#             yield self.stopWorker()
#         yield super(Worker, self).stopService()

#     def routing_key_to_class_name(self, routing_key):
#         return ''.join(map(lambda s: s.capitalize(), routing_key.split('.')))

#     def consume(self, routing_key, callback, queue_name=None,
#                 exchange_name='vumi', exchange_type='direct', durable=True,
#                 message_class=None, paused=False):

#         # use the routing key to generate the name for the class
#         # amq.routing.key -> AmqRoutingKey
#         dynamic_name = self.routing_key_to_class_name(routing_key)
#         class_name = "%sDynamicConsumer" % str(dynamic_name)
#         kwargs = {
#             'routing_key': routing_key,
#             'queue_name': queue_name or routing_key,
#             'exchange_name': exchange_name,
#             'exchange_type': exchange_type,
#             'durable': durable,
#             'start_paused': paused,
#         }
#         log.msg('Starting %s with %s' % (class_name, kwargs))
#         klass = type(class_name, (DynamicConsumer,), kwargs)
#         if message_class is not None:
#             klass.message_class = message_class
#         return self.start_consumer(klass, callback)

#     def start_consumer(self, consumer_class, *args, **kw):
#         return self._amqp_client.start_consumer(consumer_class, *args, **kw)

#     def publish_to(self, routing_key,
#                    exchange_name='vumi', exchange_type='direct', durable=True,
#                    delivery_mode=2):
#         class_name = self.routing_key_to_class_name(routing_key)
#         publisher_class = type("%sDynamicPublisher" % class_name, (Publisher,),
#             {
#                 "routing_key": routing_key,
#                 "exchange_name": exchange_name,
#                 "exchange_type": exchange_type,
#                 "durable": durable,
#                 "delivery_mode": delivery_mode,
#             })
#         return self.start_publisher(publisher_class)

#     def start_publisher(self, publisher_class, *args, **kw):
#         return self._amqp_client.start_publisher(publisher_class, *args, **kw)

#     def start_web_resources(self, resources, port, site_class=None):
#         # start the HTTP server for receiving the receipts
#         root = Resource()
#         # sort by ascending path length to make sure we create
#         # resources lower down in the path earlier
#         resources = sorted(resources, key=lambda r: len(r[1]))
#         for resource, path in resources:
#             request_path = filter(None, path.split('/'))
#             nodes, leaf = request_path[0:-1], request_path[-1]

#             def create_node(node, path):
#                 if path in node.children:
#                     return node.children.get(path)
#                 else:
#                     new_node = Resource()
#                     node.putChild(path, new_node)
#                     return new_node

#             parent = reduce(create_node, nodes, root)
#             parent.putChild(leaf, resource)

#         if site_class is None:
#             site_class = LogFilterSite
#         site_factory = site_class(root)
#         return reactor.listenTCP(port, site_factory)


# class WorkerCreator(object):
#     """
#     Creates workers
#     """

#     def __init__(self, vumi_options):
#         self.options = vumi_options

#     def create_worker(self, worker_class, config, timeout=30,
#                       bindAddress=None):
#         """
#         Create a worker factory, connect to AMQP and return the factory.

#         Return value is the AmqpFactory instance containing the worker.
#         """
#         return self.create_worker_by_class(
#             load_class_by_string(worker_class), config, timeout=timeout,
#             bindAddress=bindAddress)

#     def create_worker_by_class(self, worker_class, config, timeout=30,
#                                bindAddress=None):
#         worker = worker_class(deepcopy(self.options), config)
#         self._connect(worker, timeout=timeout, bindAddress=bindAddress)
#         return worker

#     def _connect(self, worker, timeout, bindAddress):
#         service = TCPClient(self.options['hostname'], self.options['port'],
#                             AmqpFactory(worker), timeout, bindAddress)
#         service.setServiceParent(worker)
