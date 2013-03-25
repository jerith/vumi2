from zope.interface import Interface, Attribute


class IVumiService(Interface):
    """Vumi service interface.

    This handles the AMQP connections and setup/teardown infrastructure.
    """

    # Framework infrastructure stuff.

    def get_amqp_client_factory(amqp_options):
        """Returns a protocol factory for an AMQP client.
        """

    # Things for Worker instances to call.

    connectors_paused = Attribute("Paused status of connectors. (bool)")

    def get_connector(connector_name):
        """Returns the connector with the given name.
        """

    def pause_connectors():
        """Pause all connectors.
        """

    def unpause_connectors():
        """Unpause all connectors.
        """

    # Things that Worker instances can call, but probably shouldn't.

    def setup_connector(connector_name, connector_cls, middlewares=None):
        """Set up a connector.

        You should really use `setup_ri_connector` or `setup_ro_connector`
        instead of this.
        """

    def teardown_connector(connector_name):
        """Tear down a connector.

        This shouls be called for every connector that gets set up.
        """

    def setup_ri_connector(connector_name, middlewares=True):
        """Set up a `ReceiveInboundConnector`.
        """

    def setup_ro_connector(connector_name, middlewares=True):
        """Set up a `ReceiveOutboundConnector`.
        """


class IVumiWorker(Interface):
    """Vumi worker interface.

    This specifies the interface for a vumi worker.
    """

    def amqp_connection_made():
        """Called when we have an active AMQP connection.
        """

    def amqp_connection_lost():
        """Called when our AMQP connection is lost.
        """

    def initial_setup():
        """Called when the worker service starts.

        We don't necessarily have an AMQP connection at this point.
        """


class IVumiConfigManager(Interface):
    """Manages configuration for a vumi worker.
    """

    def get_static_config():
        """Get a configuration object containing only static fields.

        This returns a config object which only contains static fields that are
        available at worker startup.
        """

    def get_message_config(message):
        """Get a message-specific configuration object.

        This returns a Deferred which fires with a config object containing all
        fields, potentially populated with message-specific data.
        """
