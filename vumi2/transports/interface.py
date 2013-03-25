from vumi2.interface import IVumiService, IVumiWorker


class IVumiTransportService(IVumiService):
    """Vumi transport service interface.

    This adds publish methods for inbound messages and events.
    """

    def publish_inbound_message(message, endpoint=None):
        """Publish an inbound message.
        """

    def publish_event(event, endpoint=None):
        """Publish an event.
        """


class IVumiTransport(IVumiWorker):
    """Vumi transport interface.
    """

    def consume_outbound_message(message):
        """Called when an outbound message is received.

        This should send the message out over the transport protocol.
        """
