import logging
import pika
import threading


class BasicConsumer(threading.Thread):
    """

    A generic RabbitMQ consumer, only provide setup details for the queue and a callback method
    then the rest of the consumer is build.

    """
    CONSUMER_READY = 'CONSUMER_READY'
    CONSUMER_PROCESSED_MESSAGE = 'CONSUMER_PROCESSED_MESSAGE'
    CONSUMER_RECONNECT = 'CONSUMER_RECONNECT'
    CONSUMER_RECONNECTED = 'CONSUMER_RECONNECTED'
    CONSUMER_CANCELED = 'CONSUMER_CANCELED'
    CONSUMER_STOPPED = 'CONSUMER_STOPPED'

    EXCHANGE = 'msg'
    EXCHANGE_TYPE = 'direct'
    ROUTING_KEY = None

    def __init__(self,
                 amqp_url,
                 queue_name,
                 queue_arguments,
                 process_message_callback,
                 ):
        """

        Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        super(BasicConsumer, self).__init__()
        self._logger = logging.getLogger('basic_consumer')
        self._queue_name = queue_name
        self.ROUTING_KEY = queue_name
        self._queue_arguments = queue_arguments
        self._process_message_callback = process_message_callback

        # threading stuff
        self.stop_request = threading.Event()

        # queue stuff
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def join(self, timeout=None):
        self.stop()
        super(BasicConsumer, self).join(timeout)

    def connect(self):
        """

        This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        self._logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, _connection):
        """

        This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type _connection: pika.SelectConnection

        """
        self._logger.info('Connection opened')
        # for unexpected close of connection
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_closed(self, _connection, reply_code, reply_text):
        """

        This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection _connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning('Connection closed, reopening in 3 seconds: (%s) %s', reply_code, reply_text)
            self._logger.info(self.CONSUMER_RECONNECT)
            self._connection.add_timeout(3, self.reconnect)

    def reconnect(self):
        """

        Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its loop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._logger.info(self.CONSUMER_RECONNECTED)
            self._connection.ioloop.start()

    def open_channel(self):
        """

        Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self._logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """

        This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self._logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

        self._logger.info(self.CONSUMER_READY)

    def add_on_channel_close_callback(self):
        """

        This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self._logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """

        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self._logger.warning('Channel %i was closed: (%s) %s', channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """

        Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self._logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, _unused_frame):
        """

        Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method _unused_frame: Exchange.DeclareOk response frame

        """
        self._logger.info('Exchange declared')
        self.setup_queue(self._queue_name)

    def setup_queue(self, queue_name):
        """

        Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self._logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, arguments=self._queue_arguments, durable=True)

    def on_queue_declareok(self, _method_frame):
        """

        Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method _method_frame: The Queue.DeclareOk frame

        """
        self._logger.info('Binding %s to %s with %s', self.EXCHANGE, self._queue_name, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self._queue_name, self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, _unused_frame):
        """

        Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame

        """
        self._logger.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        """

        This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self._logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self._queue_name,
                                                         no_ack=False)

    def add_on_cancel_callback(self):
        """

        Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self._logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """

        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self._logger.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
        self._logger.info(self.CONSUMER_CANCELED)
        if self._channel:
            self._channel.close()
        self.stop()

    def on_message(self, channel, basic_deliver, properties, body):
        """

        Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver basic_deliver: basic_deliver method
        :param pika.Spec.BasicProperties properties: properties
        :param str|unicode body: The message body

        """
        self._logger.info('Received message # %s from %s: %s', basic_deliver.delivery_tag, properties.app_id, body)
        # noinspection PyBroadException
        try:
            if self._process_message_callback(channel, body):
                self.acknowledge_message(basic_deliver.delivery_tag)
            else:
                self.reject_message(basic_deliver.delivery_tag)
        except Exception:
            self.reject_message(basic_deliver.delivery_tag, True)

        self._logger.info(self.CONSUMER_PROCESSED_MESSAGE)

    def acknowledge_message(self, delivery_tag):
        """

        Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self._logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag, requeue=False):
        """

        Reject the message delivery from RabbitMQ by sending a
        Basic.Nack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        :param bool requeue: Where to requeue the message or not

        """
        self._logger.info('Reject message %s', delivery_tag)
        self._channel.basic_nack(delivery_tag, requeue=requeue)

    def stop_consuming(self):
        """

        Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._connection.is_open and self._channel.is_open:
            self._logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, _frame):
        """

        This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _frame: The Basic.CancelOk frame

        """
        self._logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """

        Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self._logger.info('Closing the channel')
        self._channel.close()

    def stop(self):
        """

        Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._logger.info(self.CONSUMER_STOPPED)
