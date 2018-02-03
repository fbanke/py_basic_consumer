import unittest.mock
from unittest import TestCase
from unittest.mock import MagicMock
import pika
import logging
from datadriven_basic_consumer.basic_consumer import BasicConsumer


class TestBasicConsumer(TestCase):

    def create_queue(self, queue_name, passive=False):
        return self.channel.queue_declare(queue=queue_name,
                                          durable=True,
                                          arguments=self.queue_arguments,
                                          passive=passive
                                          )

    def wait_for_log_contains(self, text, level='info'):
        timeout = 1
        max_tries = 5
        while max_tries is not 0:
            if any(text in s for s in self.consumer_log_messages[level]):
                return True

            import time
            time.sleep(timeout)
            max_tries -= 1

        self.fail("Log message was not found %s" % text)

    @classmethod
    def setUpClass(cls):
        super(TestBasicConsumer, cls).setUpClass()

        # pass all log messages through to the custom handler
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        # create a logger that will log all messages
        consumer_log = logging.getLogger('basic_consumer')
        cls._consumer_log_handler = MockLoggingHandler(level=logging.DEBUG)
        consumer_log.addHandler(cls._consumer_log_handler)
        cls.consumer_log_messages = cls._consumer_log_handler.messages

    def setUp(self):
        super(TestBasicConsumer, self).setUp()

        self.rabbitmq_url = "amqp://localhost"
        self.queue_name = 'queue_name'
        self.queue_name_dead_letters = 'dl'
        self.exchange_name_dead_letters = 'dlx'
        self.queue_arguments = {
                                    "x-dead-letter-exchange": self.exchange_name_dead_letters,
                                    "x-dead-letter-routing-key": self.queue_name_dead_letters,
                               }

        # create a connection to rabbitMQ and clean the queue
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

        self.channel = self.connection.channel()
        self.channel.confirm_delivery()  # block until delivery has been confirmed on publish

        # reset queues
        self.channel.queue_delete(queue=self.queue_name)
        self.channel.queue_delete(queue=self.queue_name_dead_letters)

        # assert that the queue is empty
        res = self.create_queue(self.queue_name)
        self.assertEqual(res.method.message_count, 0)

        # create an exchange to handle dead letters and connect it to the queue
        self.channel.exchange_declare(exchange=self.exchange_name_dead_letters, exchange_type='direct')
        self.channel.queue_declare(queue=self.queue_name_dead_letters)
        self.channel.queue_bind(exchange=self.exchange_name_dead_letters,
                                routing_key=self.queue_name_dead_letters,
                                queue=self.queue_name_dead_letters)

        # reset the log
        self._consumer_log_handler.reset()

    def test_that_consume_on_valid_message_are_consumed_from_the_queue(self):
        # publish a message
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body="dummymessage"
        )

        # see that the message is in the queue
        res = self.create_queue(self.queue_name, True)
        self.assertEqual(res.method.message_count, 1)

        # accept any message
        message_callback = MagicMock(return_value=True)

        consumer = BasicConsumer(self.rabbitmq_url,
                                 self.queue_name,
                                 self.queue_arguments,
                                 message_callback
                                 )
        # start consumer
        consumer.start()

        # wait for the consumer to be ready
        self.wait_for_log_contains(BasicConsumer.CONSUMER_READY)

        # wait for the consumer to signal that is has consumed a message
        self.wait_for_log_contains(BasicConsumer.CONSUMER_PROCESSED_MESSAGE)

        # assert that the queue is empty
        res = self.create_queue(self.queue_name, True)
        self.assertEqual(res.method.message_count, 0)

        message_callback.assert_called_once()

        # stop the consumer
        consumer.join()

    def test_that_consume_on_invalid_message_does_moves_message_to_dead_letter_queue(self):
        # publish a message
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body="dummymessage"
        )

        res = self.create_queue(self.queue_name, True)
        self.assertEqual(res.method.message_count, 1)

        message_callback = MagicMock(return_value=False)

        consumer = BasicConsumer(self.rabbitmq_url,
                                 self.queue_name,
                                 self.queue_arguments,
                                 message_callback
                                 )
        # start consumer
        consumer.start()

        # wait for the consumer to be ready
        self.wait_for_log_contains(BasicConsumer.CONSUMER_READY)

        # wait for the consumer to signal that is has consumed a message
        self.wait_for_log_contains(BasicConsumer.CONSUMER_PROCESSED_MESSAGE)

        # assert that the queue is empty
        res = self.create_queue(self.queue_name, True)
        self.assertEqual(res.method.message_count, 0)

        # assert that the message is not in the dead-letters-queue
        res = self.channel.queue_declare(queue=self.queue_name_dead_letters, passive=True)
        self.assertEqual(res.method.message_count, 1)

        message_callback.assert_called_once()

        # stop the consumer
        consumer.join()

    def test_that_consume_on_failure_message_does_reschedule_the_message(self):
        # publish a message
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body="dummymessage"
        )

        res = self.create_queue(self.queue_name, True)
        self.assertEqual(res.method.message_count, 1)

        message_callback = MagicMock(side_effect=Exception("Failed to process message"))

        consumer = BasicConsumer(self.rabbitmq_url,
                                 self.queue_name,
                                 self.queue_arguments,
                                 message_callback
                                 )
        # start consumer
        consumer.start()

        # wait for the consumer to be ready
        self.wait_for_log_contains(BasicConsumer.CONSUMER_READY)

        # wait for the consumer to signal that is has consumed a message
        self.wait_for_log_contains(BasicConsumer.CONSUMER_PROCESSED_MESSAGE)

        # stop the consumer
        consumer.join()

        # assert that the message is still in the queue
        res = self.channel.queue_declare(queue=self.queue_name, passive=True)
        self.assertEqual(res.method.message_count, 1)

        # since the message is rescheduled it might be processed many times before we stop the consumer
        message_callback.assert_called()

    def test_that_close_connection_triggers_reconnect(self):
        message_callback = MagicMock(return_value=True)

        consumer = BasicConsumer(self.rabbitmq_url,
                                 self.queue_name,
                                 self.queue_arguments,
                                 message_callback
                                 )
        # start consumer
        consumer.start()

        # wait for the consumer to be ready
        self.wait_for_log_contains(BasicConsumer.CONSUMER_READY)

        # force close
        consumer._connection.close()

        self.wait_for_log_contains(BasicConsumer.CONSUMER_RECONNECT)
        self.wait_for_log_contains(BasicConsumer.CONSUMER_RECONNECTED)

        # stop the consumer
        consumer.join()

    def test_that_close_channel_triggers_stop(self):
        message_callback = MagicMock(return_value=True)

        consumer = BasicConsumer(self.rabbitmq_url,
                                 self.queue_name,
                                 self.queue_arguments,
                                 message_callback
                                 )
        # start consumer
        consumer.start()

        # wait for the consumer to be ready
        self.wait_for_log_contains(BasicConsumer.CONSUMER_READY)

        # ask for shutdown
        # if we delete the queue the server will send a Basic.Cancel to the consumer
        self.channel.queue_delete(queue=self.queue_name)

        self.wait_for_log_contains(BasicConsumer.CONSUMER_CANCELED)
        self.wait_for_log_contains(BasicConsumer.CONSUMER_STOPPED)

        # stop the consumer
        consumer.join()


class MockLoggingHandler(logging.Handler):
    """
    Mock logging handler to check for expected logs.

    Messages are available from an instance's ``messages`` dict, in order, indexed by
    a lowercase log level string (e.g., 'debug', 'info', etc.).

    """

    def __init__(self, *args, **kwargs):
        self.messages = {'debug': [], 'info': [], 'warning': [], 'error': [],
                         'critical': []}
        super(MockLoggingHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        # Store a message from ``record`` in the instance's ``messages`` dict.
        # noinspection PyBroadException
        try:
            self.messages[record.levelname.lower()].append(record.getMessage())
        except Exception:
            self.handleError(record)

    def reset(self):
        self.acquire()
        try:
            for message_list in self.messages.values():
                message_list.clear()
        finally:
            self.release()


if __name__ == '__main__':
    unittest.main()
