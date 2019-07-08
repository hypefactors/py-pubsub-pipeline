import json
import logging
import signal
import sys
import time
from typing import TypeVar, Generic, Callable, List

from google.api_core.exceptions import DeadlineExceeded, RetryError
from google.cloud.pubsub_v1 import SubscriberClient, PublisherClient

A = TypeVar('A')
B = TypeVar('B')


class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.signum = None

    def exit_gracefully(self, signum, frame):
        self.signum = signum
        self.kill_now = True


def byte_encode_json(result) -> bytes:
    return json.dumps(result).encode()


class Acknowledger:
    def __init__(self, subscriber, subscription_path, message):
        self.subscriber = subscriber
        self.subscription_path = subscription_path
        self.message = message

    def __call__(self, future):
        try:
            _ = future.result()
            logging.info(
                'Result was successfully published, '
                'acknowledging message %s' % self.message.ack_id
            )
            self.subscriber.acknowledge(
                self.subscription_path,
                [self.message.ack_id]
            )
        except:
            logging.exception(
                'Could not publish result. '
                'Will not acknowledge message %s' % self.message.ack_id
            )


def byte_load_json(data: bytes):
    data = data.decode('utf-8')
    return json.loads(data)


class PubSubPipeline(Generic[A, B]):
    def __init__(self,
                 processor: Callable[[A], B],
                 google_cloud_project: str,
                 incoming_subscription: str,
                 outgoing_topic: str,
                 message_deserializer: Callable[[bytes], A] = byte_load_json,
                 result_serializer: Callable[[B], bytes] = byte_encode_json,
                 bulk_limit=20,
                 subscriber: SubscriberClient = None,
                 publisher: PublisherClient = None,
                 respect_deadline=False,
                 deadline_exceeded_retry_wait_secs=300
                 ):
        """
        Generic google cloud PubSub pipeline. Will continuously
            - Collect messages from `incoming_subscription`
            - Deserialize the data with  `message_deserializer`
            - Process the data with `processor`,
            - Serialize the result with `result_serializer`
            - publish the result to `outgoing_topic`.

        The topics and subscriptions must exist before using this class. Messages
        from `incoming_subscription` are acknowledged only when the processed
        result is successfully published to `outgoing_topic`.

        Also handles receiving sigterms from google cloud when running
        on a pre-emptible instance. In that case will simply call `sys.exit(0)`
        at first available moment.

        :param processor: Callable that can process the data in each
                          PubSub message
        :param google_cloud_project: Name of the google cloud project that holds
                                     the PubSubs
        :param incoming_subscription: Name of the subscription to collect
                                      messages from
        :param outgoing_topic: Name of the topic to publish results to
        :param message_deserializer: Callable that can deserialize the PubSub
                                     message data
        :param result_serializer: Callable that can serialize the result data
        :param bulk_limit: Limit on bulk size of incoming PubSub messages
        :param subscriber: The google cloud PubSub :class:`SubscriberClient`
                           instance. Defaults to `SubscriberClient()`.
        :param publisher: The google cloud PubSub :class:`PublisherClient`
                          instance. Defaults to `PublisherClient()`
        :param respect_deadline: Whether to re-raise DeadlineExceeded errors
                                 while pulling messages

        """
        self.deadline_exceeded_retry_wait_secs = deadline_exceeded_retry_wait_secs
        self.respect_deadline = respect_deadline
        self.processor = processor
        self.google_cloud_project = google_cloud_project
        self.incoming_subscription = incoming_subscription
        self.outgoing_topic = outgoing_topic
        self.message_deserializer = message_deserializer
        self.result_serializer = result_serializer
        self.bulk_limit = bulk_limit
        self.publisher = (publisher if publisher is not None
                          else PublisherClient())
        self.subscriber = (subscriber if subscriber is not None
                           else SubscriberClient())

        self.subscription_path = self.subscriber.subscription_path(
            self.google_cloud_project,
            self.incoming_subscription
        )
        self.topic_path = self.publisher.topic_path(
            self.google_cloud_project,
            self.outgoing_topic
        )

    def process(self, max_processed_messages=None):
        """
        Begin collecting messages from PubSub subscription
        and processing them. Will never return unless `max_processed_messages`
        is given, in which case returns after `max_processed_messages` has
        been processed

        :param max_processed_messages: Max number of messages to process before
                                       returning.
        """
        
        logging.info('Using incoming subscription %s' % self.subscription_path)

        logging.info('Using outgoing topic %s ' % self.topic_path)

        killer = GracefulKiller()
        total_messages_processed = 0
        while True:
            if killer.kill_now:
                logging.info(
                    'Received signal %d, exiting gracefully...' % killer.signum
                )
                sys.exit(0)
            try:
                logging.info('Waiting for messages...')
                response = self.wait_for_messages()
                logging.info('Received messages')
                self._process_response(response)
                total_messages_processed += len(response.received_messages)
                if (
                        max_processed_messages is not None
                        and total_messages_processed == max_processed_messages
                ):
                    logging.info('Reached max number of processed messages')
                    return

            except KeyboardInterrupt:
                logging.info('Received keyboard interrupt. Exiting...')
                sys.exit(0)

    def _process_response(self, response):
        for message in response.received_messages:
            self._process_message(message)

    def _process_message(self, message):
        message_data = self.message_deserializer(message.message.data)
        logging.info(
            'Deserialized data from %s' % message.ack_id
        )
        result = self.processor(message_data)
        logging.info('Processed result of %s' % message.ack_id)
        serialized_result = self.result_serializer(result)
        logging.info('Serialized result of processing %s' % message.ack_id)
        acknowledger = Acknowledger(
            message=message,
            subscriber=self.subscriber,
            subscription_path=self.subscription_path
        )
        future = self.publisher.publish(
            self.topic_path, data=serialized_result
        )
        future.add_done_callback(acknowledger)

    def wait_for_messages(self):
        try:
            response = self.subscriber.pull(
                self.subscription_path,
                max_messages=self.bulk_limit
            )
            if not response.received_messages:
                return self.wait_for_messages()
            return response
        except (DeadlineExceeded, RetryError) as e:
            if isinstance(e, RetryError) and not str(e).startswith('Deadline'):
                raise e
            if self.respect_deadline:
                raise e
            time.sleep(self.deadline_exceeded_retry_wait_secs)
            logging.info('No messages received before deadline, retrying...')
            return self.wait_for_messages()


class BulkPubSubPipeline(PubSubPipeline):
    def __init__(self,
                 processor: Callable[[List[A]], B],
                 google_cloud_project: str,
                 incoming_subscription: str,
                 outgoing_topic: str,
                 *args,
                 **kwargs):
        super().__init__(processor, google_cloud_project, incoming_subscription,
                         outgoing_topic, *args, **kwargs)

    def _process_response(self, response):
        messages = response.received_messages
        message_data = [self.message_deserializer(message.message.data)
                        for message in messages]
        results = self.processor(message_data)
        serialized_results = [self.result_serializer(result)
                              for result in results]
        for result, message in zip(serialized_results, messages):
            future = self.publisher.publish(
                self.topic_path, data=result
            )
            future.add_done_callback(
                Acknowledger(
                    message=message,
                    subscription_path=self.subscription_path,
                    subscriber=self.subscriber
                )
            )
