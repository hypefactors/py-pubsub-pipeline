import json
from unittest.mock import MagicMock

import pytest

from pubsub_pipeline import PubSubPipeline, BulkPubSubPipeline


def _mock_future(on_add_done_callback=None):
    mock_future_ = MagicMock()
    # python mock magic: when side_effect is a callable,
    # it will be called with the same arguments as the
    # mock function
    if on_add_done_callback is not None:
        mock_future_.add_done_callback.side_effect = on_add_done_callback
    return mock_future_


def _mock_publisher(on_publish=None):
    publisher = MagicMock()
    publisher.topic_path.return_value = 'some/topic/path'
    if on_publish is not None:
        publisher.publish.side_effect = on_publish
    return publisher


def _message_data():
    return {
        "data": "This is some json data that is to processed",
        "nested": {
            "nestedData": "This is just some more data"
        }
    }


def processor(_):
    return _


def _mock_message():
    mock_message = MagicMock()
    mock_message.message.data = json.dumps(_message_data()).encode()
    mock_message.ack_id = 'some_ack_id'
    return mock_message


def _mock_subscriber(received_messages=(_mock_message(),)):
    subscriber = MagicMock()
    subscriber.subscription_path.return_value = 'some/subscription/path'
    subscriber.pull.return_value.received_messages = list(received_messages)
    return subscriber


@pytest.mark.parametrize('pipeline', [PubSubPipeline, BulkPubSubPipeline])
def test_message_is_acknowledged_on_successful_publish(pipeline):
    def on_publish(topic_path, data):
        assert topic_path == 'some/topic/path'
        assert isinstance(data, bytes)
        result = json.loads(data)
        assert result == _message_data()
        return mock_future

    subscriber = _mock_subscriber()
    publisher = _mock_publisher(on_publish)

    def on_add_done_callback(callback):
        callback(mock_future)
        subscriber.acknowledge.assert_called_with(
            'some/subscription/path',
            ['some_ack_id']
        )

    mock_future = _mock_future(on_add_done_callback)

    pipeline(
        google_cloud_project='',
        incoming_subscription='',
        outgoing_topic='',
        processor=processor,
        subscriber=subscriber,
        publisher=publisher
    ).process(max_processed_messages=1)


@pytest.mark.parametrize('pipeline', [PubSubPipeline, BulkPubSubPipeline])
def test_message_is_not_acknowledged_on_failure(pipeline):
    def on_add_done_callback(callback):
        callback(mock_future)
        subscriber.acknowledge.assert_not_called()

    mock_future = _mock_future(on_add_done_callback)
    mock_future.result.side_effect = Exception('Boom!')
    subscriber = _mock_subscriber()
    publisher = _mock_publisher()

    pipeline(
        google_cloud_project='',
        incoming_subscription='',
        outgoing_topic='',
        processor=processor,
        subscriber=subscriber,
        publisher=publisher
    ).process(max_processed_messages=1)
