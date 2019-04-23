import json

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient


class TestClient:
    def __init__(self,
                 google_cloud_project,
                 pipeline_incoming_topic,
                 outgoing_subscription,
                 callback=None):
        self.publisher = PublisherClient()
        self.subscriber = SubscriberClient()
        self.subscription = self.subscriber.subscription_path(
            google_cloud_project,
            outgoing_subscription
        )
        self.topic = self.publisher.topic_path(
            google_cloud_project,
            pipeline_incoming_topic
        )
        if callback is None:
            def callback(message):
                print(message.data)
                message.ack()

        self.subscriber.subscribe(self.subscription, callback=callback)

    def publish(self, data):
        data = json.dumps(data).encode()
        return self.publisher.publish(self.topic, data)
