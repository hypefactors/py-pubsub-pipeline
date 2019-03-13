# `pubsub_pipeline`

A small utility class for connecting two
Google Cloud Pubsub queues with a data processing component .



# Install

`pip install -e git+http://github.com/hypefactors/py-pubsub-pipeline.git@v0.0.1#egg=pusub_pipeline`


# Usage


```python
from pubsub_pipeline import PubSubPipeline


def processor(data: dict) -> dict:
    data['result'] = "Processing result"
    return data
    
if __name__ == '__main__':
    PubSubPipeline(
        processor=processor,
        google_cloud_project='some-project-name', 
        incoming_subscription='some/subscription/path',
        outgoing_topic='some-topic'
    ).process()
 
```

