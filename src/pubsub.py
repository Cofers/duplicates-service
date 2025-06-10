import json
import logging
from typing import Any, Dict
from google.cloud import pubsub_v1
import os
import time
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configure publisher options for batching.
publisher_options = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024 * 10,  # 10 MB max batch size.
    max_latency=0.02,        # 0.02 seconds max latency before publishing.
    max_messages=1000,       # Up to 1000 messages per batch.
)
publisher = pubsub_v1.PublisherClient(batch_settings=publisher_options)


def publish_response(data: dict, topic_name: str) -> None:
    """
    Publish a message to a Pub/Sub topic.
    
    Args:
        data (dict): The data to publish
        topic_name (str): The name of the topic to publish to
    """
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("production-400914", topic_name)
        
        # Convert data to JSON string
        attributes = {"Content-Type": "application/json"}
        message_data = json.dumps(data).encode("utf-8")
        
        # Publish the message
        future = publisher.publish(topic_path, message_data, **attributes)
        future.result()  # Wait for the publish to complete
        
    except GoogleAPIError as e:
        print(f"Error publishing message to Pub/Sub: {str(e)}")
        raise

def publish_response_old(message: Dict[str, Any], topic: str) -> None:
    """
    Publishes a processed message to a Pub/Sub topic.

    Args:
        message (Dict[str, Any]): The message payload to publish.
        topic (str): The Pub/Sub topic name.
    """
    try:
        message_bytes = json.dumps(message).encode("utf-8")
    except Exception as e:
        logger.error(f"Error encoding message to JSON: {e}")
        return

    # You may replace the hard-coded project ID with an environment variable if needed.
    project_id = os.environ.get("GCP_PROJECT", "production-400914")
    topic_path = publisher.topic_path(project_id, topic)

    try:
        attributes = {"content-type": "application/json"}
        publish_future = publisher.publish(topic_path, data=message_bytes, **attributes)
        message_id = publish_future.result()  # Blocks until the publish is complete.
        logger.debug(f"Published message with ID: {message_id}")
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")