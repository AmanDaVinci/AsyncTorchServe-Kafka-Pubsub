import os
import json
import logging
import asyncio
import configparser
from abc import ABC, abstractmethod
from typing import Any, Callable, List

from kafka.admin import KafkaAdminClient, NewTopic
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from subprocess import run
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message


log = logging.getLogger(__name__)


class BaseStreamBroker(ABC):

    @abstractmethod
    def __init__(self, config: configparser.ConfigParser) -> None:
        raise NotImplementedError

    @abstractmethod
    def create(self, topics: List[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, topics: List[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start_producer(self, loop: asyncio.AbstractEventLoop, topic: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start_consumer(self, loop: asyncio.AbstractEventLoop, topic: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def push(self, data: Any, topic: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def pull(self, callback: Callable) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    
class Kafka(BaseStreamBroker):

    def __init__(self, config: configparser.ConfigParser) -> None:
        self.producer, self.consumer = None, None
        self.servers=config["bootstrap_servers"]
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.servers)

    def create(self, topics: List[str]) -> None:
        topics = [
            NewTopic(topic, num_partitions=1, replication_factor=1)
            for topic in topics
        ]
        try:
            self.admin_client.create_topics(topics, validate_only=False)
        except TopicAlreadyExistsError as e:
            log.error("Topics already exist.")
    
    def delete(self, topics: List[str]) -> None:
        try:
            self.admin_client.delete_topics(topics)
        except UnknownTopicOrPartitionError as e:
            log.error("Topics have not been created yet.")
    
    async def start_producer(self, loop: asyncio.AbstractEventLoop, topic: str) -> None:
        self.producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers=self.servers, compression_type="gzip",
            value_serializer= lambda x: json.dumps(x).encode(),
        )
        await self.producer.start()

    async def start_consumer(self, loop: asyncio.AbstractEventLoop, topic: str) -> None:
        self.consumer = AIOKafkaConsumer(
            topic, loop=loop, bootstrap_servers=self.servers
        )
        await self.consumer.start()

    async def push(self, data: Any, topic: str) -> None:
        if self.producer is None:
            log.error("Producer has not been started yet.")
        await self.producer.send_and_wait(topic, data)
    
    async def pull(self, callback: Callable) -> None:
        if self.consumer is None:
            log.error("Consumer has not been started yet.")
        async for data in self.consumer:
            await callback(data.value)

    async def stop(self) -> None:
        if self.consumer: await self.consumer.stop()
        if self.producer:  await self.producer.stop()


class PubSub(BaseStreamBroker):

    def __init__(self, config: configparser.ConfigParser) -> None:
        self.publisher, self.subscriber = None, None
        self.project_id = config["project"]
        self.subscription_suffix = ".sub"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["account_keypath"]
        try:
            run(["gcloud", "info"], check=True)
        except FileNotFoundError as e:
            log.error("Most likely gcloud is not available.")
            log.error("Please install gcloud: https://cloud.google.com/sdk/docs/install")

    # TODO: maybe rewrite with publisher.create_topic
    def create(self, topics: List[str]) -> None:
        for topic in topics:
            subscription = f"{topic}.sub"
            run(["gcloud", "pubsub", "topics", "create", topic], check=True)
            run(["gcloud", "pubsub", "subscriptions", "create", subscription, "--topic", topic], check=True)

    def delete(self, topics: List[str]) -> None:
        for topic in topics:
            subscription = f"{topic}.sub"
            run(["gcloud", "pubsub", "subscriptions", "delete", subscription], check=True)
            run(["gcloud", "pubsub", "topics", "delete", topic], check=True)
    
    async def start_producer(self, loop: asyncio.AbstractEventLoop, topic: str) -> None:
        self.publisher = pubsub_v1.PublisherClient()

    async def start_consumer(self, loop: asyncio.AbstractEventLoop, topic: str) -> None:
        self.subscriber = pubsub_v1.SubscriberClient()
        self.sub_path = self.subscriber.subscription_path(
            project=self.project_id, subscription=topic+self.subscription_suffix
        )

    async def push(self, data: Any, topic: str) -> None:
        if self.publisher is None:
            log.error("Publisher has not been started yet.")
        topic_path = self.publisher.topic_path(self.project_id, topic)
        future = self.publisher.publish(topic_path, json.dumps(data).encode())

    async def pull(self, callback: Callable) -> None:
        if self.subscriber is None:
            log.error("Subscriber has not been started yet.")
        async def wrapper_callback(message: Message, callback=callback) -> None:
            await callback(message.message.data)
        while True:
            response = self.subscriber.pull(
                request={"max_messages": 1, "subscription": self.sub_path}
            )
            if response.received_messages:
                ack_ids = []
                for msg in response.received_messages:
                    await wrapper_callback(msg)
                    ack_ids.append(msg.ack_id)
                self.subscriber.acknowledge(
                    request={"ack_ids": ack_ids, "subscription": self.sub_path}
                )

    async def stop(self) -> None:
        if self.subscriber: self.subscriber.close()