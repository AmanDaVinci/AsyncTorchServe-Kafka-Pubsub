import json
import logging
import asyncio
from image_classification.utils import import_predictor_class
from async_torchserve.stream_processors import BaseStreamProcessor
from async_torchserve.utils import get_producer_consumer_topics

log = logging.getLogger(__name__)


class ModelServer:

    def __init__(self, model_package: str, stream_processor: BaseStreamProcessor):
        self.processor = stream_processor
        model_class = import_predictor_class(model_package)
        self.model = model_class()
        log.info(f"Initialized model server for {self.model.name}")
        self.consumer_topic, self.producer_topic = get_producer_consumer_topics(self.model)
        log.info(f"{self.model.name}: consuming data from topic {self.consumer_topic}")
        log.info(f"{self.model.name}: producing predictions to topic {self.producer_topic}")
    
    async def start(self, loop: asyncio.AbstractEventLoop):
        log.info(f"{self.model.name}: starting producer and consumer")
        await self.processor.start_consumer(loop, self.consumer_topic)
        await self.processor.start_producer(loop, self.producer_topic)
    
    async def process(self):
        async def then_predict_and_push(data):
            data = json.loads(data)
            prediction = self.model(data)
            # serialized_prediction = json.dumps(prediction).encode()
            await self.processor.push(prediction, self.producer_topic)
        await self.processor.pull(then_predict_and_push)
    
    async def stop(self):
        log.info(f"{self.model.name}: stopping stream processor")
        await self.processor.stop()