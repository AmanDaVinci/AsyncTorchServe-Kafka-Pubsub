import configparser
from typing import Tuple
from importlib import import_module
from base_predictor import BasePredictor
from async_torchserve.stream_brokers import BaseStreamBroker


def get_producer_consumer_topics(model: BasePredictor) -> Tuple[str, str]:
    base_topic_name = ".".join([
        "model_server",
        model.name,
        str(model.major_version),
        str(model.minor_version),
    ])
    return f"{base_topic_name}.inputs", f"{base_topic_name}.outputs"

def load_stream_broker(config: configparser.ConfigParser) -> BaseStreamBroker:
    module_name = "async_torchserve.stream_brokers"
    class_name = config["options"]["stream_broker"]
    stream_broker_class = getattr(import_module(module_name), class_name)
    return stream_broker_class(config[class_name])