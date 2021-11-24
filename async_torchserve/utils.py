import configparser
from typing import Any, Tuple
from importlib import import_module
from base_predictor import BasePredictor
from async_torchserve.stream_processors import BaseStreamProcessor


def get_producer_consumer_topics(model: BasePredictor) -> Tuple[str, str]:
    base_topic_name = ".".join([
        "model_server",
        model.name,
        str(model.major_version),
        str(model.minor_version),
    ])
    return f"{base_topic_name}.inputs", f"{base_topic_name}.outputs"

def load_stream_processor(config: configparser.ConfigParser) -> BaseStreamProcessor:
    module_name = "async_torchserve.stream_processors"
    class_name = config["options"]["stream_processor"]
    stream_processor_class = getattr(import_module(module_name), class_name)
    return stream_processor_class(config[class_name])