import sys
import argparse
import configparser
from pathlib import Path
from image_classification.utils import import_predictor_class

sys.path.append(str(Path(__file__).resolve().parents[1]))
from async_torchserve.utils import load_stream_processor, get_producer_consumer_topics


CONFIG_FILE = "config.ini"


def main():
    parser = argparse.ArgumentParser(description="Create or delete topics")
    parser.add_argument("--create", dest="create", action="store_true")
    parser.add_argument("--delete", dest="delete", action="store_true")
    args = parser.parse_args()

    config = configparser.ConfigParser(allow_no_value=True)
    config.read(CONFIG_FILE)
    stream_processor = load_stream_processor(config)
    print(f"Using {stream_processor.__class__.__name__} stream processor")

    for model_package in config["models"]:
        model_class = import_predictor_class(model_package)
        model = model_class()
        consumer_topic, producer_topic = get_producer_consumer_topics(model)
        if args.create:
            stream_processor.create([consumer_topic, producer_topic])
        elif args.delete:
            stream_processor.delete([consumer_topic, producer_topic])

if __name__=="__main__":
    main()