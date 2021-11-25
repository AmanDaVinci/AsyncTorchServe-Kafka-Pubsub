import sys
import asyncio
import argparse
import configparser
from pathlib import Path
from image_classification.datasets.fashion_mnist import FashionMNIST

sys.path.append(str(Path(__file__).resolve().parents[1]))
from async_torchserve.utils import load_stream_broker
from async_torchserve.stream_brokers import BaseStreamBroker

CONFIG_FILE = "config.ini"
ENCODING = "latin-1"


async def push_interactively(loop: asyncio.AbstractEventLoop, 
                             dataset: FashionMNIST, 
                             stream_broker: BaseStreamBroker, 
                             topic: str) -> None:
    await stream_broker.start_producer(loop, topic)
    try:
        images, labels = dataset.load_data()
        print(f"Enter an index from 0 to {len(dataset)-1} to send from dataset")
        for line in sys.stdin:
            index = int(line.split()[0])
            image, label = images[index], labels[index]
            data = {
                "id": index,
                "image": image.tobytes().decode(ENCODING),
                "height": image.height,
                "width": image.width,
                "pillow_mode": image.mode,
                "encoding": ENCODING,
            }
            await stream_broker.push(data, topic)
            print(f"Sent an image of {dataset.classes[label]} class")
    except KeyboardInterrupt:
        print("\nRegistered keyboard interrupt. Stopping.")
        await stream_broker.stop()
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push data to a streaming topic")
    parser.add_argument(
        "--topic", 
        default="model_server.fashion-image-classifier.0.1.inputs",
        type=str
    ) 
    parser.add_argument(
        "--data_directory", 
        default="data/FashionMNIST",
        type=str
    ) 
    parser.add_argument(
        "--download", 
        dest="download",
        action="store_true"
    ) 
    args = parser.parse_args()

    data_dir = Path(args.data_directory)
    data_dir.mkdir(exist_ok=True, parents=True)
    if args.download: print(f"Downloading data to {args.data_directory}")
    dataset = FashionMNIST(is_train=False, data_dir=data_dir, 
                           download_data=args.download, transform=None)
    print(f"Loaded data from {args.data_directory}")

    config = configparser.ConfigParser(allow_no_value=True)
    config.read(CONFIG_FILE)
    stream_broker = load_stream_broker(config)
    print(f"Using {stream_broker.__class__.__name__} stream broker")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        push_interactively(loop, dataset, stream_broker, args.topic)
    )
    loop.close()