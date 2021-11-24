import sys
import asyncio
import argparse
import configparser
from pathlib import Path
from image_classification.datasets.fashion_mnist import FashionMNIST
from async_torchserve.stream_processors import BaseStreamProcessor

sys.path.append(str(Path(__file__).resolve().parents[1]))
from async_torchserve.utils import load_stream_processor

CONFIG_FILE = "config.ini"
ENCODING = "latin-1"


async def push_interactively(loop: asyncio.AbstractEventLoop, 
                             dataset: FashionMNIST, 
                             stream_processor: BaseStreamProcessor, 
                             topic: str) -> None:
    await stream_processor.start_producer(loop, topic)
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
            await stream_processor.push(data, topic)
            print(f"Sent an image of {dataset.classes[label]} class")
    except KeyboardInterrupt:
        print("\nRegistered keyboard interrupt. Stopping.")
        await stream_processor.stop()
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
    args = parser.parse_args()

    data_dir = Path(args.data_directory)
    data_dir.mkdir(exist_ok=True, parents=True)
    dataset = FashionMNIST(is_train=False, data_dir=data_dir, 
                           download_data=False, transform=None)
    print(f"Loaded data from {args.data_directory}")

    config = configparser.ConfigParser(allow_no_value=True)
    config.read(CONFIG_FILE)
    stream_processor = load_stream_processor(config)
    print(f"Using {stream_processor.__class__.__name__} stream processor")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        push_interactively(loop, dataset, stream_processor, args.topic)
    )
    loop.close()