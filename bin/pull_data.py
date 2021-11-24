import sys
import json
import argparse
import asyncio
import configparser
from pathlib import Path
from async_torchserve.stream_processors import BaseStreamProcessor

sys.path.append(str(Path(__file__).resolve().parents[1]))
from async_torchserve.utils import load_stream_processor

CONFIG_FILE = "config.ini"


async def streaming_pull(loop: asyncio.AbstractEventLoop, 
                         stream_processor: BaseStreamProcessor, 
                         topic: str) -> None:
    await stream_processor.start_consumer(loop, topic)
    print(f"Pulling from: {topic}")
    async def then_print_data(data): 
        print(json.loads(data))
    try:
        await stream_processor.pull(then_print_data)
    except KeyboardInterrupt:
        print("\nRegistered keyboard interrupt. Stopping.")
        await stream_processor.stop()
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Receive data from a streaming topic")
    parser.add_argument(
        "--topic", 
        default="model_server.fashion-image-classifier.0.1.outputs",
        type=str
    ) 
    args = parser.parse_args()

    config = configparser.ConfigParser(allow_no_value=True)
    config.read(CONFIG_FILE)
    stream_processor = load_stream_processor(config)
    print(f"Using {stream_processor.__class__.__name__} stream processor")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        streaming_pull(loop, stream_processor, args.topic)
    )
    loop.close()
