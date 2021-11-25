import sys
import json
import argparse
import asyncio
import configparser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from async_torchserve.utils import load_stream_broker
from async_torchserve.stream_brokers import BaseStreamBroker

CONFIG_FILE = "config.ini"


async def streaming_pull(loop: asyncio.AbstractEventLoop, 
                         stream_broker: BaseStreamBroker, 
                         topic: str) -> None:
    await stream_broker.start_consumer(loop, topic)
    print(f"Pulling from: {topic}")
    async def then_print_data(data): 
        print(json.loads(data))
    try:
        await stream_broker.pull(then_print_data)
    except KeyboardInterrupt:
        print("\nRegistered keyboard interrupt. Stopping.")
        await stream_broker.stop()
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
    stream_broker = load_stream_broker(config)
    print(f"Using {stream_broker.__class__.__name__} stream broker")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        streaming_pull(loop, stream_broker, args.topic)
    )
    loop.close()
