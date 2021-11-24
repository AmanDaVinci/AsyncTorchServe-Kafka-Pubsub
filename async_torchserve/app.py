import argparse
import asyncio
import logging
import configparser
from async_torchserve.model_server import ModelServer
from async_torchserve.utils import load_stream_processor

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main(config: configparser.ConfigParser):
    stream_processor = load_stream_processor(config)
    log.info(f"Using {stream_processor.__class__.__name__} stream processor")

    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    model_servers = [
        ModelServer(model_package, stream_processor) 
        for model_package in config["models"]
    ]
    log.info("Starting all model servers")
    for model_server in model_servers:
        loop.run_until_complete(model_server.start(loop))
    try:
        log.info("Ready to serve predictions...")
        for model_server in model_servers:
            loop.run_until_complete(model_server.process())
    except KeyboardInterrupt:
        log.info("Model serving interrupted")
    finally:
        for model_server in model_servers:
            loop.run_until_complete(model_server.stop())
        loop.close()
        log.info("Shutdown all model servers successfully")


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Run asynchronous model servers")
    parser.add_argument("--config", type=str, help="path to configuration file") 
    args = parser.parse_args()
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(args.config)
    main(config)