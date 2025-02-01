import argparse

import concurrent
import concurrent.futures
from carmen2bag.vendor.rich_argparse  import RichHelpFormatter
from rich.console import Console
from typing import NamedTuple 
from typing import Generator, AsyncGenerator

import asyncio
import contextlib
import rosbag2_py


from carmen2bag.carmen_converter import RosMsg, carmen2ros
from carmen2bag.async_utils import read_line_by_line

console = Console()

@contextlib.contextmanager
def bag_writer(output_file: str, storage_id: str, storage_preset_profile: str | None) -> Generator[rosbag2_py.SequentialWriter, None, None]:
    writer = rosbag2_py.SequentialWriter()
    writer.open(
        rosbag2_py.StorageOptions(uri=output_file, storage_id=storage_id, storage_preset_profile=storage_preset_profile),
        rosbag2_py.ConverterOptions(
            input_serialization_format="cdr", output_serialization_format="cdr"
        ),
    )
    try:
        yield writer
    finally:
        del writer


async def write_messages(loop: asyncio.BaseEventLoop, output_file: str, storage_id: str, storage_preset_profile: str | None, messages: AsyncGenerator[RosMsg, None], executor: concurrent.futures.Executor | None = None):
    with bag_writer(output_file=output_file, storage_id=storage_id, storage_preset_profile=storage_preset_profile) as writer:
        topic_cache = set()

        for msg in messages:

            # create topic if we did not see it before
            if msg.topic not in topic_cache:
                writer.create_topic(
                    rosbag2_py.TopicMetadata(
                        name=msg.topic, type=msg.type, serialization_format="cdr"
                    )
                )
                topic_cache.add(msg.topic)
            loop.add_reader()
            # defer execution while writing data
            await loop.run_in_executor(executor, writer.write, msg.topic, msg.data, msg.timestamp)

async def read_messages(input_file: str, loop: asyncio.BaseEventLoop = asyncio.get_event_loop(), executor: concurrent.futures.Executor | None = None) -> AsyncGenerator[RosMsg, None]:
    async for line in read_line_by_line(loop, input_file, executor):
        # skip comments
        if not line.startswith("#"):
            yield carmen2ros(line)

async def print_messages(input_file: str):
    async for k in read_messages(input_file):
        console.print(k)

class Carmen2BagArgs(NamedTuple):
    input_file: str
    output_file: str
    storage_id: str
    storage_preset_profile: str | None

def get_args() -> Carmen2BagArgs:
    parser = argparse.ArgumentParser(
                prog='carmen2bag',
                description='What the program does',
                epilog='Text at the bottom of help', 
                formatter_class=RichHelpFormatter)
        
    parser.add_argument('-i', '--input', type=str, required=True, help='Input CARMEN file')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output bag file')
    parser.add_argument('--storage', type=str, choices=list(rosbag2_py.get_registered_readers()),
                        default=rosbag2_py.get_default_storage_id(), help='Type of storage to use')
    parser.add_argument('--storage-preset-profile', type=str,
                    default=None, help='Optional storage preset profile to use')
    
    parsed_args = parser.parse_args()

    return Carmen2BagArgs(input_file=parsed_args.input,
                          output_file=parsed_args.output,
                          storage_id=parsed_args.storage,
                          storage_preset_profile=parsed_args.storage_preset_profile)

def main():
    args = get_args()
    console.print(args)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(print_messages(args.input_file))
    
if __name__ == '__main__':
    main()
