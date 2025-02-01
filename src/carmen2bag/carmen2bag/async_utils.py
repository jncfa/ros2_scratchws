from queue import Empty
from typing import TypeVar
import asyncio
import concurrent
from typing import AsyncGenerator
from multiprocessing import Queue, Process

def get_until_timeout(shared_queue: Queue, timeout: float = 0.01):
    try:
        return shared_queue.get(timeout=timeout)
    except Empty:
        return None

def read_file(input_file: str, shared_queue: Queue):
    with open(input_file) as f:
        for line in f:
            shared_queue.put(line)
            
async def read_line_by_line(loop: asyncio.BaseEventLoop, input_file: str, executor: concurrent.futures.Executor | None = None) -> AsyncGenerator[str, None]:
    # spin up process that reads file with shared queue
    shared_queue = Queue()

    try:
        file_read_bg = Process(target=read_file, args=(input_file, shared_queue))
        file_read_bg.start()

        # loop while we expect more data to come
        while file_read_bg.is_alive() or not shared_queue.empty():
            data = await loop.run_in_executor(executor, get_until_timeout, shared_queue)
            
            if (data is not None):
                yield data
    
    finally:
        if file_read_bg.is_alive():
            file_read_bg.kill()