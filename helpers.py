import re
import sys
import time
import asyncio
import logging
import sqlite3
import datetime
import functools
import subprocess
from typing import Any, Generator, Sequence, Iterable, Coroutine, Callable


def check_packages(packages: Iterable[Sequence[str]]) -> None:
    for pkg, pi in packages:
        try:
            __import__(pkg)
        except:
            subprocess.check_call((sys.executable, '-m', 'pip', 'install', '-U', pi))


check_packages((('psutil', 'psutil'), ('aiohttp', 'aiohttp')))

import psutil
import aiohttp

logger = logging.getLogger(__name__)


def mem_usage() -> int:
    return psutil.Process().memory_info().rss / ( 1024 * 1024 ) # MB


async def stop() -> None:
    loop = asyncio.get_event_loop()
    loop.stop()
    loop.close()


def run(cmd: Sequence[str], timeout: int = 300, **kwds: Any) -> Generator[str, None, bool]:
    process: subprocess.Popen = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        **kwds
    )
    last_time = time.time()
    while process.returncode is None:
        process.poll()
        line = process.stdout.readline().strip()
        if line:
            last_time = time.time()
            yield line
        elif time.time() - last_time > timeout:
            process.kill()
            logger.error(f'timeout: {process.pid} {cmd}')
            break
        time.sleep(0.01)
    if process.returncode == 0:
        return True
    else:
        logger.error("\n".join(process.stderr.readlines()))
        return False


def dict_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict:
    fields: Generator = (column[0] for column in cursor.description)
    return {key: value for key, value in zip(fields, row)}


def retrieve_db(database: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrap(*args: Any, **kwds: Any) -> Any:
            with sqlite3.connect(database) as con:
                con.row_factory = dict_factory
                return func(*args, con=con, **kwds)
        return wrap
    return decorator


async def check_tasks(tasks: list[asyncio.Task], interval: int = 60) -> None:
    last_time = time.time()
    while tasks:
        check = False
        if time.time() - last_time > interval:
            last_time = time.time()
            check = True
        done_tasks = []
        for task in tasks:
            name = task.get_name()
            if task.done():
                logger.debug(f'The task is done: "{name}"')
                done_tasks.append(task)
                if exception := task.exception():
                    logger.error(f'{name}: {exception}')
            else:
                if check:
                    logger.debug(f'{name}: {task.get_stack()}')
        for task in done_tasks:
            tasks.remove(task)
        await asyncio.sleep(1)


def set_logger(level: str = None,
               format: str = None,
               date_format: str = None,
               redacted_patterns: Iterable = None,
               redacted_substitute: str = None,
               handlers: Iterable = None,
               loggers: Iterable = None) -> None:
    try:
        level = getattr(logging, (level or 'info').upper(), logging.INFO)
        fomatter = RedactedFormatter(
            patterns=redacted_patterns or (r'apikey=(.{10})',),
            substitute=redacted_substitute or '<REDACTED>',
            fmt=format or '%(asctime)s|%(levelname)8s| %(message)s <%(filename)s:%(lineno)d#%(funcName)s>',
            datefmt=date_format or '%Y-%m-%dT%H:%M:%S'
        )
        if not handlers:
            handlers = (logging.StreamHandler(),)
        for mod in loggers or ():
            module_logger = logging.getLogger(mod)
            module_logger.setLevel(level)
            for handler in handlers:
                if not any(isinstance(h, type(handler)) for h in module_logger.handlers):
                    handler.setFormatter(fomatter)
                    module_logger.addHandler(handler)
    except Exception as e:
        logger.warning(f'로깅 설정 실패: {e}', exc_info=True)
        logging.basicConfig(
            level=level or logging.DEBUG,
            format=format or '%(asctime)s|%(levelname)8s| %(message)s <%(filename)s:%(lineno)d#%(funcName)s>',
            datefmt=date_format or '%Y-%m-%dT%H:%M:%S'
        )


async def queue_task(coroutine: Coroutine, queue: asyncio.Queue, data: Iterable, *args: Any, task_size: int = 1, prefix: str = 'task', interval: int = 60, **kwds: Any) -> None:
    tasks = []
    for i in range(task_size):
        name = f"{prefix}-{i}"
        task = asyncio.create_task(coroutine(queue, name, *args, **kwds), name=name)
        tasks.append(task)
    check_task = asyncio.create_task(check_tasks(tasks, interval=interval), name=f'checking-{prefix}')
    whole_tasks = (check_task, *tasks)
    for item in data:
        await queue.put(item)
    for _ in range(task_size):
        await queue.put(None)
    try:
        await asyncio.gather(*whole_tasks)
    except asyncio.CancelledError:
        for task in whole_tasks:
            if task.cancelled():
                logger.warning(f"Canceled: {task.get_name()}")
            elif exception := task.exception():
                logger.error(f"{task.get_name()}: {exception}")


def countdown(seconds: int) -> None:
    for i in range(seconds, 0, -1):
        time.sleep(1)
        logger.debug(f'{i}....')


class RedactedFormatter(logging.Formatter):

    def __init__(self, *args: Any, patterns: Iterable = (), substitute: str = '<REDACTED>', **kwds: Any):
        super(RedactedFormatter, self).__init__(*args, **kwds)
        self.patterns = tuple(re.compile(pattern, re.I) for pattern in patterns)
        self.substitute = substitute

    def format(self, record):
        msg = super().format(record)
        for pattern in self.patterns:
            match = pattern.search(msg)
            if match:
                groups = groups if len(groups := match.groups()) > 0 else (match.group(0),)
                for found in groups:
                    msg = self.redact(re.compile(found, re.I), msg)
        return msg

    def formatTime(self, record: logging.LogRecord, datefmt: str = None):
        dt = datetime.datetime.fromtimestamp(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
            return s[:-3]
        else:
            return super().formatTime(record, datefmt)

    def redact(self, pattern: re.Pattern, text: str) -> str:
        return pattern.sub(self.substitute, text)


def http_api(default_headers: dict = None, timeout: int = 30) -> Callable:
    def decorator(func: Callable) -> Coroutine:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwds: Any) -> dict:
            api: dict = func(*args, **kwds) or {}
            params: dict = api.get('params')
            data: dict = api.get('data')
            json_: dict = api.get('json')
            headers: dict = api.get('headers') or {}
            auth: tuple = api.get('auth')
            url: str = api.get('url')
            method: str = api.get('method')
            result = {
                'status_code': 0,
                'text': '',
                'exception': '',
                'json': {},
                'url': '',
            }
            async with aiohttp.ClientSession(headers=default_headers, timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                try:
                    async with session.request(method, url, params=params, json=json_, data=data, auth=auth, headers=headers) as response:
                        result['status_code'] = response.status
                        result['url'] = response.url
                        if response.content:
                            result['text'] = await response.text()
                        if response.content and response.content_type == 'application/json':
                            result['json'] = await response.json()
                except Exception as e:
                    logger.warning(repr(e))
                    result['exception'] = str(e)
            return result
        return wrapper
    return decorator


def apply_cache(func: Callable, maxsize: int = 64) -> Callable:
    @functools.lru_cache(maxsize=maxsize)
    def wrapper(*args: Any, ttl_hash: int = 3600, **kwds: Any):
        del ttl_hash
        return func(*args, **kwds)
    return wrapper


def get_ttl_hash(seconds: int = 3600) -> int:
    return round(time.time() / seconds)
