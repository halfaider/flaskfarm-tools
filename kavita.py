import asyncio
import logging
import urllib.parse
import sqlite3
from typing import Any, Generator

from config import kavita as config
from helpers import http_api, retrieve_db

logger = logging.getLogger(__name__)
http_api = http_api(config.headers)
retrieve_db = retrieve_db(config.db)


@http_api
def plugin_authenticate(url: str = config.url, plugin_name: str = 'Kavita DB tools', apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Plugin/authenticate'),
        'params': {'pluginName': plugin_name, 'apiKey': apikey},
        'method': 'POST',
    }


@http_api
def scan_folder(folder: str, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-folder'),
        'json': {'folderPath': folder, 'apiKey': apikey},
        'method': 'POST',
    }


@retrieve_db
def fetch_one(query: str, con: sqlite3.Connection) -> dict:
    return con.execute(query).fetchone()


@retrieve_db
def fetch_all(query: str, con: sqlite3.Connection) -> Generator[dict, None, None]:
    for row in con.execute(query):
        yield row


async def main(*args: Any, **kwds: Any) -> None:
    result = await plugin_authenticate()
    print(result)


if __name__ == '__main__':
    asyncio.run(main())
