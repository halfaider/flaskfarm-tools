import asyncio
import logging
import urllib.parse
from typing import Any

from config import kavita as config
from helpers import http_api

logger = logging.getLogger(__name__)
http_api = http_api(config.headers)


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

async def main(*args: Any, **kwds: Any) -> None:
    result = await plugin_authenticate()
    print(result)


if __name__ == '__main__':
    asyncio.run(main())
