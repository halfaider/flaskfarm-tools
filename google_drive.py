import time
import html
import pathlib
import logging
import traceback
from typing import Any

from helpers import apply_cache, get_ttl_hash, check_packages
from config import google as config

check_packages((('httplib2', 'httplib2'), ('google_auth_httplib2', 'google-auth-httplib2'), ('google-api-python-client', 'google-api-python-client')))

from httplib2 import Http
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2 import credentials
from googleapiclient.discovery import build, Resource
from googleapiclient.http import HttpRequest
from googleapiclient import errors

logger = logging.getLogger(__name__)


class GoogleDrive:

    _token = None
    _scopes = None
    _credentials = None
    _api_drive = None
    _api_activity = None
    _cache_enable = False
    _cache_ttl = 600 # seconds
    _cache_maxsize = 64 # each

    def __init__(self, token: dict, scopes: tuple, cache_enable: bool = False, cache_maxsize: int = 64, cache_ttl: int = 600):
        self._token = token
        self._scopes = scopes
        self._credentials: credentials.Credentials = credentials.Credentials.from_authorized_user_info(self.token, self.scopes)
        self._cache_enable = cache_enable
        self._cache_ttl = cache_ttl
        self._cache_maxsize = cache_maxsize
        authorized_http = AuthorizedHttp(self.credentials, http=Http())
        self._api_drive: Resource = build('drive', 'v3', requestBuilder=self.build_google_request, http=authorized_http)
        self._api_activity: Resource = build('driveactivity', 'v2', requestBuilder=self.build_google_request, http=authorized_http)
        if self.cache_enable:
            self.get_file = apply_cache(self.get_file, self.cache_maxsize)

    @property
    def token(self) -> str:
        return self._token

    @property
    def scopes(self) -> tuple:
        return self._scopes

    @property
    def credentials(self) -> credentials.Credentials:
        return self._credentials
    
    @property
    def cache_enable(self) -> bool:
        return self._cache_enable

    @property
    def cache_ttl(self) -> int:
        return self._cache_ttl

    @property
    def cache_maxsize(self) -> int:
        return self._cache_maxsize

    @property
    def api_drive(self) -> Resource:
        return self._api_drive

    @property
    def api_activity(self) -> Resource:
        return self._api_activity

    def build_google_request(self, http: AuthorizedHttp, *args: Any, **kwargs: Any):
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        new_http = AuthorizedHttp(self.credentials, http=Http())
        return HttpRequest(new_http, *args, **kwargs)

    def get_full_path(self, item_id: str, ancestor: str = '') -> tuple[str, tuple[str, str], str]:
        if not item_id:
            raise Exception(f'ID를 확인하세요: "{item_id}"')
        ancestor_id, _, root = ancestor.partition('#')
        # do not use cache
        file = self.get_file(item_id, ttl_hash=time.time())
        web_view = file.get('webViewLink')
        if root and item_id == ancestor_id:
            current_path = [(root, ancestor_id)]
        else:
            current_path = [(file.get('name'), file.get('id'))]
            break_counter = 100
            while file.get('parents') and break_counter > 0:
                ttl_hash = get_ttl_hash(self.cache_ttl) if self.cache_enable else time.time()
                file = self.get_file(file.get('parents')[0], ttl_hash=ttl_hash)
                if root and file.get('id') == ancestor_id:
                    current_path.append((root, ancestor_id))
                    break
                else:
                    current_path.append((file.get('name'), file.get('id')))
                break_counter -= 1
        if len(current_path[-1][1]) < 20:
            current_path[-1] = (f'/{current_path[-1][1]}', current_path[-1][1])
        full_path = pathlib.Path(*[p[0] for p in current_path[::-1] if p[0]])
        parent = current_path[1] if len(current_path) > 1 else current_path[0]
        if self.cache_enable:
            logger.debug(self.get_file.cache_info())
        return str(full_path), parent, web_view

    def get_file(self, item_id: str, fields: str = 'id, name, parents, mimeType, webViewLink', ttl_hash: int | float = 3600) -> dict:
        result = {'id': item_id}
        try:
            result = self.api_drive.files().get(
                fileId=item_id,
                fields=fields,
                supportsAllDrives=True,
            ).execute()
            #logger.debug(f'file={result}')
        except Exception as e:
            self.handle_error(e)
        return result

    def get_files(self, query: str) -> dict:
        result = self.api_drive.files().list(
            q=query,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        return result

    def handle_error(self, error: Exception) -> None:
        if isinstance(error, errors.HttpError):
            logger.error(f'Google: error=HttpError status_code={error.resp.status} reason="{html.escape(error._get_reason().strip())}" uri="{error.uri}"')
        else:
            logger.error(traceback.format_exc())


google_drive = GoogleDrive(config.token, 
                           config.scopes,
                           config.cache_enable,
                           config.cache_maxsize,
                           config.cache_ttl)
