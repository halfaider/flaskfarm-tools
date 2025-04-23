import os
import sys
import copy
import json
import pathlib
import logging
import dataclasses
from typing import Mapping, Sequence

import yaml

from helpers import set_logger

logger = logging.getLogger(__name__)


def get_default_headers() -> Mapping[str, str]:
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }


def get_default_extra_agents() -> Mapping[int, str]:
    return {
        1: 'com.plexapp.agents.themoviedb',
        2: 'com.plexapp.agents.thetvdb',
        8: 'com.plexapp.agents.lastfm',
        9: 'com.plexapp.agents.lastfm',
    }


def get_default_media_types() -> Mapping[int, str]:
    return {
        1: 'movie',
        2: 'show',
        3: 'season',
        4: 'episode',
        5: 'trailer',
        6: 'comic',
        7: 'person',
        8: 'artist',
        9: 'album',
        10: 'track',
        11: 'picture',
        12: 'clip',
        13: 'photo',
        14: 'photoalbum',
        15: 'playlist',
        16: 'playlistFolder',
        18: 'collection',
        42: 'optimizedVersion'
    }


@dataclasses.dataclass
class _BaseConfig:
    dry_run: bool = True
    workers: int = 2
    batch_size: int = 100
    retry: int = 10
    countdown: int = 5
    mappings: Mapping[str, str] = dataclasses.field(default_factory=dict)
    headers: Mapping[str, str] = dataclasses.field(default_factory=get_default_headers)

    def map_path(self, target: str) -> str:
        for old, new in self.mappings.items():
            if old in target:
                return target.replace(old, new)
        return target


@dataclasses.dataclass
class _PlexConfig:
    url: str
    token: str
    application: str = '/usr/lib/plexmediaserver'
    support: str = '/var/lib/plexmediaserver/Library/Application Support/Plex Media Server'
    machine_id: str = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
    link: str =  None
    check_count: int = 60
    check_interval: int = 10
    force_rematch: bool = False
    score_min: int = 70
    score_min_extra: int = -1
    title_match_ratio: float = 0.7
    margin_of_year: int = 1000
    extra_agents: Mapping[int, str] = dataclasses.field(default_factory=get_default_extra_agents)
    db: str = None
    metadata: str = None
    media: str = None
    sqlite: str = None
    metadata_url_columns: Sequence[str] = ('user_thumb_url', 'user_art_url', 'user_banner_url', 'user_music_url', 'user_clear_logo_url')
    media_types: Mapping[int, str] = dataclasses.field(default_factory=get_default_media_types)


@dataclasses.dataclass
class PlexConfig(_BaseConfig, _PlexConfig):

    def __post_init__(self) -> None:
        if not self.url or not self.token:
            raise Exception('url 또는 token 값이 없습니다.')
        if not self.link:
            self.link = f'{self.url}/web/index.html#!/server/{self.machine_id}/details?key=%2Flibrary%2Fmetadata%2F'
        self.headers.update({
            'Accept': 'application/json',
            'X-Plex-Token': self.token
        })
        if not self.db:
            self.db = f'{self.support}/Plug-in Support/Databases/com.plexapp.plugins.library.db'
        if not self.metadata:
            self.metadata = f'{self.support}/Metadata'
        if not self.media:
            self.media = f'{self.support}/Media'
        if not self.sqlite:
            self.sqlite = f'{self.application}/Plex SQLite'


@dataclasses.dataclass
class _KavitaConfig:
    url: str
    apikey: str
    db: str


@dataclasses.dataclass
class KavitaConfig(_BaseConfig, _KavitaConfig):

    def __post_init__(self) -> None:
        if not self.url or not self.apikey:
            raise Exception('url 또는 apikey 값이 없습니다.')


yaml_config = None
for yaml_file in (pathlib.Path(os.getcwd(), 'config.yaml'),
                  pathlib.Path(__file__).with_name('config.yaml')):
    try:
        with open(yaml_file, 'r') as file_stream:
            yaml_config = yaml.safe_load(file_stream)
            logger.info(f'{yaml_file.resolve()} 파일을 불러왔습니다.')
            break
    except:
        pass
else:
    raise Exception('config.yaml 파일을 불러오지 못 했습니다.')

if not yaml_config:
    raise Exception('설정 값을 가져올 수 없습니다.')

# logging
config_py = pathlib.Path(__file__)
modules = set(file.stem for file in config_py.parent.glob('*.py'))
if '__main__' not in modules:
    modules.add('__main__')
log_settings = yaml_config.pop('logging', {}) or {}
set_logger(
    level=log_settings.get('level'),
    format=log_settings.get('format'),
    date_format=log_settings.get('date_format'),
    redacted_patterns=log_settings.get('redacted_patterns'),
    redacted_substitute=log_settings.get('redacted_substitute'),
    loggers=modules,
)

this = sys.modules[__name__]
default_config = yaml_config.pop('default', {}) or {}
for key in yaml_config:
    base = copy.deepcopy(default_config)
    base.update(yaml_config[key] or {})
    class_name = key.capitalize() + 'Config'
    class_ = getattr(this, class_name)
    setattr(this, key, class_(**base))

if __name__ == '__main__':
    #logger.debug(getattr(this, 'plex'))
    #logger.debug(getattr(this, 'kavita'))
    print(json.dumps(dataclasses.asdict(getattr(this, 'plex')), indent=2))
    print(json.dumps(dataclasses.asdict(getattr(this, 'kavita')), indent=2))
