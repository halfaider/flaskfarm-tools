import asyncio
import logging
import sqlite3
import pathlib
import datetime
import urllib.parse
from typing import Any, Generator, Sequence

from config import kavita as config
from helpers import http_api, retrieve_db, string_bool

logger = logging.getLogger(__name__)
http_api = http_api(config.headers)
retrieve_db = retrieve_db(config.db)


async def get_headers(require_token: bool = True, url: str = config.url, apikey: str = config.apikey) -> dict | None:
    headers = {
        "Content-Type": "application/json"
    }
    if require_token:
        result = await plugin_authenticate(url=url, apikey=apikey)
        if not 300 > (result.get('status_code') or 0) > 199:
            logger.error(f'인증 실패: {result}')
        token = result.get('json').get('token')
        headers['Authorization'] = f"Bearer {token}"
    return headers


@http_api
async def plugin_authenticate(url: str = config.url, apikey: str = config.apikey, plugin_name: str = config.plugin_name) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Plugin/authenticate'),
        'params': {'pluginName': plugin_name, 'apiKey': apikey},
        'method': 'POST',
    }


@http_api
async def scan_folder(folder: str, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-folder'),
        'json': {'folderPath': folder, 'apiKey': apikey},
        'method': 'POST',
    }


@http_api
async def scan(library_id: int | str, force: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan'),
        'params': {'libraryId': library_id, 'force': string_bool(force)},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api
async def scan_all(force: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-all'),
        'params': {'force': string_bool(force)},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api
async def scan_series(series_id: int, library_id: int = -1, force: bool = False, colorscape: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    if library_id < 1:
        row = fetch_one('SELECT LibraryId FROM Series WHERE id = ?', (series_id,))
        if row:
            library_id = row['LibraryId']
    return {
        'url': urllib.parse.urljoin(url, '/api/Series/scan'),
        'json': {'libraryId': library_id, 'seriesId': series_id, 'forceUpdate': force, 'forceColorscape': colorscape},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


async def scan_series_by_path(path: str, is_dir: bool = False, force: bool = False, colorscape: bool = False, url: str = config.url, apikey: str = config.apikey) -> None:
    path = pathlib.Path(path)
    rows = tuple(fetch_all(f'SELECT Id, LibraryId, Name FROM Series WHERE FolderPath = ?', (str(path if is_dir else path.parent),)))
    # path로 하나의 series만 검색이 되어야 실행
    if not len(rows) == 1:
        logger.error(f'스캔 경로 확인: {str(path)}')
        return
    logger.info(f'Scan: {rows[0]["Name"]}')
    await scan_series(rows[0]['Id'], library_id=rows[0]['LibraryId'], force=force, colorscape=colorscape, url=url, apikey=apikey)


@http_api
async def scan_multiple(library_ids: Sequence[int | str], force: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    # 작동 안 하는 듯
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-multiple'),
        'json': {'ids': library_ids, 'force': force},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api
async def jobs(url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Server/jobs'),
        'method': 'GET',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@retrieve_db
def fetch_one(query: str, params: Sequence[str] | dict[str, str] = (), con: sqlite3.Connection = None) -> dict:
    return con.execute(query, params).fetchone()


@retrieve_db
def fetch_all(query: str, params: Sequence[str] | dict[str, str] = (), con: sqlite3.Connection = None) -> Generator[dict, None, None]:
    for row in con.execute(query, params):
        yield row


@retrieve_db
def execute(query: str, params: Sequence[str] | dict[str, str] = (), con: sqlite3.Connection = None) -> sqlite3.Cursor:
    return con.execute(query, params)


def get_library_by_cover(cover: str, con: sqlite3.Connection = None) -> int:
    """
    Library:
        Series: LibraryId
            Volume: SeriesId
                Chapter: VolumeId

    Library
        l{library_id}.ext
        l22.png

        `l`로 시작하는 cover는 Library 테이블에서만 사용
        id는 실제와 일치하지 않을 수 있음
    Series
        s{series_id}.ext 사용자 지정
        _s{series_id}.ext 자동 생성
        _s11271.png

        Volume, Chapter 테이블에서 _s{series_id}.ext 커버를 사용하기도 함
    Volume, Chapter
        v{volume_id}_c{chapter_id}.ext
        v249724_c310060.png

        Series 테이블에서 v{volume_id}_c{chapter_id}.ext 커버를 사용하기도 함
    """
    queries = (
        # 1. Library
        "SELECT id as LibraryId FROM Library WHERE CoverImage = ?",
        # 2. Series
        "SELECT LibraryId FROM Series WHERE CoverImage = ?",
        # 3. Volume
        """
        SELECT s.LibraryId
        FROM Volume AS v
        JOIN Series AS s ON v.SeriesId = s.id
        WHERE v.CoverImage = ?
        """,
        # 4. Chapter
        """
        SELECT s.LibraryId
        FROM Chapter AS c
        JOIN Volume AS v ON c.VolumeId = v.id
        JOIN Series AS s ON v.SeriesId = s.id
        WHERE c.CoverImage = ?
        """
    )
    if cover.startswith('l'):
        row = con.execute(queries[0], (cover,)).fetchone()
        if row:
            return row['LibraryId']
    else:
        for sql in queries[1:]:
            row = con.execute(sql, (cover,)).fetchone()
            if row:
                return row['LibraryId']
    return -1


def get_tables_using_cover(cover: str, con: sqlite3.Connection = None) -> list:
    tables = []
    for table in config.tables_with_cover:
        row = con.execute(f'SELECT id FROM {table} WHERE CoverImage = ?', (cover,)).fetchone()
        if row:
            tables.append(table)
    return tables


def is_cover_used(cover: str, con: sqlite3.Connection = None) -> bool:
    for table in config.tables_with_cover:
        row = con.execute(f'SELECT id FROM {table} WHERE CoverImage = ?', (cover,)).fetchone()
        if row:
            return True
    return False


def should_be_ignored(cover: pathlib.Path) -> bool:
    if cover.is_dir():
        return True
    for pattern in config.ignore_cover_patterns:
        if pattern.search(cover.name):
            return True
    return False


def print_fails(fails: list[tuple[pathlib.Path, str]]) -> None:
    if not fails:
        return
    for fail in fails:
        logger.info(f'{str(fail[0])} reason="{str(fail[1])}"')
    logger.info(f'총 개수: {len(fails)}')


@retrieve_db
def organize_covers(covers: str = '/kavita/config/covers', dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """커버 이미지를 각 라이브러리 폴더로 이동. 하위 폴더는 검색하지 않음. 데이터베이스에서 커버 이미지로 라이브러리 ID를 검색한 후 그 ID로 폴더를 생성하여 이동.
    Args:
        covers: 커버 폴더 경로
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:

    Examples:
        >>> organize_covers('/docker/volumes/kavita/config/covers', dry_run=True)
    """
    path_covers = pathlib.Path(covers)
    fails = []
    for path in path_covers.glob('*'):
        if should_be_ignored(path):
            # 디렉토리, 공통으로 사용하는 커버는 제외
            continue
        library_id = get_library_by_cover(path.name, con=con)
        if library_id < 1:
            # 라이브러리를 알 수 없는 커버는 제외
            continue
        new_path = path_covers / f'{library_id}' / path.name
        try:
            if not dry_run and not new_path.parent.exists():
                logger.debug(f'Create: {new_path.parent}')
                new_path.parent.mkdir(parents=True)
            logger.info(f'{path} -> {new_path}')
            if not dry_run:
                path.rename(new_path)
            new_value = f'{library_id}/{path.name}'
            logger.info(f'Update CoverImage with {new_value}')
            for table in config.tables_with_cover:
                if not dry_run:
                    con.execute(f'UPDATE {table} SET CoverImage = ? WHERE CoverImage = ?', (new_value, path.name))
        except Exception as e:
            logger.exception(f'커버 정리 실패: {path}')
            fails.append((path, str(e)))
    print_fails(fails)


@retrieve_db
def clean_covers(covers: str = '/kavita/config/covers', recursive: bool = True, dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """데이터베이스에서 커버 이미지를 사용중인 레코드가 없으면 삭제
    Args:
        covers: 커버 폴더 경로
        recursive: 하위 폴더 탐색 여부
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:

    Examples:
        >>> clean_covers('/docker/volumes/kavita/config/covers', dry_run=True)
    """
    path_covers = pathlib.Path(covers)
    fails = []
    for path in path_covers.rglob('*') if recursive else path_covers.glob('*'):
        if should_be_ignored(path):
            continue
        search_path = str(path.relative_to(path_covers))
        logger.debug(f'Search: {search_path}')
        if is_cover_used(search_path, con=con):
            continue
        logger.info(f'Remove: {path}')
        if not dry_run:
            try:
                path.unlink()
            except Exception as e:
                logger.exception(f'삭제 실패: {path}')
                fails.append((path, str(e)))
    print_fails(fails)


def is_updated(series_id: int, start: float) -> bool:
    # 정확하지 않음
    timestmap_format = '%Y-%m-%d %H:%M:%S.%f%z'
    row = execute("SELECT * FROM Series WHERE Id = ?", (series_id,)).fetchone()
    if not row:
        logger.error(f'No series found: {series_id}')
        return True
    last_scanned = datetime.datetime.strptime(f"{row['LastFolderScannedUtc'][:-1]}+0000", timestmap_format).timestamp()
    last_modified = datetime.datetime.strptime(f"{row['LastModifiedUtc'][:-1]}+0000", timestmap_format).timestamp()
    return max(last_modified, last_scanned) > start


async def scan_series_by_query(query: str, params: Sequence[str] | dict[str, str] = (), interval: int | float = 30.0, check: int | float = 5.0) -> None:
    """쿼리문으로 시리즈 스캔
    Args:
        query: 쿼리문
        params: 쿼리문 매개변수
        interval: 각 시리즈 스캔 간격 (초)
        wait: 업데이트 확인 대기 시간 (초)
    Returns:
        None:
    Examples:
        >>> scan_series_by_query('SELECT * FROM Series WHERE CoverImage NOT LIKE ?', ('12345/%',), interval=60)
    """
    targets = tuple(fetch_all(query, params))
    last_index = len(targets) - 1
    for idx, row in enumerate(targets):
        start = datetime.datetime.now(datetime.timezone.utc).timestamp()
        logger.info(f'Scan: {row["Id"]}')
        await scan_series(row['Id'], library_id=row['LibraryId'])
        while not is_updated(row['Id'], start):
            logger.debug(f'Waiting for update: {row["Id"]}')
            await asyncio.sleep(check)
        if idx < last_index:
            await asyncio.sleep(interval)


async def main(*args: Any, **kwds: Any) -> None:
    result = await plugin_authenticate()
    print(result)


if __name__ == '__main__':
    asyncio.run(main())
