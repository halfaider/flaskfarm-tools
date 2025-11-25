import time
import asyncio
import logging
import sqlite3
import pathlib
import datetime
import urllib.parse
import shutil
from typing import Any, Generator, Sequence, Callable

from config import kavita as config
from helpers import http_api, retrieve_db, string_bool

logger = logging.getLogger(__name__)
retrieve_db = retrieve_db(config.db)

kavita_token = None

async def get_headers(require_token: bool = True, url: str = config.url, apikey: str = config.apikey) -> dict | None:
    global kavita_token
    headers = {
        "Content-Type": "application/json"
    }
    if require_token and kavita_token is None:
        result = await plugin_authenticate(url=url, apikey=apikey)
        if not 300 > (result.get('status_code') or 0) > 199:
            logger.error(f'인증 실패: {result}')
        else:
            kavita_token = result.get('json').get('token')
    headers['Authorization'] = f"Bearer {kavita_token}"
    return headers


@http_api(config.headers)
async def plugin_authenticate(url: str = config.url, apikey: str = config.apikey, plugin_name: str = config.plugin_name) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Plugin/authenticate'),
        'params': {'pluginName': plugin_name, 'apiKey': apikey},
        'method': 'POST',
    }


@http_api(config.headers)
async def scan_folder(folder: str, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-folder'),
        'json': {'folderPath': folder, 'apiKey': apikey},
        'method': 'POST',
    }


@http_api(config.headers)
async def scan(library_id: int | str, force: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan'),
        'params': {'libraryId': library_id, 'force': string_bool(force)},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api(config.headers)
async def scan_all(force: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-all'),
        'params': {'force': string_bool(force)},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api(config.headers)
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


@http_api(config.headers)
async def scan_multiple(library_ids: Sequence[int | str], force: bool = False, url: str = config.url, apikey: str = config.apikey) -> dict:
    # 작동 안 하는 듯
    return {
        'url': urllib.parse.urljoin(url, '/api/Library/scan-multiple'),
        'json': {'ids': library_ids, 'force': force},
        'method': 'POST',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api(config.headers)
async def jobs(url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Server/jobs'),
        'method': 'GET',
        'headers': await get_headers(url=url, apikey=apikey)
    }


@http_api(config.headers, 5)
async def series_cover(series_id: int, method: str = 'GET', url: str = config.url, apikey: str = config.apikey, read_body: bool = False) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/image/series-cover'),
        'method': method,
        'params': {'seriesId': series_id, 'apiKey': apikey},
        'read_body': read_body,
    }


@http_api(config.headers, 5)
async def volume_cover(volume_id: int, method: str = 'GET', url: str = config.url, apikey: str = config.apikey, read_body: bool = False) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/image/volume-cover'),
        'method': method,
        'params': {'volumeId': volume_id, 'apiKey': apikey},
        'read_body': read_body,
    }


@http_api(config.headers)
async def series_refresh_metadata(library_id: int, series_id: int, force: bool = False, color_scape: bool = False, method: str = 'POST', url: str = config.url, apikey: str = config.apikey) -> dict:
    return {
        'url': urllib.parse.urljoin(url, '/api/Series/refresh-metadata'),
        'method': method,
        'headers': await get_headers(url=url, apikey=apikey),
        'json': {
            'libraryId': library_id,
            'seriesId': series_id,
            'forceUpdate': force,
            'forceColorscape': color_scape,
        },
    }


def fetch_one(query: str, params: Sequence[str] | dict[str, str] = ()) -> dict:
    return execute(query, params).fetchone()


def fetch_all(query: str, params: Sequence[str] | dict[str, str] = ()) -> Generator[dict, None, None]:
    for row in execute(query, params):
        yield row


@retrieve_db
def execute(query: str, params: Sequence[str] | dict[str, str] = (), retry_count: int = config.retry, con: sqlite3.Connection = None) -> sqlite3.Cursor:
    for idx in range(retry_count):
        try:
           return con.execute(query, params)
        except Exception as e:
            logger.exception(f'Retry ({idx})')
            time.sleep(5)
    raise Exception(f'Max retry count exceeded: {query}')


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
        "SELECT id as LibraryId FROM Library WHERE CoverImage LIKE ?",
        # 2. Series
        "SELECT LibraryId FROM Series WHERE CoverImage LIKE ?",
        # 3. Volume
        """
        SELECT s.LibraryId
        FROM Volume AS v
        JOIN Series AS s ON v.SeriesId = s.id
        WHERE v.CoverImage LIKE ?
        """,
        # 4. Chapter
        """
        SELECT s.LibraryId
        FROM Chapter AS c
        JOIN Volume AS v ON c.VolumeId = v.id
        JOIN Series AS s ON v.SeriesId = s.id
        WHERE c.CoverImage LIKE ?
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
def organize_covers(covers: str = '/kavita/config/covers', quantity: int = -1, sub_path: str = None, dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """커버 이미지를 각 라이브러리 폴더로 이동. 하위 폴더는 검색하지 않음. 데이터베이스에서 커버 이미지로 라이브러리 ID를 검색한 후 그 ID로 폴더를 생성하여 이동.
    Args:
        covers: 커버 폴더 경로
        quantity: 옮길 파일 갯수를 정해서 부분 실행. 모든 파일: -1
        sub_path: 이동할 하위 폴더 이름. covers 폴더 아래에 생성
        dry_run: 실제 실행 여부. config.yaml 설정을 기본값으로 사용
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:

    Examples:
        >>> organize_covers('/kavita/config/covers', quantity=100, sub_path='google', dry_run=True)
    """
    path_covers = pathlib.Path(covers)
    fails = []
    library_ids = set()
    counter = 0
    for path in path_covers.glob('*'):
        if should_be_ignored(path):
            # 디렉토리, 공통으로 사용하는 커버는 제외
            continue
        library_id = get_library_by_cover(f'%{path.name}', con=con)
        if library_id < 1:
            # 라이브러리를 알 수 없는 커버는 제외
            continue
        if quantity >= 0 and quantity <= counter:
            break
        counter += 1
        library_ids.add(library_id)
        if sub_path:
            new_path = path_covers / sub_path / f'{library_id}' / path.name
        else:
            new_path = path_covers / f'{library_id}' / path.name
        try:
            if not dry_run and not new_path.parent.exists():
                logger.debug(f'Create: {new_path.parent}')
                new_path.parent.mkdir(parents=True)
            logger.info(f'{path} -> {new_path}')
            if not dry_run:
                try:
                    path.rename(new_path)
                except OSError as e:
                    if e.errno == 18:
                        #logger.warning(f"Cross-device link detected, falling back to shutil.move: {e}")
                        shutil.move(path, new_path)
                    else:
                        raise
            new_value = str(new_path.relative_to(path_covers))
            logger.info(f'Update CoverImage with {new_value}')
            if not dry_run:
                for table in config.tables_with_cover:
                    con.execute(f'UPDATE {table} SET CoverImage = ? WHERE CoverImage = ?', (new_value, path.name))
        except KeyboardInterrupt as e:
            logger.exception(f'사용자 중단: {path}')
            break
        except Exception as e:
            logger.exception(f'커버 정리 실패: {path.name}')
            fails.append((path, str(e)))
    print_fails(fails)


@retrieve_db
def fix_organized_covers(library_ids: Sequence[int] | str = (), covers: str = '/kavita/config/covers', sub_path: str = None, cover_image_like: str = '%.png', dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """커버 파일은 이동 되었는데 DB 업데이트가 안 됐을 경우 실행
    cover_image_like에 SQL LIKE 패턴을 지정하여 해당 되는 레코드만 업데이트

    Args:
        library_ids: 라이브러리 ID 리스트
        covers: 커버 폴더 경로
        sub_path: 커버 폴더 내 하위 폴더 이름
        cover_image_like: DB에 저장된 커버 파일 이름의 패턴(SQL LIKE)
        dry_run: 실제 실행 여부. config.yaml의 값을 기본값으로 사용
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:

    Examples:
        >>> fix_organized_covers([101, 102, 103], covers='/mnt/kavita/covers', sub_path='google', dry_run=True)
    """
    query_select = "SELECT Id, CoverImage FROM {table} WHERE CoverImage LIKE ?"
    query_update = "UPDATE {table} SET CoverImage = ? WHERE Id = ?"
    path_covers = pathlib.Path(covers)

    def update_cover(table: str, library_id: int, row: dict) -> None:
        try:
            if not row:
                return
            cover_image = row.get('CoverImage') or ''
            path_image = pathlib.Path(cover_image)
            new_path = path_covers
            if sub_path:
                new_path = new_path / sub_path
            new_path = new_path / str(library_id) / path_image.name
            if should_be_ignored(new_path):
                return
            relative_path = new_path.relative_to(path_covers)
            logger.info(f'Update: {cover_image} -> {relative_path}')
            if not dry_run:
                con.execute(query_update.format(table=table), (str(relative_path), row['Id']))
        except Exception as e:
            logger.exception(f'{table}: {row["Id"]}')

    for library_id in library_ids:
        lib_row = con.execute(f"{query_select.format(table='Library')} AND Id = ?", (cover_image_like, library_id)).fetchone()
        update_cover('Library', library_id, lib_row)
        for row in con.execute(f"{query_select.format(table='Series')} AND LibraryId = ?", (cover_image_like, library_id)).fetchall():
            update_cover('Series', library_id, row)
            for vol_row in con.execute(f"{query_select.format(table='Volume')} AND SeriesId = ?", (cover_image_like, row['Id'])).fetchall():
                update_cover('Volume', library_id, vol_row)
                for ch_row in con.execute(f"{query_select.format(table='Chapter')} AND VolumeId = ?", (cover_image_like, vol_row['Id'])).fetchall():
                    update_cover('Chapter', library_id, ch_row)


@retrieve_db
def clean_covers(covers: str = '/kavita/config/covers', subs: Sequence[str] = (), recursive: bool = True, dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """데이터베이스에서 커버 이미지를 사용중인 레코드가 없으면 삭제
    Args:
        covers: 커버 폴더 경로
        sub: covers의 하위 폴더 이름. 특정 폴더만 정리하고 싶을 경우 지정
        recursive: 하위 폴더 탐색 여부
        dry_run: 실제 실행 여부. config.yaml의 값을 기본값으로 사용
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:

    Examples:
        >>> clean_covers('/docker/volumes/kavita/config/covers', subs=['sub_path/101'], recursive=False, dry_run=True)
    """
    root_path = pathlib.Path(covers)
    target_paths = []
    for sub in subs:
        sub_path = root_path / sub
        if sub_path.exists():
            target_paths.append(sub_path)
        else:
            logger.error(f'존재하지 않는 폴더: {sub_path}')
            return

    if not target_paths:
        target_paths.append(root_path)

    fails = []
    for target_path in target_paths:
        for path in target_path.rglob('*') if recursive else target_path.glob('*'):
            if should_be_ignored(path):
                continue
            search_path = str(path.relative_to(root_path))
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


def is_series_updated(series_id: int, start: float) -> bool:
    # 정확하지 않음
    timestmap_format = '%Y-%m-%d %H:%M:%S.%f%z'
    row = execute("SELECT * FROM Series WHERE Id = ?", (series_id,)).fetchone()
    if not row:
        logger.error(f'No series found: {series_id}')
        return True
    last_scanned = datetime.datetime.strptime(f"{row['LastFolderScannedUtc'][:-1]}+0000", timestmap_format).timestamp()
    last_modified = datetime.datetime.strptime(f"{row['LastModifiedUtc'][:-1]}+0000", timestmap_format).timestamp()
    return max(last_modified, last_scanned) > start


async def scan_series_by_query(query: str, params: Sequence[str] | dict[str, str] = (), interval: int | float = 30.0, check: int | float = 5.0, force: bool = False) -> None:
    """쿼리문으로 시리즈 스캔
    Args:
        query: 쿼리문
        params: 쿼리문 매개변수
        interval: 각 시리즈 스캔 간격 (초)
        check: 업데이트 확인 대기 시간 (초)
        force: 강제 업데이트 여부
    Returns:
        None:
    Examples:
        >>> scan_series_by_query('SELECT * FROM Series WHERE CoverImage NOT LIKE ?', ('12345/%',), interval=60, check=6, force=True)
    """
    targets = tuple(fetch_all(query, params))
    last_index = len(targets) - 1
    for idx, row in enumerate(targets):
        start = datetime.datetime.now(datetime.timezone.utc).timestamp()
        logger.info(f'Scan: {row["Id"]}')
        result = await scan_series(row['Id'], library_id=row['LibraryId'], force=force)
        while not is_series_updated(row['Id'], start):
            logger.debug(f'Waiting for update: {row["Id"]}')
            await asyncio.sleep(check)
        if idx < last_index:
            await asyncio.sleep(interval)


async def refresh_series(library_id: int, series_id: int, method: str = 'POST', force: bool = False, color_scape: bool = False, url: str = config.url, apikey: str = config.apikey, check: int | float = 5.0,) -> None:
    start = datetime.datetime.now(datetime.timezone.utc).timestamp()
    logger.info(f'시리즈 새로고침: {series_id}')
    result = await series_refresh_metadata(library_id, series_id, method=method, force=force, color_scape=color_scape, url=url, apikey=apikey)
    if 300 > result.get('status_code') > 199:
        while not is_series_updated(series_id, start):
            logger.debug(f'시리즈 새로고침 대기중: {series_id}')
            await asyncio.sleep(check)
    else:
        logger.error(f'시리즈 새로고침 실패: {series_id=} status_code={result.get("status_code")}')


async def series_scan_worker(scan_queue: asyncio.Queue, interval: int = 60, check: int = 5, dry_run: bool = config.dry_run) -> None:
    scanning = set()
    scanned = set()

    while True:
        library_id, series_id = await scan_queue.get()
        logger.debug(f"남은 새로고침 수: {scan_queue.qsize()}")
        if series_id in scanning or series_id in scanned:
            scan_queue.task_done()
            continue
        scanning.add(series_id)
        try:
            if not dry_run:
                #start = datetime.datetime.now(datetime.timezone.utc).timestamp()
                logger.info(f'Scan: {series_id}')
                result = await scan_series(series_id, library_id=library_id, force=True)
                if not 300 > result.get('status_code') > 199:
                    logger.error(f'시리즈 스캔 실패: {series_id=} status_code={result.get("status_code")}')
                # 그냥 기다리지 말고 카비타가 알아서 처리하도록...
                #while not is_series_updated(series_id, start):
                #    logger.debug(f'Waiting for update: {series_id}')
                #    await asyncio.sleep(check)
                #logger.debug(f"Sereis updated: {series_id}")
                ## 다음 스캔이 바로 시작되면 10분 딜레이 될 수 있으므로 여유를 주고 시작되도록...
                #await asyncio.sleep(interval)
        finally:
            scanning.remove(series_id)
            scan_queue.task_done()


async def check_cover_image(row: sqlite3.Row, scan_queue: asyncio.Queue, url: str = config.url, apikey: str = config.apikey) -> None:
    cover_image = row.get('CoverImage')
    library_id = row['LibraryId']
    series_id = row['Id']
    is_normal = True

    if cover_image:
        result = await series_cover(series_id, read_body=False, url=url, apikey=apikey)
        is_normal = True if 300 > result.get('status_code') > 199 else False
    else:
        is_normal = False

    if is_normal:
        for vol_row in fetch_all(f'SELECT Id, CoverImage, SeriesId FROM Volume WHERE SeriesId = ?', (series_id,)):
            vol_cover_image = vol_row.get('CoverImage')
            volume_id = vol_row['Id']

            if vol_cover_image:
                result = await volume_cover(volume_id, read_body=False, url=url, apikey=apikey)
                is_normal = True if 300 > result.get('status_code') > 199 else False
            else:
                is_normal = False

    if not is_normal:
        logger.info(f"비정상 커버: {url}/library/{library_id}/series/{series_id}")
        await scan_queue.put((library_id, series_id))


async def scan_no_cover(library_id: int | None = None, semaphore: int = 10, dry_run: bool = config.dry_run, url: str = config.url, apikey: str = config.apikey) -> None:
    """시리즈 및 볼륨의 커버 이미지가 비정상인 경우 해당 시리즈를 refresh 시도

    Args:
        library_id: 라이브러리 ID. 지정하지 않으면 전체 라이브러리
        covers: 커버 폴더 경로
        semaphore: 커버 이미지 검증 작업을 동시에 실행할 개수
        dry_run: 실제 실행 여부. config.yaml의 값을 기본값으로 사용
        url: 링크 표시용 카비타 URL. conifg.yaml의 값을 기본값으로 사용
        apikey: 카비타 API 키. config.yaml의 값을 기본값으로 사용
    Returns:
        None:
    Examples:
        >>> scan_no_cover(101, covers='/kavita/config/covers', semaphore=5, url='http://kavita:5000', apikey='abcdefg')
    """
    count_query = f'SELECT COUNT(*) AS count FROM Series'
    series_query = f'SELECT Id, CoverImage, LibraryId FROM Series'
    if library_id:
        count_query += f' WHERE LibraryId = ?'
        series_query += f' WHERE LibraryId = ?'
    count = fetch_one(count_query, (library_id,) if library_id else ())
    total = int(count['count'])
    series = fetch_all(series_query, (library_id,)if library_id else ())
    scan_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(semaphore)

    done = 0
    lock = asyncio.Lock()

    async def wrapped_check(row):
        nonlocal done
        async with semaphore:
            await check_cover_image(row, scan_queue, url=url, apikey=apikey)
            async with lock:
                done += 1
                #if done <= 100 or total - done <= 100 or done % 100 == 0:
                #    logger.info(f"{done}/{total} checks completed ({done/total*100:.1f}%)")
                logger.debug(f"{done}/{total} 확인 완료 ({done/total*100:.1f}%)")

    scan_task = asyncio.create_task(series_scan_worker(scan_queue, interval=60, check=5, dry_run=dry_run))
    check_tasks = [wrapped_check(row) for row in series]
    await asyncio.gather(*check_tasks)
    await scan_queue.join()
    scan_task.cancel()


async def main(*args: Any, **kwds: Any) -> None:
    result = await plugin_authenticate()
    print(result)


@retrieve_db
def undo_organized_covers(library_ids: Sequence[int] | str = (), covers: str = '/kavita/config/covers', con: sqlite3.Connection = None) -> None:
    path_covers = pathlib.Path(covers)
    for lib_id in library_ids:
        path_lib = path_covers / str(lib_id)
        if not path_lib.exists():
            continue
        for path in path_lib.glob('*'):
            new_path = path_covers / path.name
            path.rename(new_path)
            logger.info(new_path)

    query_select = "SELECT Id, CoverImage FROM {table} WHERE CoverImage LIKE ?"
    query_update = "UPDATE {table} SET CoverImage = ? WHERE Id = ?"
    finding = '%/%'
    def update_cover(table: str, row: dict) -> None:
        try:
            con.execute(query_update.format(table=table), (f"{row['CoverImage'].split('/')[1]}", row['Id']))
        except Exception as e:
            logger.exception(f'{table}: {row["Id"]}')
    for lib_id in library_ids:
        lib_row = con.execute(f"{query_select.format(table='Library')} AND Id = ?", (finding, lib_id)).fetchone()
        update_cover('Library', lib_row)
        for row in con.execute(f"{query_select.format(table='Series')} AND LibraryId = ?", (finding, lib_id)).fetchall():
            update_cover('Series', row)
            for vol_row in con.execute(f"{query_select.format(table='Volume')} AND SeriesId = ?", (finding, row['Id'])).fetchall():
                update_cover('Volume', vol_row)
                for ch_row in con.execute(f"{query_select.format(table='Chapter')} AND VolumeId = ?", (finding, vol_row['Id'])).fetchall():
                    update_cover('Chapter', ch_row)


if __name__ == '__main__':
    asyncio.run(main())
