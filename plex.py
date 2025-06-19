import re
import json
import time
import shutil
import sqlite3
import asyncio
import pathlib
import logging
import traceback
import unicodedata
import urllib.parse
from typing import Generator, Sequence

from config import plex as config
from helpers import run, http_api, retrieve_db

logger = logging.getLogger(__name__)
http_api = http_api(config.headers)
retrieve_db = retrieve_db(config.db)


def _execute(query: str,
             executable: str = config.sqlite,
             db: str = config.db,
             retry_count: int = config.retry) -> Generator[str, None, None]:
    """
    Plex는 전용 sqlite3를 사용하기 때문에 데이터를 변경시 전용 툴을 사용해야 함
    메모리 오버헤드가 발생할 수 있으므로 대량의 데이터 조회에 적합하지 않음
    """
    for _ in range(retry_count):
        try:
            generator = run((executable, db, query))
            while True:
                yield next(generator)
        except StopIteration as e:
            if e.value:
                break
            time.sleep(5)


def execute(query: str) ->  None:
    for _ in _execute(query):
        break


def execute_batch(queries: Sequence[str], batch_size: int = config.batch_size) -> None:
    for i in range(0, len(queries), batch_size):
        batch = queries[i:i + batch_size]
        try:
            for _ in _execute(';'.join(batch)):
                break
        except StopIteration:
            pass


def execute_json(query: str) -> Generator[dict, None, None]:
    for line in _execute(query):
        try:
            if line:
                yield json.loads(line)
        except GeneratorExit:
            pass
        except:
            logger.error(traceback.format_exc())


@retrieve_db
def fetch_one(query: str, con: sqlite3.Connection = None) -> dict:
    return con.execute(query).fetchone()


@retrieve_db
def fetch_all(query: str, con: sqlite3.Connection = None) -> Generator[dict, None, None]:
    for row in con.execute(query):
        yield row


@retrieve_db
def get_metadata_by_id(metadata_id: int, con: sqlite3.Connection = None) -> dict:
    query = f"SELECT * FROM metadata_items WHERE id = ?"
    return con.execute(query, (metadata_id,)).fetchone()

@retrieve_db
def get_section_by_id(section_id: int, con: sqlite3.Connection = None) -> dict:
    query = f"SELECT * FROM library_sections WHERE id = ?"
    return con.execute(query, (section_id,)).fetchone()


@retrieve_db
def get_media_parts_by_metadata_id(metadata_id: int, con: sqlite3.Connection = None) -> list:
    query = f"SELECT media_parts.id, media_parts.file FROM media_parts, media_items WHERE media_parts.media_item_id = media_items.id AND media_items.metadata_item_id = ?"
    return con.execute(query, (metadata_id,)).fetchall()


@http_api
async def delete_media(meta_id: int, media_id: int, url: str = config.url) -> dict:
    return {
        "url": urllib.parse.urljoin(url, f"/library/metadata/{meta_id}/media/{media_id}"),
        "method": "DELETE",
    }


@http_api
async def empty_trash(section_id: int = -1, url: str = config.url) -> tuple[bool, str]:
    return {
        "url": urllib.parse.urljoin(url, f"/library/sections/{section_id}/emptyTrash"),
        "method": "PUT",
    }


@http_api
async def matches(metadata_id: int,
            title: str,
            year: int = None,
            agent: str = None,
            manual: bool = True,
            language: str = 'ko',
            url: str = config.url) -> dict:
    params = {
        'title': title,
    }
    if year:
        params['year'] = year
    if agent:
        params['agent'] = agent
    if manual:
        params['manual'] = 1
    if language:
        params['language'] = language
    return {
        'url': urllib.parse.urljoin(url, f"/library/metadata/{metadata_id}/matches"),
        'method': 'GET',
        'params': params
    }


@http_api
async def match(metadata_id: int = -1, guid: str = None, name: str = None, year: int = None, url: str = config.url) -> dict:
    params = {
        'guid': guid,
        'name': name,
        'year': year,
    }
    params = {k: v for k, v in params.items() if v}
    return {
        'url': urllib.parse.urljoin(url, f"/library/metadata/{metadata_id}/match"),
        'method': 'PUT',
        'params': params,
    }


@http_api
async def refresh(metadata_id: int, url: str = config.url) -> dict:
    return {
        'url': urllib.parse.urljoin(url, f"/library/metadata/{metadata_id}/refresh"),
        'method': 'PUT',
    }


@http_api
async def analyze(metadata_id: int, url: str = config.url) -> dict:
    return {
        'url': urllib.parse.urljoin(url, f"/library/metadata/{metadata_id}/analyze"),
        'method': 'PUT',
    }


@http_api
async def scan(section_id: int, path: str = None, cancel: bool = False, url: str = config.url) -> dict:
    params = {'path': path} if path else None
    return {
        'url': urllib.parse.urljoin(url, f"/library/sections/{section_id}/refresh"),
        'method': 'GET' if not cancel else 'DELETE',
        'params': params,
    }


async def is_updated(metadata_id: int, start: float) -> bool:
    row = get_metadata_by_id(metadata_id)
    if not row:
        # 새로운 혹은 다른 메타데이터로 변경되는 경우
        logger.warning(f"삭제 되었어요: {metadata_id}")
        return True
    else:
        timestamp_keys = ('originally_available_at', 'available_at', 'refreshed_at', 'added_at', 'updated_at', 'created_at', 'deleted_at')
        timestamp_values = tuple(map(lambda x: ok if (ok := row.get(x)) else 0, timestamp_keys))
        max_timestamp_value = max(timestamp_values)
        msg = f"{metadata_id} - {row['title']} ({row['year']})"
        if max_timestamp_value >= start:
            #max_timestamp_key = timestamp_keys[timestamp_values.index(max_timestamp_value)]
            #logger.debug(f"{max_timestamp_key=} value={max_timestamp_value=}")
            logger.info(f"업데이트 완료: {msg}")
            return True
        else:
            # 업데이트할 필요가 없어서 timestamp가 변경되지 않을 경우 계속 대기해야 함
            for act in fetch_all(f'SELECT * FROM activities WHERE finished_at > {int(start)}'):
                if row['title'] in act['subtitle']:
                    logger.info(f"활동이 완료됨: {act}")
                    return True
            logger.info(f"업데이트 중: {msg}")
            return False


async def check_update(metadata_id: int,
                       result: dict,
                       start: float,
                       check_count: int = config.check_count,
                       check_interval: int = config.check_interval) -> bool:
    if 300 > result.get('status_code') or 0 > 199:
        for i in range(check_count):
            if await is_updated(metadata_id, start):
                return True
            else:
                await asyncio.sleep(check_interval)
        logger.warning(f"대기 시간 초과: {metadata_id}")
    else:
        logger.warning(f"업데이트를 할 수 없어요: {result['status_code']} {result['url']} {result['text']}")
    return False


async def rematch(metadata_id: int = -1, guid: str = None, name: str = None, year: int = None) -> None:
    start = int(time.time())
    result = await match(metadata_id, guid, name, year)
    await check_update(metadata_id, result, start)


def get_extra_data_url(extra_data: dict) -> str:
    new_url = ""
    for idx, key in enumerate(extra_data, start=1):
        if key == "url":
            continue
        if idx > 1:
            new_url += "&"
        new_url += f"{urllib.parse.quote(key)}={urllib.parse.quote(extra_data[key], safe='').replace('.', '%2E')}"
    return new_url


@retrieve_db
async def delete_not_exists(section_id: int, mount_anchor: str, /, dry_run: bool = config.dry_run, print_exists: bool = False, con: sqlite3.Connection = None) -> None:
    """파일이 삭제되었지만 휴지통 비우기로 처리되지 않는 미디어를 DB에서 삭제
    Args:
        section_id: 섹션 아이디. 모든 섹션을 지정하려면 section_id를 -1로 지정
        mount_anchor: 폴더 안에 있는 파일을 삭제할 경로. 마운트 오류로 삭제되는 걸 방지하기 위해 mount_anchor로 지정한 경로가 존재할 때만 삭제
        dry_run: 실제 실행 여부. 기본값: ``config.yaml``에 정의된 dry_run
        print_exists: 존재하는 파일을 디버그 로그에 출력할 지 여부. 기본값: False
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:

    Examples:
        전체 섹션 탐색

        >>> delete_not_exists(-1, '/mnt/gds/GDRIVE/VIDEO/방송중', dry_run=True, print_exists=True)

    """
    query = """SELECT media_parts.id AS part_id, media_items.id AS media_id, metadata_items.id AS meta_id, media_parts.file
    FROM media_parts, media_items, metadata_items
    WHERE media_items.id = media_parts.media_item_id
    AND metadata_items.id = media_items.metadata_item_id"""
    if int(section_id) > 0:
        query += f" AND metadata_items.library_section_id = {section_id}"
    anchor = pathlib.Path(mount_anchor)
    cursor: sqlite3.Cursor = con.execute(query)
    idx = 0
    for row in cursor:
        idx += 1
        file = row.get('file') or ''
        if not file:
            continue
        path = pathlib.Path(file)
        if path.exists():
            if print_exists:
                logger.debug(f"{idx}. {row['meta_id']}: {str(path)}")
            continue
        logger.info(f"{row['meta_id']}: NOT EXISTS: {str(path)}")
        if not anchor.exists():
            logger.debug(f"SKIP: {anchor=} is not exists")
            continue
        if not dry_run:
            logger.info(f"{idx}. {row['meta_id']}: DELETE: meta={row.get('meta_id') or -1} media={row.get('media_id') or -1}")
            result = await delete_media(row.get('meta_id') or -1, row.get('media_id') or -1)
            if not 300 > result.get('status_code') > 199:
                logger.warning(f"{idx}. {row['meta_id']}: COULD NOT DELETE: status_code={result.get('status_code')}")


@retrieve_db
def update_title_sort(section_id: int, dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """라이브러리 색인 목록의 음절을 자음으로 수정
    Args:
        section_id: 섹션 아이디. 모든 섹션을 지정하려면 section_id를 -1로 지정
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:
    Examples:
        전체 섹션 탐색
        >>> update_title_sort(-1, dry_run=True)
    """
    query = "SELECT id, title, title_sort, metadata_type FROM metadata_items"
    if int(section_id) > 0:
        query += f" WHERE library_section_id = {section_id}"
    cursor: sqlite3.Cursor = con.execute(query)
    execute_queries = []
    for row in cursor:
        if not row.get('title'):
            continue
        if row.get('title_sort'):
            first_char = row['title_sort'][0]
        else:
            first_char = row['title'][0]
        if first_char.isalnum() and not 44032 <= ord(first_char) <= 55203:
            continue
        new_title_sort = "".join([word for word in re.split(r'\W', row['title']) if word])

        if not new_title_sort:
            logger.warning(f"색인용 문자가 없습니다: '{row['title']}'")
            new_title_sort = row['title']
        new_title_sort = unicodedata.normalize('NFKD', new_title_sort)
        logger.debug(f"{row['id']}: [{new_title_sort[0]}][{row['title_sort'][0] if row['title_sort'] else ''}]{row['title']}")
        if new_title_sort != row['title_sort']:
            execute_queries.append("UPDATE metadata_items SET title_sort = '" + new_title_sort.replace("'", "''") + f"' WHERE id = {row['id']}")
    if not dry_run and execute_queries:
        execute_batch(execute_queries)


@retrieve_db
def update_review_source(new_text: str = 'Unknown', dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """메타데이터와 연결된 리뷰의 source 데이터를 수정
    Args:
        new_text: 대체할 문자
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:
    Examples:
        >>> update_review_source(dry_run=True)
        >>> update_review_source(dry_run=False, new_text='blank')
    """
    query = f"SELECT taggings.id, taggings.extra_data, taggings.metadata_item_id FROM taggings, tags WHERE tags.tag_type = 10 AND taggings.tag_id = tags.id AND taggings.extra_data LIKE ?;"
    cursor: sqlite3.Cursor = con.execute(query, ('%"at:source":""%',))
    execute_queries = []
    for row in cursor:
        try:
            extra_data: dict = json.loads(row.get('extra_data', '{}'))
        except:
            logger.error(traceback.format_exc())
            continue
        if not extra_data.get('at:source') == '':
            continue
        logger.debug(f"┌metadata_id={row['metadata_item_id']} before={extra_data.copy()}")
        extra_data['at:source'] = new_text
        extra_data['url'] = get_extra_data_url(extra_data)
        logger.debug(f"└metadata_id={row['metadata_item_id']}  after={extra_data}")
        if row['extra_data'] != (extra_data_json := json.dumps(extra_data)):
            execute_queries.append("UPDATE taggings SET extra_data = '" + extra_data_json.replace("'", "''") + f"' WHERE id={row.get('id')}")
    if not dry_run and execute_queries:
        execute_batch(execute_queries)


@retrieve_db
def update_clip_key(search: str, replace: str, dry_run: bool = config.dry_run, con: sqlite3.Connection = None) -> None:
    """부가 영상의 url을 수정
    Args:
        search: 찾을 내용
        replace: 바꿀 내용
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력

    Returns:
        None:
    Examples:
        >>> update_clip_key('http://localhost:9999', 'https://my.ff.dns.org', dry_run=True)
    """
    query = f"SELECT id, media_parts.extra_data FROM media_parts WHERE media_parts.extra_data LIKE ?"
    cursor: sqlite3.Cursor = con.execute(query, (f"%{search}%",))
    execute_queries = []
    for row in cursor:
        try:
            extra_data = json.loads(row.get('extra_data') or '{}')
        except:
            logger.error(traceback.format_exc())
            continue
        if not extra_data.get('at:key'):
            continue
        logger.debug(f"┌{extra_data.copy()}")
        extra_data['at:key'] = extra_data.get('at:key').replace(search, replace)
        extra_data['url'] = get_extra_data_url(extra_data)
        logger.debug(f"└{extra_data}")
        if row['extra_data'] != (extra_data_json := json.dumps(extra_data)):
            execute_queries.append("UPDATE media_parts SET extra_data = '" + extra_data_json.replace("'", "''") + f"' WHERE id={row.get('id')}")
    if not dry_run and execute_queries:
        execute_batch(execute_queries)


def get_bundle_path(hash: str, metadata_type: str, metadata_path: str = config.metadata) -> pathlib.Path:
    if metadata_type == 1:
        content_type = 'Movies'
    elif metadata_type in (2, 3, 4):
        content_type = 'TV Shows'
    else:
        return None
    return pathlib.Path(metadata_path) / content_type / hash[0] / f"{hash[1:]}.bundle"


def get_ancestors(row: dict, con: sqlite3.Connection) -> pathlib.Path:
    parent_row = None
    grand_parent_row = None
    if row['metadata_type'] in (3, 4):
        parent_row = con.execute(
            f"SELECT * FROM metadata_items WHERE id = ?",
            (row['parent_id'],)
        ).fetchone()
        if row['metadata_type'] == 4:
            grand_parent_row = con.execute(
                f"SELECT * FROM metadata_items WHERE id = ?",
                (parent_row['parent_id'],)
            ).fetchone()
        hash = grand_parent_row['hash'] if row['metadata_type'] == 4 else parent_row['hash']
    else:
        hash = row['hash']
    return hash, parent_row, grand_parent_row


async def delete_bundle(metadata_id: int, bundle: str | pathlib.Path, shoud_refresh: bool = True, dry_run: bool = config.dry_run) -> None:
    """메타데이터 번들 폴더를 삭제
    Args:
        metadata_id: 메타데이터 아이디
        refrsh: 메타데이터 새로고침 여부
        dry_run: 실제 실행 여부
    Returns:
        None:
    Examples:
        >>> delete_bundle(metadata_id=1, refresh=False, dry_run=True)
    """
    path_bundle = pathlib.Path(bundle)
    logger.info(f'번들 삭제: {metadata_id} ({path_bundle})')
    if not dry_run:
        shutil.rmtree(path_bundle)
        if shoud_refresh:
            logger.info(f'새로고침: {metadata_id}')
            start = int(time.time())
            result = await refresh(metadata_id)
            await check_update(metadata_id, result, start)


async def clean_bundle(metadata_id: int, dry_run: bool = config.dry_run) -> None:
    """메타데이터 번들 폴더를 삭제 후 새로고침
    Args:
        metadata_id: 메타데이터 아이디
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력
    Returns:
        None:
    Examples:
        >>> prune_metadata(metadata_id=1, dry_run=True)
    """
    metadata = fetch_one("SELECT * FROM metadata_items WHERE id = ?", (metadata_id,))
    if not metadata:
        logger.warning(f"존재하지 않는 메타데이터: {metadata_id}")
        return
    if not metadata['metadata_type'] in (1, 2):
        logger.warning(f"영화와 TV 쇼의 메타데이터만 처리 가능합니다: {metadata_id}")
        return
    path_bundle = get_bundle_path(metadata['hash'], metadata['metadata_type'])
    delete_bundle(metadata_id, path_bundle, dry_run=dry_run)


def find_none_file(root: str | pathlib.Path) -> Generator[pathlib.Path, None, None]:
    for file in pathlib.Path(root).rglob('*'):
        if file.is_dir():
            continue
        try:
            with file.open('rb') as f:
                if f.read(4) == b'None':
                    yield file
        except Exception:
            logger.exception(file)


async def find_and_clean_bundle(library_id: int, dry_run: bool = config.dry_run) -> None:
    """메타데이터 번들 폴더에 가짜 파일(None)이 있을 경우 번들 삭제 후 새로고침
    Args:
        library_id: 라이브러리 아이디
        dry_run: 실제 실행 여부
        con: sqlite3 커넥션. 데코레이터에 의해 자동 입력
    Returns:
        None:
    Examples:
        >>> find_and_clean_none_file(library_id=1, dry_run=False)
    """
    for row in fetch_all(f"SELECT * FROM metadata_items WHERE library_section_id = {library_id}"):
        if not row['metadata_type'] in (1, 2):
            continue
        path_bundle = get_bundle_path(row['hash'], row['metadata_type'])
        none_files = tuple(find_none_file(path_bundle))
        if none_files:
            logger.info(f'{row["id"]}: {row["title"]} "{path_bundle}"')
            for idx, none_file in enumerate(none_files):
                prefix = '└' if idx >= len(none_files) - 1 else '│'
                logger.debug(f'{prefix} {none_file.relative_to(path_bundle)}')
            await delete_bundle(row['id'], path_bundle, dry_run=dry_run)
