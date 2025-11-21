import sys
import time
import sqlite3
import pathlib
import asyncio
import logging
import traceback
import xml.etree.ElementTree as ET
from typing import Any, Iterable

import plex
from helpers import queue_task, countdown
from config import plex as config

logger = logging.getLogger(__name__)


async def phase_1(con: sqlite3.Connection,
                  query: str,
                  dry_run: bool = config.dry_run,
                  start_count: int = config.countdown,
                  columns: Iterable[str] = config.metadata_url_columns) -> None:
    # 1차 시도: 새로운 Plex 기본 에이전트는 Info.xml을 사용하지 않고 DB에 포스터 url을 저장함
    execute_quries = []
    for idx, row in enumerate(con.execute(query)):
        logger.debug(f'{idx}. 1차 분석중: id={row["id"]} title="{row["title"]}"')

        for column in columns:
            tagging_row = con.execute(
                f"""SELECT id, text
                FROM taggings
                WHERE thumb_url = ? AND metadata_item_id = ?""",
                (row[column], row['id'])
            ).fetchone()
            # http url이면 업데이트
            if tagging_row and (text := tagging_row.get('text')) and text.startswith('http') and text != row[column]:
                execute_quries.append(f"UPDATE metadata_items SET {column} = '" + text.replace("'", "''") + f"' WHERE id = {row['id']}")
    logger.info(f'Update: 미디어 URL을 알고 있는 메타데이터 개수: {len(execute_quries)}')
    countdown(start_count)
    if not dry_run and execute_quries:
        plex.execute_batch(execute_quries)


async def phase_2(con: sqlite3.Connection, query: str, dry_run: bool = config.dry_run, start_count: int = config.countdown) -> None:
    to_be_updated = dict()
    # 2차 시도: Info.xml에서 URL이 있으면 업데이트
    for idx, row in enumerate(con.execute(query)):
        if not row['metadata_type'] in (1, 2, 3, 4):
            # 영화, TV 쇼, TV 시즌, TV 에피소드가 아니면 건너 뛰기
            continue

        logger.debug(f'{idx}. 2차 분석중: id={row["id"]} title="{row["title"]}"')

        hash, parent_row, grand_parent_row = plex.get_ancestors(row, con)
        if row['metadata_type'] == 3 and not parent_row:
            logger.warning(f'시즌의 부모가 없음: {row}')
            continue
        if row['metadata_type'] == 4 and not grand_parent_row:
            logger.warning(f'에피소드의 조부모가 없음: {row}')
            continue

        path_contents = plex.get_bundle_path(hash, row['metadata_type']) + '/Contents'
        path_info = path_contents / '_combined' / 'Info.xml'

        if not path_info.exists():
            continue

        # Info.xml 파일에서 미디어 url을 찾아 보기
        try:
            tree_info = ET.parse(path_info)
        except:
            logger.error(traceback.format_exc())
            continue

        info_media = {
            'posters': {
                'column': 'user_thumb_url',
                'candidates': tuple(),
                'xpath_urls': './/posters/item[@url]'
            },
            'art': {
                'column': 'user_art_url',
                'candidates': tuple(),
                'xpath_urls': './/art/item[@url]'
            },
            'banners': {
                'column': 'user_banner_url',
                'candidates': tuple(),
                'xpath_urls': './/banners/item[@url]'
            },
            'themes': {
                'column': 'user_music_url',
                'candidates': tuple(),
                'xpath_urls': './/themes/item[@url]'
            },
        }
        for media in info_media:
            if media == 'themes' and row['metadata_type'] in (3, 4):
                continue
            if row['metadata_type'] == 4 and media in ('art', 'banners', 'themes'):
                continue
            if filename := row[info_media[media]['column']]:
                scheme, _, name = filename.partition('://')
                if scheme.startswith('http'):
                    info_media[media]['filename'] = None
                else:
                    info_media[media]['filename'] = name.split('/')[-1]
            if filename := info_media[media].get('filename'):
                if row['metadata_type'] == 4:
                    info_media[media]['candidates'] = (f'.//thumbs/item[@preview="{filename}"]',)
                else:
                    info_media[media]['candidates'] = (f'.//{media}/item[@media="{filename}"]', f'.//posters/item[@preview="{filename}"]')
        if row['metadata_type'] == 4:
            info_media['posters']['xpath_urls'] = './/thumbs/item[@url]'

        if row['metadata_type'] in (1, 2):
            _tree = tree_info
        if row['metadata_type'] in (3, 4):
            season_num = parent_row['index'] if row['metadata_type'] == 4 else row['index']
            if row['metadata_type'] == 4:
                episode_num = row['index']
                path_xml = path_contents / '_combined' / 'seasons' / str(season_num) / 'episodes' / f"{episode_num}.xml"
            else:
                path_xml = path_contents / '_combined' / 'seasons' / f"{season_num}.xml"

            if not path_xml.exists():
                # 시즌은 TV 쇼의 데이터를 사용, 에피소드는 TV 쇼의 데이터에 없어 건너뛰기
                if row['metadata_type'] == 4:
                    continue
                else:
                    _tree = tree_info
            else:
                _tree = ET.parse(path_xml)

        for media in info_media:
            # 기존에 사용하던 포스터를 우선 선택
            found = None
            for candidate in info_media[media]['candidates']:
                found = _tree.find(candidate)
                if found is not None:
                    break
            if found is None:
                # 없으면 첫번째 포스터를 차선택
                items = _tree.findall(info_media[media]['xpath_urls'])
                found_url = items[0].get('url') if items else None
            else:
                found_url = found.get('url')
            # url이 http로 시작하면 db를 이 주소로 수정하도록 목록에 추가
            if found_url and found_url.startswith('http') and not row[info_media[media]['column']].startswith('http'):
                to_be_updated.setdefault(row['id'], {})
                to_be_updated[row['id']][info_media[media]['column']] = (found_url, row[info_media[media]['column']])
    logger.info(f'Update: 미디어 URL을 찾은 메타데이터 개수: {len(to_be_updated)}')
    countdown(start_count)
    if not dry_run and to_be_updated:
        batch_queries = []
        for _id in to_be_updated:
            for column in to_be_updated[_id]:
                batch_queries.append(f"UPDATE metadata_items SET {column} = '" + to_be_updated[_id][column][0].replace("'", "''") + f"' WHERE id = {_id}")
        plex.execute_batch(batch_queries)


async def phase_3(con: sqlite3.Connection,
                  query: str,
                  dry_run: bool = config.dry_run,
                  start_count: int = config.countdown,
                  media_path: str = config.media,
                  plex_link: str = config.link,
                  worker_size: int = config.workers) -> None:
    to_be_refreshed = set()
    to_be_analyzed = dict()
    not_exists = {
        1: dict(),
        2: dict(),
        3: dict(),
        4: dict(),
    }
    # 3차 시도: DB에 입력된 미디어의 파일이 존재하지 않은 경우 업데이트
    for idx, row in enumerate(con.execute(query)):
        logger.debug(f'{idx}. 3차 분석중: id={row["id"]} title="{row["title"]}"')
        '''
        upload://posters/seasons/8/episodes/9/com.plexapp.agents.sjva_agent_eb975fea11e39b810d6e028a7dada7a2dc250b52
        upload://posters/com.plexapp.agents.sjva_agent_6bb677711346144821e9ca98a0f7d8ff994cffb1
        metadata://seasons/2/episodes/4/thumbs/com.plexapp.agents.sjva_agent_7280d544900659a61225b4516d2202b1f7f80c44
        metadata://posters/tv.plex.agents.series_e259ecc843b8b648eb21a5b308a9445daa8af835
        metadata://themes/tv.plex.agents.series_fdf5ffbaeed015f05450dff6352c1be9bf6e6ee2
        metadata://seasons/1/posters/com.plexapp.agents.sjva_agent_c1ab2c544b62bda323fc23409d1bccc389f5e152
        metadata://seasons/1/episodes/6/thumbs/com.plexapp.agents.sjva_agent_a7cd356d8432f9494dfa71b59d574b3687cb997b
        media://c/a7b0b87bec1b4257f8deffefa012739b462e735.bundle/Contents/Thumbnails/thumb1.jpg
        https://metadata-static.plex.tv/extras/iva/895274/40486edd920333c90d9e685649ff0e8c.jpg
        '''
        hash, parent_row, grand_parent_row = plex.get_ancestors(row, con)
        if row['metadata_type'] == 3 and not parent_row:
            logger.warning(f'시즌의 부모가 없음: {row}')
            continue
        if row['metadata_type'] == 4 and not grand_parent_row:
            logger.warning(f'에피소드의 조부모가 없음: {row}')
            continue

        path_contents = plex.get_bundle_path(hash, row['metadata_type']) + '/Contents'

        #for column in config.metadata_url_columns:
        for column in ('user_thumb_url',):
            if not row.get(column):
                continue
            scheme, _, path = row[column].partition('://')
            if scheme.startswith('http'):
                continue
            if scheme == 'media':
                full_path = pathlib.Path(media_path) / 'localhost' / path
            else:
                full_path = path_contents / '_combined' / path

            if not full_path.exists():
                # taggings에 http 정보가 있는지 확인
                tagging_row = con.execute(
                    f"""SELECT id, text
                    FROM taggings
                    WHERE thumb_url = ? AND metadata_item_id = ?""",
                    (row.get(column), row['id'])
                ).fetchone()

                if tagging_row and (text := tagging_row.get('text')) and text.startswith('http'):
                    continue
                if row['metadata_type'] == 3:
                    to_be_refreshed.add(parent_row['id'])
                elif row['metadata_type'] == 4:
                    # 쇼 id: 에피소드 id
                    to_be_analyzed[grand_parent_row['id']] = row['id']
                else:
                    to_be_refreshed.add(row['id'])
                not_exists[row['metadata_type']].setdefault(row['id'], {})
                not_exists[row['metadata_type']][row['id']][column] = full_path

    logger.info(f'영화 포스터 파일 누락: {len(not_exists[1])}')
    logger.info(f'TV쇼 포스터 파일 누락: {len(not_exists[2])}')
    logger.info(f'시즌 포스터 파일 누락: {len(not_exists[3])}')
    logger.info(f'에피소드 썸네일 파일 누락: {len(not_exists[4])}')

    for _id in to_be_refreshed:
        to_be_analyzed.pop(_id, None)

    for _id in to_be_refreshed:
        logger.debug(f'{_id}: link="{plex_link + str(_id)}"')
    logger.info(f'Refresh: 메타데이터 새로고침이 필요한 메타데이터 개수: {len(to_be_refreshed)}')
    countdown(start_count)
    if not dry_run and to_be_refreshed:
        queue = asyncio.Queue()
        await queue_task(worker, queue, to_be_refreshed, task_size=worker_size, job='refresh')

    for _id in to_be_analyzed.values():
        logger.debug(f'{_id}: link="{plex_link + str(_id)}"')
    logger.info(f'Analyze: 분석이 필요한 에피소드 개수: {len(to_be_analyzed)}')
    countdown(start_count)
    if not dry_run and to_be_analyzed:
        queue = asyncio.Queue()
        await queue_task(worker, queue, to_be_analyzed.values(), task_size=worker_size, job='analyze')


@plex.retrieve_db
async def update_metamedia(metadata_id: int | str = None, section_id: int | str = None, query: str = None, con: sqlite3.Connection = None) -> None:
    select_query = f"SELECT * FROM metadata_items WHERE metadata_type IN (1, 2, 3, 4)"
    if metadata_id and int(metadata_id) > 0:
        select_query += f" AND id = {metadata_id}"
    elif section_id and int(section_id) > 0:
        select_query += f" AND library_section_id = {section_id}"
    elif query:
        select_query = query
    await phase_1(con, select_query)
    await phase_2(con, select_query)
    await phase_3(con, select_query)


async def worker(queue: asyncio.Queue, name: str, job: str, plex_link: str = config.link) -> None:
    while True:
        id_ = await queue.get()
        if id_ is None:
            queue.task_done()
            break
        link = plex_link + str(id_)
        info = f"id={id_} link=\"{link}\""
        logger.debug(f'작업 시작({name}): {info}')
        try:
            start = time.time()
            if job == 'refresh':
                result = await plex.refresh(id_)
            elif job == 'analyze':
                result = await plex.analyze(id_)
            await plex.check_update(id_, result, start=start)
        finally:
            queue.task_done()
            logger.debug(f'작업 종료({name}): {info}')


async def main_(*args: Any, **kwds: Any):
    await update_metamedia(*args, **kwds)


def main(*args: Any, **kwds: Any):
    asyncio.run(main_(*args, **kwds))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(*sys.argv[1:])
    else:
        main()
