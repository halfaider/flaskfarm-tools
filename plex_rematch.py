import sys
import asyncio
import pathlib
import logging
import traceback
from difflib import SequenceMatcher
from typing import Any, Iterable

import plex
from helpers import queue_task, check_packages
from config import plex as config

check_packages((('guessit', 'guessit'),))

from guessit import guessit

logger = logging.getLogger(__name__)

NO_MATCHES = []


def get_keyword(guid: str, target_agent: str) -> str | None:
    '''
    plex.agents
    {tmdb-696506}
    {imdb-tt33175825}: 아직 테스트 단계

    sjva_agent
    FT132868
    MT407777
    '''
    source_agent, meta_id = guid.split('://')
    meta_id = meta_id.split('?')[0]
    if source_agent == target_agent:
        return
    if source_agent not in ('com.plexapp.agents.sjva_agent', 'com.plexapp.agents.themoviedb'):
        return
    if source_agent == 'com.plexapp.agents.sjva_agent' and meta_id[0:2] in ('FT', 'MT'):
        meta_id = meta_id[2:]
    if target_agent.startswith('tv.plex.agents'):
        return "{" + f"tmdb-{meta_id}" + "}"
    if target_agent == 'com.plexapp.agents.sjva_agent':
        return f"MT{meta_id}"


def resolve_agent(agent: str, section_id: int) -> str:
    if not agent:
        agent = plex.get_section_by_id(section_id)['agent']
    return agent


def skip_for_safe(row: dict, agent: str, score: int) -> bool:
    if row['metadata_type'] not in (1, 2):
        logger.debug(f"지원하지 않는 메타데이터 타입: {row['metadata_type']}")
        return True
    if score == 999:
        logger.debug(f"검색 건너 뛰기: id={row['id']} title=\"{row['title']}\" agent=\"{agent}\"")
        return True
    return False


async def match_with_guid(row: dict, agent: str) -> bool:
    if keyword := get_keyword(row['guid'], agent):
        result = await plex.matches(row['id'], keyword, None, agent)
        if 300 > result.get('status_code') > 199 or not result.get('json'):
            container = (result.get('json') or {}).get('MediaContainer') or {}
            search_results = container.get('SearchResult')
            if search_results:
                sr = search_results[0]
                logger.info(f"GUID로 매칭: \"{row['title']}\" ({row['year']}) => name=\"{sr['name']}\" year={sr.get('year')} guid={sr['guid']} score={sr.get('score') or -1}")
                await plex.rematch(row['id'], sr['guid'], sr['name'], sr.get('year'))
                return True
    return False


def get_file_info(row: dict) -> tuple[str, int]:
    if row['metadata_type'] == 2:
        # TV 쇼의 media_parts는 에피소드의 메타데이터와 연계
        episode_row = plex.fetch_one(f"SELECT * FROM metadata_items WHERE parent_id IN (SELECT DISTINCT id FROM metadata_items WHERE parent_id = {row['id']})")
        query_target_id = episode_row['id']
    else:
        query_target_id = row['id']
    media_parts = plex.get_media_parts_by_metadata_id(query_target_id)
    if media_parts:
        file = pathlib.Path(media_parts[0].get('file') or '')
        f_matches = guessit(file.name)
        alter = f_matches.get('alternative_title') or ''
        if type(alter) is list:
            alter = ' '.join(alter).strip()
        f_title = ' '.join((f_matches.get('title') or '', alter)).strip()
        f_year = f_matches.get('year') or -1
        logger.debug(f'파일이름: title="{f_title}" year={f_year}')
    else:
        f_title = ''
        f_year = -1
    return f_title, f_year


async def handle_matches(row: dict, agent: str = None, score: int= -1, plex_link: str = config.link) -> bool:
    agent = resolve_agent(agent, row['library_section_id'])
    if skip_for_safe(row, agent, score):
        return False

    # 기존 guid에 메타데이터 사이트의 id가 있는지 확인
    if await match_with_guid(row, agent):
        return True

    # 파일명을 우선 검색
    f_title, f_year = get_file_info(row)
    year = f_year if f_year > 0 else row['year']
    title_candidates = [f_title] if f_title else []
    for title in (row['title'], row['original_title']):
        if title and title not in title_candidates:
            title_candidates.append(title)

    # title 후보로 모두 검색 후 비교 시작
    search_results = []
    for title in title_candidates:
        result = await plex.matches(row['id'], title, None, agent)
        if not 300 > result.get('status_code') > 199 or not result.get('json'):
            logger.warning(f"검색을 할 수 없어요: {result['status_code']} {result['url']} {result['text']}")
            continue
        container = (result.get('json') or {}).get('MediaContainer') or {}
        if (container.get('size') or 0) < 1:
            logger.warning(f"검색 결과 없음: {title=} {year=}")
            continue
        search_results.extend(container.get('SearchResult') or [])

    for sr in search_results:
        try:
            if not is_match_with(row, sr, title_candidates, year, score):
                continue
            # 최종 변경 대상
            logger.info(f"변경: \"{title_candidates[0]}\" ({year}) => name=\"{sr['name']}\" year={sr.get('year')} guid={sr['guid']} score={sr.get('score') or -1}")
            await plex.rematch(row['id'], sr['guid'], sr['name'], sr.get('year'))
            return True
        except:
            logger.error(traceback.format_exc())
    logger.info(f"일치하는 검색이 없어요: {title_candidates[0]} ({year}) link={plex_link + str(row['id'])}")
    return False


def is_match_with(row: dict,
                  sr: dict,
                  title_candidates: list,
                  year: int,
                  score: int,
                  title_match_ratio: float = config.title_match_ratio,
                  margin_of_year: int = config.margin_of_year,
                  force_rematch: bool = config.force_rematch) -> bool:
    prefix_msg = f"\"{title_candidates[0]}\" ({year}) : \"{sr.get('name')}\" ({sr.get('year')})"
    # 포스터가 디스코드 링크일 경우
    if (sr.get('thumb') or '').find('discord') > 0:
        logger.debug(f"{prefix_msg} >> 건너뛰기-디스코드 링크: \"{sr.get('thumb')}\"")
        return False

    # 제목 일치율이 TITLE_MATCH_RATIO 미만일 경우
    matcher = SequenceMatcher(None, sr.get('name') or '', '')
    for title in title_candidates:
        matcher.set_seq2(title)
        if matcher.ratio() >= title_match_ratio:
            logger.debug(f"\"{title}\" ({year}) : \"{sr.get('name')}\" ({sr.get('year')}) >> 제목 일치율: {matcher.ratio() * 100:.2f}%")
            break
    else:
        logger.debug(f"{prefix_msg} >> 건너뛰기-제목 일치율")
        return False

    # 연도가 오차 범위를 벗어날 경우
    if year and not year + margin_of_year >= (sr.get('year') or 1900) >= year - margin_of_year:
        logger.debug(f"{prefix_msg} >> 건너뛰기-연도: {sr.get('year')}")
        return False

    # 기준 점수보다 미만일 경우
    if (sr.get('score') or -1) < score:
        logger.debug(f"{prefix_msg} >> 건너뛰기-점수: {sr.get('score')}")
        return False

    # 이미 동일한 guid이고 강제 조건이 아니면 rematch 안 함
    if row['guid'] == sr['guid'] and not force_rematch:
        logger.debug(f"{prefix_msg} >> 건너뛰기-동일한 guid: {sr['guid']}")
        return False

    return True


async def worker(queue: asyncio.Queue,
                 name: str,
                 plex_link: str = config.link,
                 score_min: int = config.score_min,
                 score_min_extra: int = config.score_min_extra,
                 extra_agents: Iterable[str] = config.extra_agents,
                 plex_media_types: Iterable[str] = config.media_types) -> None:
    while True:
        row = await queue.get()
        if row is None:
            queue.task_done()
            break
        logger.debug(f"대기 중인 작업: {queue.qsize()} 건")
        link = plex_link + str(row['id'])
        info = f"id={row['id']} title=\"{row['title']}\" link=\"{link}\""
        logger.debug(f'작업 시작({name}): {info}')
        try:
            if row['metadata_type'] in (1, 2, 8, 9):
                result = await handle_matches(row, score=score_min)
                if not result and extra_agents:
                    logger.debug(f"다른 에이전트로 시도: {extra_agents[row['metadata_type']]}")
                    result = await handle_matches(row, agent=extra_agents[row['metadata_type']], score=score_min_extra)
                if not result:
                    NO_MATCHES.append((row['title'], plex_link + str(row['id'])))
            else:
                logger.warning(f"지원하지 않는 메타데이터 타입: {plex_media_types[row['metadata_type']]}")
        finally:
            queue.task_done()
            logger.debug(f'작업 종료({name}): {info}')


async def main_(query: str, dry_run: bool = config.dry_run, worker_size: int = config.workers, plex_link: str = config.link) -> None:
    if not dry_run:
        await queue_task(worker, asyncio.Queue(), plex.fetch_all(query), task_size=worker_size, prefix='rematch')
        if NO_MATCHES:
            for idx, no_match in enumerate(NO_MATCHES):
                logger.debug(f'{idx + 1:>03}. {no_match[0]}: {no_match[1]}')
            logger.info(f'\n직접 매치가 필요한 항목들: {len(NO_MATCHES)}\n')
    else:
        counter = 0
        for row in plex.fetch_all(query):
            link = plex_link + str(row['id'])
            logger.debug(f"{row['id']}: {row['title']} ({row['year']}) guid=\"{row['guid']}\" link=\"{link}\"")
            counter += 1
        logger.info(f'일치항목 수정을 시도할 메타데이터 개수: {counter}')


def main(*args: Any, **kwds: Any):
    asyncio.run(main_(*args, **kwds))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(*sys.argv[1:])
    else:
        main()
