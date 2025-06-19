"""
이 파일을 app.py로 복사해서 사용
아래 원하는 작업의 주석을 제거해서 다음과 같이 실행

python3 /path/to/app.py

"""
import time
import logging
import sqlite3
import asyncio
from typing import Any

import config
import plex
import plex_update_metamedia
import plex_rematch
import kavita
from helpers import mem_usage

logger = logging.getLogger(__name__)

async def main(*args: Any, **kwds: Any) -> None:
    """
    Plex 포스터 새로고침
    포스터 파일이 삭제되어 포스터 표시가 안 되는 메타데이터 중 포스터 url 정보가 있으면 url로 대체
    URL이 없을 경우 영화와 쇼는 새로고침, 에피소드는 분석
    모든 섹션을 지정하려면 -1 입력"""
    #await plex_update_metamedia.update_metamedia(1)

    """
    Plex 일치항목 일괄 수정
    - "title" column 값이 정확히 "인터스텔라"인 데이터:
        SELECT * FROM metadata_items WHERE title = '인터스텔라'
    - 네이버 영화 메타데이터가 입혀진 데이터:
        SELECT * FROM metadata_items WHERE guid LIKE '%sjva_agent://MN%'
    - 다음 영화 메타데이터가 입혀진 데이터:
        SELECT * FROM metadata_items WHERE guid LIKE '%sjva_agent://MD%'
    - 섹션 아이디가 정확히 4 인 데이터:
        SELECT * FROM metadata_items WHERE library_section_id = 4
    - 섹션 아이디가 4 이고 다음 영화 메타데이터가 입혀진 데이터:
        SELECT * FROM metadata_items WHERE library_section_id = 4 AND guid LIKE '%sjva_agent://MD%'
    - 섹션 아이디가 1 이고 user_thumb_url이 http로 시작하지 않는 데이터:
        SELECT * FROM metadata_items WHERE library_section_id = 1 AND user_thumb_url NOT LIKE 'http%'
    - 매칭 안 된 항목:
        SELECT * FROM metadata_items WHERE guid LIKE 'local://%'
    - guid 중복:
        SELECT * FROM metadata_items WHERE guid IN (SELECT guid FROM metadata_items WHERE library_section_id = 12 GROUP BY guid HAVING COUNT(guid) > 1) AND library_section_id = 12
    - 테스트로 10개만 시도:
        SELECT * FROM metadata_items WHERE guid LIKE '%sjva_agent://MD% LIMIT 10'
    쿼리문을 대상으로 일치항목 변경을 시도
    """
    #query = f"SELECT * FROM metadata_items WHERE guid LIKE '%sjva_agent://%' AND metadata_type = 1 LIMIT 10;"
    #await plex_rematch.main_(query)

    """
    Plex 일치항목 강제 수정
    기본 에이전트로 설정된 라이브러리에서 타 에이전트로 강제 매칭을 시도
    매칭 후 메타데이터 새로고침을 하면 원래 에이전트의 데이터로 복구 됨
    """
    #await plex_rematch.force_match_with_agent([104435, 120317], 'com.plexapp.agents.sjva_agent_movie')

    """
    Plex 휴지통 처리
    파일이 삭제되었지만 휴지통 비우기로 처리되지 않는 미디어를 DB에서 삭제
    마운트 오류로 삭제되는 걸 방지하기 위해 두번째 인자의 경로가 존재할 때만 삭제처리
    모든 섹션을 지정하려면 -1 입력"""
    #await plex.delete_not_exists(12, '/mnt/cloud/gds/GDRIVE/VIDEO/방송중')

    """
    Plex 색인 정리
    라이브러리 색인 목록의 음절을 자음으로 수정
    모든 섹션을 지정하려면 -1 입력"""
    #plex.update_title_sort(1)

    """
    Plex 부가 영상의 url을 수정"""
    #plex.update_clip_key('찾을 내용', '바꿀 내용')

    """
    Plex 리뷰의 source를 수정
    Plex 어플리케이션에서 오류 발생시 실행"""
    #plex.update_review_source()

    """
    Plex 메타데이터의 번들 폴더를 삭제 후 새로고침
    누적된 포스터 목록을 초기화 하는 등의 용도로 사용
    메타데이터 ID를 입력"""
    #await plex.clean_bundle(123456)

    """
    Plex 라이브러리를 검색하여 가짜 포스터 파일(None)이 있을 경우 번들 폴더를 삭제 후 새로고침
    메타데이터 새로고침 후 가짜 파일이 선택되어 포스터가 표시 안 될 경우 사용
    라이브러리 ID를 입력"""
    #await plex.find_and_clean_bundle(1)

    """
    Plex DB 쿼리 실행
    조회는 fectch_all() 혹은 fetch_one() 사용
    DB 수정은 execte(), execute_batch(), execute_gen() 사용
    connection을 직접 다룰 경우 @plex.retrieve_db 데코레이터 사용"""
    #query = f"SELECT * FROM metadata_items WHERE metadata_type = 3;"
    #print(plex.fetch_one(query))

    #query = f"SELECT id, extra_data FROM taggings WHERE tag_id IN (SELECT id from tags WHERE tag_type = 10)"
    #for row in plex.fetch_all(query):
    #    print(row)

    #query = "UPDATE metadata_items SET content_rating_age = 15 WHERE id = 1"
    #plex.execute(query)

    #queries = [
    #    "UPDATE metadata_items SET content_rating_age = 19 WHERE id = 1",
    #    "UPDATE metadata_items SET content_rating_age = 19 WHERE id = 2",
    #]
    #plex.execute_batch(queries)

    #@plex.retrieve_db
    #def test(con: sqlite3.Connection) -> None:
    #    query = "SELECT * FROM metadata_items WHERE metadata_type = 2"
    #    for row in con.execute(query):
    #        print(row)
    #test()

    #async def refresh_season() -> None:
    #    query = f"SELECT * FROM metadata_items WHERE metadata_type = 3 AND guid LIKE '%local://%';"
    #    for row in plex.fetch_all(query):
    #        start = time.time()
    #        logger.debug(row)
    #        result = await plex.refresh(row['id'])
    #        await plex.check_update(row['id'], result, start)
    #await refresh_season()

    """
    Google Drive 파일 검색
    https://developers.google.com/workspace/drive/api/guides/search-files?hl=ko"""
    #from google_drive import google_drive
    #query = "name contains '런닝맨' and name contains '240218'"
    #result = google_drive.get_files(query)
    #print(result)
    #file = result.get('files')[0]
    #result = google_drive.get_file(file['id'])
    #print(result)

    """
    Kavita 커버 파일 분산
    covers 폴더 하나에 너무 많은 커버 이미지 파일이 집중되는 것을 방지하기 위해서
    가능할 경우 각 라이브러리 폴더에 커버 이미지 파일을 분산"""
    #kavita.organize_covers('/mnt/cloud/kavita/covers', dry_run=False)

    """
    Kavita 커버 파일 정리
    사용하지 않는 커버 파일을 찾아서 삭제"""
    #kavita.clean_covers('/mnt/cloud/kavita/covers', dry_run=False)

    """
    Kavita 폴더 스캔
    해당 폴더를 포함하는 라이브러리를 스캔"""
    #await kavita.scan_folder('/mnt/gds2/GDRIVE/READING/책/일반/가')

    """
    Kavita 경로로 시리즈 스캔
    이미 존재하는 하나의 시리즈만 검색 되도록 경로를 지정"""
    #await kavita.scan_series_by_path('/mnt/gds2/GDRIVE/READING/만화/연재/아/열혈강호/01권#199.zip', is_dir=False)

    """
    Kavita 시리즈 스캔
    이미 존재하는 시리즈 ID를 지정하여 스캔"""
    #await kavita.scan_series(12345)

    """
    Kavita 모든 라이브러리를 스캔"""
    #await kavita.scan_all()

    """
    Kavita 특정 라이브러리만 스캔"""
    #await kavita.scan(103, force=True)

    """
    Kavita DB 쿼리 실행"""
    #row = kavita.fetch_one('SELECT * FROM Library WHERE id = ?, (123,))
    #print(row)
    #rows = kavita.fetch_all('SELECT * FROM Library')
    #for row in rows:
    #    print(row)
    #cs = kavita.execute('SELECT * FROM Series WHERE id = :id', {'id': 12345})
    #print(cs.fetchone())


if __name__ == "__main__":
    start_mem = mem_usage()
    start_time = time.time()
    asyncio.run(main())
    logger.debug(f"메모리 변동: {mem_usage() - start_mem:.3f}MB")
    logger.debug(f"걸린 시간: {time.time() - start_time:.3f}s")
