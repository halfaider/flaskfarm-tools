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

import plex
import plex_update_metamedia
import plex_rematch
import config
from helpers import mem_usage

logger = logging.getLogger(__name__)

async def main(*args: Any, **kwds: Any) -> None:
    """
    포스터 파일이 삭제되어 포스터 표시가 안 되는 메타데이터 중 포스터 url 정보가 있으면 url로 대체
    URL이 없을 경우 영화와 쇼는 새로고침, 에피소드는 분석
    모든 섹션을 지정하려면 -1 입력"""
    #await plex_update_metamedia.update_metamedia(1)

    """
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
    #plex_rematch.main(query)

    """
    파일이 삭제되었지만 휴지통 비우기로 처리되지 않는 미디어를 DB에서 삭제
    마운트 오류로 삭제되는 걸 방지하기 위해 두번째 인자의 경로가 존재할 때만 삭제처리
    모든 섹션을 지정하려면 -1 입력"""
    #await plex.delete_not_exists(12, '/mnt/cloud/gds/GDRIVE/VIDEO/방송중')

    """
    라이브러리 색인 목록의 음절을 자음으로 수정
    모든 섹션을 지정하려면 -1 입력"""
    #plex.update_title_sort(1)

    """
    부가 영상의 url을 수정"""
    #plex.update_clip_key('찾을 내용', '바꿀 내용')

    """
    리뷰의 source를 수정"""
    #plex.update_review_source()

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
    Kavita
    """
    #import kavita
    #await kavita.scan_folder('/mnt/gds2/GDRIVE/READING/책/일반/가')

    #rows = kavita.fetch_all('SELECT * FROM series LIMIT 10')
    #for row in rows:
    #    print(row)


if __name__ == "__main__":
    start_mem = mem_usage()
    start_time = time.time()
    asyncio.run(main())
    logger.debug(f"메모리 변동: {mem_usage() - start_mem:.3f}MB")
    logger.debug(f"걸린 시간: {time.time() - start_time:.3f}s")
