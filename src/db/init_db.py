"""
데이터베이스 초기화 및 테이블 생성 스크립트
"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.db.connection import DatabaseManager
from src.db.base import Base
from src.models import FileInfo, UploadResult
from src.utils.logger import LoggerManager
from config.settings import get_config


def init_database():
    """데이터베이스를 초기화하고 테이블을 생성합니다."""
    
    # 로거 설정
    logger_manager = LoggerManager()
    logger = logger_manager.get_logger(__name__)
    
    try:
        logger.info("데이터베이스 초기화 시작")
        
        # 설정 로드
        config = get_config()
        db_config = config.get('database', {})
        
        logger.info(f"데이터베이스 타입: {db_config.get('type', 'sqlite')}")
        
        # 데이터베이스 매니저 초기화
        db_manager = DatabaseManager()
        
        # 연결 테스트
        logger.info("데이터베이스 연결 테스트 중...")
        if db_manager.test_connection():
            logger.info("✅ 데이터베이스 연결 성공")
        else:
            logger.error("❌ 데이터베이스 연결 실패")
            return False
        
        # 테이블 생성
        logger.info("데이터베이스 테이블 생성 중...")
        db_manager.create_tables(Base)
        logger.info("✅ 데이터베이스 테이블 생성 완료")
        
        # 테이블 정보 출력
        engine = db_manager.get_engine()
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        logger.info(f"생성된 테이블: {', '.join(tables)}")
        
        # 각 테이블의 컬럼 정보 출력
        for table_name in tables:
            columns = inspector.get_columns(table_name)
            logger.info(f"테이블 '{table_name}' 컬럼:")
            for column in columns:
                nullable = "NULL" if column['nullable'] else "NOT NULL"
                logger.info(f"  - {column['name']}: {column['type']} ({nullable})")
        
        logger.info("🎉 데이터베이스 초기화 완료!")
        return True
        
    except Exception as e:
        logger.error(f"데이터베이스 초기화 실패: {str(e)}")
        logger.exception("상세 에러 정보:")
        return False
    
    finally:
        # 데이터베이스 연결 종료
        if 'db_manager' in locals():
            db_manager.close()


def drop_database():
    """데이터베이스 테이블을 삭제합니다. (주의: 모든 데이터가 삭제됩니다)"""
    
    # 로거 설정
    logger_manager = LoggerManager()
    logger = logger_manager.get_logger(__name__)
    
    try:
        logger.warning("⚠️ 데이터베이스 테이블 삭제 시작 (모든 데이터가 삭제됩니다)")
        
        # 설정 로드
        config = get_config()
        db_config = config.get('database', {})
        
        # 데이터베이스 매니저 초기화
        db_manager = DatabaseManager()
        
        # 테이블 삭제
        logger.info("데이터베이스 테이블 삭제 중...")
        db_manager.drop_tables(Base)
        logger.info("✅ 데이터베이스 테이블 삭제 완료")
        
        return True
        
    except Exception as e:
        logger.error(f"데이터베이스 테이블 삭제 실패: {str(e)}")
        logger.exception("상세 에러 정보:")
        return False
    
    finally:
        # 데이터베이스 연결 종료
        if 'db_manager' in locals():
            db_manager.close()


def reset_database():
    """데이터베이스를 초기화합니다 (테이블 삭제 후 재생성)."""
    
    # 로거 설정
    logger_manager = LoggerManager()
    logger = logger_manager.get_logger(__name__)
    
    logger.info("🔄 데이터베이스 리셋 시작")
    
    # 테이블 삭제
    if drop_database():
        logger.info("테이블 삭제 완료")
        
        # 테이블 재생성
        if init_database():
            logger.info("🎉 데이터베이스 리셋 완료!")
            return True
        else:
            logger.error("❌ 테이블 재생성 실패")
            return False
    else:
        logger.error("❌ 테이블 삭제 실패")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='데이터베이스 초기화 도구')
    parser.add_argument('action', choices=['init', 'drop', 'reset'], 
                       help='수행할 작업: init(초기화), drop(삭제), reset(리셋)')
    
    args = parser.parse_args()
    
    if args.action == 'init':
        success = init_database()
        sys.exit(0 if success else 1)
    elif args.action == 'drop':
        success = drop_database()
        sys.exit(0 if success else 1)
    elif args.action == 'reset':
        success = reset_database()
        sys.exit(0 if success else 1)
