"""
데이터베이스 연결 모듈
SQLite 및 PostgreSQL 연결을 관리합니다.
"""

import os
import sqlite3
from pathlib import Path
from typing import Optional, Union
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import sys

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 설정 모듈 임포트
try:
    from config.settings import get_database_config
except ImportError:
    # 설정 모듈이 없는 경우 기본값 사용
    def get_database_config():
        return {
            'type': 'sqlite',
            'sqlite_path': 'data/app.db',
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'password',
            'dbname': 'file_monitor_db'
        }


class DatabaseManager:
    """데이터베이스 연결 및 세션 관리를 담당하는 클래스"""
    
    def __init__(self):
        self.config = get_database_config()
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
        self._setup_database()
    
    def _setup_database(self):
        """데이터베이스 연결을 설정합니다."""
        db_type = self.config.get('type', 'sqlite')
        
        if db_type == 'sqlite':
            self._setup_sqlite()
        elif db_type == 'postgresql':
            self._setup_postgresql()
        else:
            raise ValueError(f"지원하지 않는 데이터베이스 타입: {db_type}")
    
    def _setup_sqlite(self):
        """SQLite 데이터베이스를 설정합니다."""
        sqlite_path = self.config.get('sqlite_path', 'data/app.db')
        
        # 데이터베이스 디렉토리 생성
        db_path = Path(sqlite_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # SQLite 연결 문자열
        database_url = f"sqlite:///{sqlite_path}"
        
        # SQLite 엔진 생성
        self.engine = create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            poolclass=QueuePool,
            pool_size=1,
            max_overflow=0,
            echo=False  # SQL 쿼리 로깅 (개발 시 True로 설정)
        )
        
        # 세션 팩토리 생성
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        print(f"SQLite 데이터베이스 연결 설정 완료: {sqlite_path}")
    
    def _setup_postgresql(self):
        """PostgreSQL 데이터베이스를 설정합니다."""
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 5432)
        user = self.config.get('user', 'postgres')
        password = self.config.get('password', '')
        dbname = self.config.get('dbname', 'file_monitor_db')
        pool_size = self.config.get('pool_size', 5)
        max_overflow = self.config.get('max_overflow', 10)
        
        # PostgreSQL 연결 문자열
        database_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # PostgreSQL 엔진 생성
        self.engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,  # 연결 상태 확인
            echo=False  # SQL 쿼리 로깅 (개발 시 True로 설정)
        )
        
        # 세션 팩토리 생성
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        print(f"PostgreSQL 데이터베이스 연결 설정 완료: {host}:{port}/{dbname}")
    
    def get_engine(self) -> Engine:
        """데이터베이스 엔진을 반환합니다."""
        if not self.engine:
            raise RuntimeError("데이터베이스 엔진이 초기화되지 않았습니다.")
        return self.engine
    
    def get_session(self) -> Session:
        """데이터베이스 세션을 반환합니다."""
        if not self.SessionLocal:
            raise RuntimeError("데이터베이스 세션이 초기화되지 않았습니다.")
        return self.SessionLocal()
    
    def test_connection(self) -> bool:
        """데이터베이스 연결을 테스트합니다."""
        try:
            with self.get_session() as session:
                if self.config.get('type') == 'sqlite':
                    result = session.execute(text("SELECT 1"))
                else:
                    result = session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            print(f"데이터베이스 연결 테스트 실패: {e}")
            return False
    
    def create_tables(self, base):
        """데이터베이스 테이블을 생성합니다."""
        try:
            base.metadata.create_all(bind=self.engine)
            print("데이터베이스 테이블 생성 완료")
        except Exception as e:
            print(f"테이블 생성 실패: {e}")
    
    def drop_tables(self, base):
        """데이터베이스 테이블을 삭제합니다."""
        try:
            base.metadata.drop_all(bind=self.engine)
            print("데이터베이스 테이블 삭제 완료")
        except Exception as e:
            print(f"테이블 삭제 실패: {e}")
    
    def close(self):
        """데이터베이스 연결을 종료합니다."""
        if self.engine:
            self.engine.dispose()
            print("데이터베이스 연결이 종료되었습니다.")


# 전역 데이터베이스 매니저 인스턴스
_db_manager = DatabaseManager()

def get_db_manager() -> DatabaseManager:
    """데이터베이스 매니저를 반환합니다."""
    return _db_manager

def get_engine() -> Engine:
    """데이터베이스 엔진을 반환합니다."""
    return _db_manager.get_engine()

def get_session() -> Session:
    """데이터베이스 세션을 반환합니다."""
    return _db_manager.get_session()

def test_connection() -> bool:
    """데이터베이스 연결을 테스트합니다."""
    return _db_manager.test_connection()

def create_tables(base):
    """데이터베이스 테이블을 생성합니다."""
    _db_manager.create_tables(base)

def drop_tables(base):
    """데이터베이스 테이블을 삭제합니다."""
    _db_manager.drop_tables(base)


# 데이터베이스 연결 테스트
if __name__ == "__main__":
    print("데이터베이스 연결 테스트:")
    
    # 연결 테스트
    if test_connection():
        print("✅ 데이터베이스 연결 성공!")
    else:
        print("❌ 데이터베이스 연결 실패!")
    
    # 설정 정보 출력
    config = get_database_config()
    print(f"\n데이터베이스 설정:")
    for key, value in config.items():
        if key != 'password':  # 비밀번호는 보안상 출력하지 않음
            print(f"  {key}: {value}")
    
    # 엔진 정보 출력
    engine = get_engine()
    print(f"\n데이터베이스 엔진:")
    print(f"  타입: {type(engine).__name__}")
    print(f"  URL: {engine.url}")
    
    # 연결 종료
    _db_manager.close()
