"""
데이터베이스 기본 모델 모듈
SQLAlchemy Base 클래스와 공통 모델 기능을 정의합니다.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import event

# SQLAlchemy Base 클래스 생성
Base = declarative_base()


class TimestampMixin:
    """타임스탬프 필드를 제공하는 믹스인 클래스"""
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SoftDeleteMixin:
    """소프트 삭제 기능을 제공하는 믹스인 클래스"""
    
    is_deleted = Column(Boolean, default=False, nullable=False)
    deleted_at = Column(DateTime, nullable=True)


class BaseModel(Base, TimestampMixin):
    """모든 모델의 기본 클래스"""
    
    __abstract__ = True
    
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    
    def save(self, session: Session) -> 'BaseModel':
        """모델을 데이터베이스에 저장합니다."""
        try:
            session.add(self)
            session.commit()
            session.refresh(self)
            return self
        except Exception as e:
            session.rollback()
            raise e
    
    def update(self, session: Session, **kwargs) -> 'BaseModel':
        """모델을 업데이트합니다."""
        try:
            for key, value in kwargs.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            
            self.updated_at = datetime.utcnow()
            session.commit()
            session.refresh(self)
            return self
        except Exception as e:
            session.rollback()
            raise e
    
    def delete(self, session: Session, hard_delete: bool = False) -> bool:
        """모델을 삭제합니다."""
        try:
            if hard_delete or not hasattr(self, 'is_deleted'):
                session.delete(self)
            else:
                # 소프트 삭제
                self.is_deleted = True
                self.deleted_at = datetime.utcnow()
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            raise e
    
    def to_dict(self) -> dict:
        """모델을 딕셔너리로 변환합니다."""
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                result[column.name] = value.isoformat()
            else:
                result[column.name] = value
        return result
    
    @classmethod
    def get_by_id(cls, session: Session, id: int) -> Optional['BaseModel']:
        """ID로 모델을 조회합니다."""
        return session.query(cls).filter(cls.id == id).first()
    
    @classmethod
    def get_all(cls, session: Session, limit: Optional[int] = None, offset: Optional[int] = None) -> list:
        """모든 모델을 조회합니다."""
        query = session.query(cls)
        
        if hasattr(cls, 'is_deleted'):
            query = query.filter(cls.is_deleted == False)
        
        if offset:
            query = query.offset(offset)
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def count(cls, session: Session) -> int:
        """모델의 총 개수를 반환합니다."""
        query = session.query(cls)
        
        if hasattr(cls, 'is_deleted'):
            query = query.filter(cls.is_deleted == False)
        
        return query.count()


# 데이터베이스 이벤트 리스너 설정
@event.listens_for(Base, 'before_insert', propagate=True)
def timestamp_before_insert(mapper, connection, target):
    """삽입 전 타임스탬프 설정"""
    if hasattr(target, 'created_at'):
        target.created_at = datetime.utcnow()
    if hasattr(target, 'updated_at'):
        target.updated_at = datetime.utcnow()


@event.listens_for(Base, 'before_update', propagate=True)
def timestamp_before_update(mapper, connection, target):
    """업데이트 전 타임스탬프 설정"""
    if hasattr(target, 'updated_at'):
        target.updated_at = datetime.utcnow()


# 유틸리티 함수들
def create_all_tables(engine):
    """모든 테이블을 생성합니다."""
    Base.metadata.create_all(bind=engine)


def drop_all_tables(engine):
    """모든 테이블을 삭제합니다."""
    Base.metadata.drop_all(bind=engine)


def get_table_names() -> list:
    """모든 테이블 이름을 반환합니다."""
    return list(Base.metadata.tables.keys())


# 테스트 코드
if __name__ == "__main__":
    print("SQLAlchemy Base 클래스 테스트:")
    
    # 테이블 정보 출력
    print(f"정의된 테이블: {get_table_names()}")
    
    # Base 클래스 정보 출력
    print(f"Base 클래스: {Base}")
    print(f"Base 메타데이터: {Base.metadata}")
    
    # 믹스인 클래스 정보 출력
    print(f"TimestampMixin: {TimestampMixin}")
    print(f"SoftDeleteMixin: {SoftDeleteMixin}")
    print(f"BaseModel: {BaseModel}")
    
    print("✅ Base 클래스 설정 완료!")
