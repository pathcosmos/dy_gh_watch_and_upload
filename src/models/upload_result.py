"""
업로드 결과 모델
API 응답 정보를 저장하는 모델입니다.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Float, JSON
from sqlalchemy.orm import Session

# 프로젝트 루트를 Python 경로에 추가
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.db.base import BaseModel, SoftDeleteMixin


class UploadResult(BaseModel, SoftDeleteMixin):
    """파일 업로드 결과를 저장하는 모델"""
    
    __tablename__ = 'upload_results'
    
    # 파일 정보
    file_path = Column(String(500), nullable=False, comment='원본 파일 경로')
    file_name = Column(String(255), nullable=False, comment='원본 파일명')
    file_size = Column(Integer, nullable=False, comment='파일 크기 (바이트)')
    file_extension = Column(String(20), nullable=True, comment='파일 확장자')
    
    # 모니터링 정보
    folder_name = Column(String(100), nullable=False, comment='모니터링된 폴더명 (Sega_1, Sega_2, Sega_3)')
    scan_date = Column(DateTime, nullable=False, comment='파일 스캔 날짜')
    
    # API 업로드 정보 (http://211.231.137.111:18000/upload 응답 기반)
    api_file_id = Column(String(100), nullable=True, comment='외부 API에서 할당된 파일 고유 식별자')
    api_filename = Column(String(255), nullable=True, comment='API 응답의 파일명')
    api_file_size = Column(Integer, nullable=True, comment='API 응답의 파일 크기')
    api_upload_time = Column(DateTime, nullable=True, comment='API 응답의 업로드 시간')
    download_url = Column(String(500), nullable=True, comment='API 응답의 다운로드 URL')
    view_url = Column(String(500), nullable=True, comment='API 응답의 파일 보기 URL')
    api_message = Column(Text, nullable=True, comment='API 응답 메시지')
    
    # 업로드 상태
    upload_status = Column(String(50), nullable=False, default='pending', comment='업로드 상태 (pending, in_progress, success, failed)')
    upload_attempts = Column(Integer, nullable=False, default=0, comment='업로드 시도 횟수')
    last_upload_attempt = Column(DateTime, nullable=True, comment='마지막 업로드 시도 시간')
    
    # 에러 정보
    error_message = Column(Text, nullable=True, comment='에러 메시지')
    error_details = Column(JSON, nullable=True, comment='상세 에러 정보 (JSON)')
    
    # 메타데이터
    extra_data = Column(JSON, nullable=True, comment='추가 메타데이터 (JSON)')
    
    def __repr__(self):
        return f"<UploadResult(id={self.id}, file_name='{self.file_name}', status='{self.upload_status}')>"
    
    @classmethod
    def create_from_file_info(cls, session: Session, file_path: str, folder_name: str, **kwargs) -> 'UploadResult':
        """파일 정보로부터 UploadResult를 생성합니다."""
        from pathlib import Path
        
        path_obj = Path(file_path)
        
        upload_result = cls(
            file_path=file_path,
            file_name=path_obj.name,
            file_size=path_obj.stat().st_size if path_obj.exists() else 0,
            file_extension=path_obj.suffix.lower(),
            folder_name=folder_name,
            scan_date=datetime.utcnow(),
            **kwargs
        )
        
        return upload_result.save(session)
    
    def update_api_response(self, session: Session, api_response: dict) -> 'UploadResult':
        """API 응답 정보로 모델을 업데이트합니다."""
        # API 응답에서 정보 추출
        self.api_file_id = api_response.get('file_id')
        self.api_filename = api_response.get('filename')
        self.api_file_size = api_response.get('file_size')
        
        # 업로드 시간 파싱
        if api_response.get('upload_time'):
            try:
                self.api_upload_time = datetime.fromisoformat(api_response['upload_time'].replace('Z', '+00:00'))
            except:
                self.api_upload_time = datetime.utcnow()
        
        self.download_url = api_response.get('download_url')
        self.view_url = api_response.get('view_url')
        self.api_message = api_response.get('message')
        
        # 업로드 성공으로 상태 변경
        self.upload_status = 'success'
        self.upload_attempts += 1
        self.last_upload_attempt = datetime.utcnow()
        
        return self.update(session)
    
    def mark_upload_failed(self, session: Session, error_message: str, error_details: Optional[dict] = None) -> 'UploadResult':
        """업로드 실패로 표시합니다."""
        self.upload_status = 'failed'
        self.upload_attempts += 1
        self.last_upload_attempt = datetime.utcnow()
        self.error_message = error_message
        self.error_details = error_details
        
        return self.update(session)
    
    def mark_in_progress(self, session: Session) -> 'UploadResult':
        """업로드 진행 중으로 표시합니다."""
        self.upload_status = 'in_progress'
        self.last_upload_attempt = datetime.utcnow()
        
        return self.update(session)
    
    def reset_for_retry(self, session: Session) -> 'UploadResult':
        """재시도를 위해 상태를 초기화합니다."""
        self.upload_status = 'pending'
        self.error_message = None
        self.error_details = None
        
        return self.update(session)
    
    @classmethod
    def get_pending_uploads(cls, session: Session, limit: Optional[int] = None) -> list:
        """업로드 대기 중인 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.upload_status == 'pending',
            cls.is_deleted == False
        ).order_by(cls.created_at.asc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_failed_uploads(cls, session: Session, limit: Optional[int] = None) -> list:
        """업로드 실패한 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.upload_status == 'failed',
            cls.is_deleted == False
        ).order_by(cls.last_upload_attempt.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_successful_uploads(cls, session: Session, limit: Optional[int] = None) -> list:
        """업로드 성공한 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.upload_status == 'success',
            cls.is_deleted == False
        ).order_by(cls.last_upload_attempt.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_by_folder(cls, session: Session, folder_name: str, limit: Optional[int] = None) -> list:
        """특정 폴더의 업로드 결과를 조회합니다."""
        query = session.query(cls).filter(
            cls.folder_name == folder_name,
            cls.is_deleted == False
        ).order_by(cls.created_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_by_date_range(cls, session: Session, start_date: datetime, end_date: datetime, limit: Optional[int] = None) -> list:
        """날짜 범위로 업로드 결과를 조회합니다."""
        query = session.query(cls).filter(
            cls.scan_date >= start_date,
            cls.scan_date <= end_date,
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()


# 테스트 코드
if __name__ == "__main__":
    print("UploadResult 모델 테스트:")
    
    # 모델 정보 출력
    print(f"테이블명: {UploadResult.__tablename__}")
    print(f"컬럼 수: {len(UploadResult.__table__.columns)}")
    
    # 컬럼 정보 출력
    print("\n컬럼 정보:")
    for column in UploadResult.__table__.columns:
        print(f"  {column.name}: {column.type} (nullable={column.nullable})")
    
    # 메서드 정보 출력
    print(f"\n메서드 수: {len([m for m in dir(UploadResult) if not m.startswith('_')])}")
    
    print("✅ UploadResult 모델 설정 완료!")
