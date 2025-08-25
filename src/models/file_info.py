"""
파일 정보 모델
모니터링된 파일의 기본 정보를 저장하는 모델입니다.
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
from src.utils.logger import get_logger


class FileInfo(BaseModel, SoftDeleteMixin):
    """파일 정보를 저장하는 모델"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_logger(__name__)
    
    __tablename__ = 'file_infos'
    
    # 파일 기본 정보
    file_path = Column(String(500), nullable=False, unique=True, comment='파일의 절대 경로')
    file_name = Column(String(255), nullable=False, comment='파일명')
    file_size = Column(Integer, nullable=False, comment='파일 크기 (바이트)')
    file_extension = Column(String(20), nullable=True, comment='파일 확장자')
    
    # 파일 시스템 정보
    folder_path = Column(String(500), nullable=False, comment='파일이 위치한 폴더 경로')
    folder_name = Column(String(100), nullable=False, comment='모니터링된 폴더명 (Sega_1, Sega_2, Sega_3)')
    relative_path = Column(String(500), nullable=False, comment='기준 폴더로부터의 상대 경로')
    
    # 날짜 정보
    scan_date = Column(DateTime, nullable=False, comment='파일 스캔 날짜')
    file_created_date = Column(DateTime, nullable=True, comment='파일 생성 날짜')
    file_modified_date = Column(DateTime, nullable=True, comment='파일 수정 날짜')
    
    # 파일 상태
    is_image = Column(Boolean, nullable=False, default=True, comment='이미지 파일 여부')
    is_valid = Column(Boolean, nullable=False, default=True, comment='유효한 파일 여부')
    processing_status = Column(String(50), nullable=False, default='new', comment='처리 상태 (new, processing, processed, error)')
    
    # 파일 메타데이터
    mime_type = Column(String(100), nullable=True, comment='MIME 타입')
    image_dimensions = Column(String(50), nullable=True, comment='이미지 크기 (width x height)')
    color_space = Column(String(50), nullable=True, comment='색상 공간')
    extra_data = Column(JSON, nullable=True, comment='추가 파일 메타데이터 (JSON)')
    
    def __repr__(self):
        return f"<FileInfo(id={self.id}, file_name='{self.file_name}', status='{self.processing_status}')>"
    
    @classmethod
    def create_from_path(cls, session: Session, file_path: str, base_folder: str, **kwargs) -> 'FileInfo':
        """파일 경로로부터 FileInfo를 생성합니다."""
        from pathlib import Path
        
        path_obj = Path(file_path)
        base_folder_obj = Path(base_folder)
        
        # 상대 경로 계산
        try:
            relative_path = str(path_obj.relative_to(base_folder_obj))
        except ValueError:
            relative_path = str(path_obj)
        
        # 폴더명 추출 (Sega_1, Sega_2, Sega_3)
        folder_name = base_folder_obj.name
        
        # 파일 정보 수집
        stat_info = path_obj.stat()
        
        file_info = cls(
            file_path=str(file_path),
            file_name=path_obj.name,
            file_size=stat_info.st_size,
            file_extension=path_obj.suffix.lower(),
            folder_path=str(path_obj.parent),
            folder_name=folder_name,
            relative_path=relative_path,
            scan_date=datetime.utcnow(),
            file_created_date=datetime.fromtimestamp(stat_info.st_ctime),
            file_modified_date=datetime.fromtimestamp(stat_info.st_mtime),
            is_image=path_obj.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'],
            **kwargs
        )
        
        return file_info.save(session)
    
    def mark_processing(self, session: Session) -> 'FileInfo':
        """처리 중으로 표시합니다."""
        self.processing_status = 'processing'
        return self.update(session)
    
    def mark_processed(self, session: Session) -> 'FileInfo':
        """처리 완료로 표시합니다."""
        self.processing_status = 'processed'
        return self.update(session)
    
    def mark_error(self, session: Session, error_message: str = None) -> 'FileInfo':
        """에러 상태로 표시합니다."""
        self.processing_status = 'error'
        if error_message:
            self.extra_data = self.extra_data or {}
            self.extra_data['error_message'] = error_message
        return self.update(session)
    
    def update_image_metadata(self, session: Session, mime_type: str = None, dimensions: str = None, color_space: str = None) -> 'FileInfo':
        """이미지 메타데이터를 업데이트합니다."""
        if mime_type:
            self.mime_type = mime_type
        if dimensions:
            self.image_dimensions = dimensions
        if color_space:
            self.color_space = color_space
        
        return self.update(session)
    
    @classmethod
    def get_new_files(cls, session: Session, limit: Optional[int] = None) -> list:
        """새로 스캔된 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.processing_status == 'new',
            cls.is_deleted == False
        ).order_by(cls.scan_date.asc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_processing_files(cls, session: Session, limit: Optional[int] = None) -> list:
        """처리 중인 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.processing_status == 'processing',
            cls.is_deleted == False
        ).order_by(cls.scan_date.asc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_processed_files(cls, session: Session, limit: Optional[int] = None) -> list:
        """처리 완료된 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.processing_status == 'processed',
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_error_files(cls, session: Session, limit: Optional[int] = None) -> list:
        """에러가 발생한 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.processing_status == 'error',
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_by_folder(cls, session: Session, folder_name: str, limit: Optional[int] = None) -> list:
        """특정 폴더의 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.folder_name == folder_name,
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_by_date_range(cls, session: Session, start_date: datetime, end_date: datetime, limit: Optional[int] = None) -> list:
        """날짜 범위로 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.scan_date >= start_date,
            cls.scan_date <= end_date,
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_image_files(cls, session: Session, limit: Optional[int] = None) -> list:
        """이미지 파일들만 조회합니다."""
        query = session.query(cls).filter(
            cls.is_image == True,
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_by_extension(cls, session: Session, extension: str, limit: Optional[int] = None) -> list:
        """특정 확장자의 파일들을 조회합니다."""
        query = session.query(cls).filter(
            cls.file_extension == extension.lower(),
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def search_by_name(cls, session: Session, search_term: str, limit: Optional[int] = None) -> list:
        """파일명으로 검색합니다."""
        query = session.query(cls).filter(
            cls.file_name.contains(search_term),
            cls.is_deleted == False
        ).order_by(cls.scan_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_file_count_by_status(cls, session: Session) -> dict:
        """상태별 파일 개수를 반환합니다."""
        statuses = ['new', 'processing', 'processed', 'error']
        counts = {}
        
        for status in statuses:
            count = session.query(cls).filter(
                cls.processing_status == status,
                cls.is_deleted == False
            ).count()
            counts[status] = count
        
        return counts
    
    def extract_image_metadata(self, session: Session) -> bool:
        """이미지 파일의 메타데이터를 추출합니다."""
        try:
            if not self.is_image:
                return False
            
            from PIL import Image
            import os
            
            # 파일이 실제로 존재하는지 확인
            if not os.path.exists(self.file_path):
                self.logger.warning(f"파일이 존재하지 않습니다: {self.file_path}")
                return False
            
            # Pillow로 이미지 열기
            with Image.open(self.file_path) as img:
                # 기본 메타데이터
                self.mime_type = img.format
                self.image_dimensions = f"{img.width}x{img.height}"
                self.color_space = img.mode
                
                # 추가 메타데이터
                extra_data = {}
                
                # EXIF 데이터 추출
                if hasattr(img, '_getexif') and img._getexif():
                    exif_data = img._getexif()
                    if exif_data:
                        # EXIF 태그 정보
                        exif_info = {}
                        for tag_id, value in exif_data.items():
                            try:
                                tag_name = Image.TAGS.get(tag_id, f"Unknown_{tag_id}")
                                exif_info[tag_name] = str(value)
                            except:
                                continue
                        extra_data['exif'] = exif_info
                
                # 이미지 통계 정보
                extra_data['statistics'] = {
                    'width': img.width,
                    'height': img.height,
                    'mode': img.mode,
                    'format': img.format,
                    'size': img.size
                }
                
                # 파일 크기 정보
                file_stat = os.stat(self.file_path)
                extra_data['file_info'] = {
                    'size_bytes': file_stat.st_size,
                    'created_time': file_stat.st_ctime,
                    'modified_time': file_stat.st_mtime,
                    'accessed_time': file_stat.st_atime
                }
                
                self.extra_data = extra_data
                
                # 처리 상태 업데이트
                self.processing_status = 'processed'
                self.updated_at = datetime.utcnow()
                
                # 데이터베이스에 저장
                self.update(session)
                
                return True
                
        except Exception as e:
            self.logger.error(f"이미지 메타데이터 추출 실패: {str(e)}")
            self.processing_status = 'error'
            self.error_message = str(e)
            self.updated_at = datetime.utcnow()
            self.update(session)
            return False
    
    def calculate_checksum(self, session: Session, algorithm: str = 'sha256') -> str:
        """파일의 체크섬을 계산합니다."""
        try:
            import hashlib
            import os
            
            if not os.path.exists(self.file_path):
                return ""
            
            hasher = hashlib.new(algorithm)
            with open(self.file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hasher.update(chunk)
            
            checksum = hasher.hexdigest()
            
            # 체크섬을 extra_data에 저장
            if not self.extra_data:
                self.extra_data = {}
            self.extra_data['checksum'] = {
                'algorithm': algorithm,
                'value': checksum
            }
            
            self.update(session)
            return checksum
            
        except Exception as e:
            self.logger.error(f"체크섬 계산 실패: {str(e)}")
            return ""


# 테스트 코드
if __name__ == "__main__":
    print("FileInfo 모델 테스트:")
    
    # 모델 정보 출력
    print(f"테이블명: {FileInfo.__tablename__}")
    print(f"컬럼 수: {len(FileInfo.__table__.columns)}")
    
    # 컬럼 정보 출력
    print("\n컬럼 정보:")
    for column in FileInfo.__table__.columns:
        print(f"  {column.name}: {column.type} (nullable={column.nullable})")
    
    # 메서드 정보 출력
    print(f"\n메서드 수: {len([m for m in dir(FileInfo) if not m.startswith('_')])}")
    
    print("✅ FileInfo 모델 설정 완료!")
