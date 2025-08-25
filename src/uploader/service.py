"""
업로더 서비스
파일 업로드 및 응답 처리 로직을 담당합니다.
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

# 프로젝트 루트를 Python 경로에 추가
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.models.file_info import FileInfo
from src.models.upload_result import UploadResult
from src.services.api_client import APIClient
from src.db.connection import DatabaseManager


class UploaderService:
    """파일 업로드 및 응답 처리 서비스"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.api_client = APIClient()
        self.db_manager = DatabaseManager()
        
        # 재시도 설정 로드
        from config.settings import get_config
        config = get_config()
        api_config = config.get('api', {})
        self.max_retries = api_config.get('retry_attempts', 3)
        self.retry_delay = api_config.get('retry_delay_seconds', 5)
        
        self.logger.info(f"UploaderService 초기화 완료 - 최대 재시도: {self.max_retries}, 재시도 지연: {self.retry_delay}초")
    
    def upload_and_record(self, file_obj: FileInfo) -> bool:
        """
        파일을 업로드하고 결과를 DB에 기록합니다.
        
        Args:
            file_obj: 업로드할 FileInfo 객체
            
        Returns:
            bool: 업로드 성공 여부
        """
        try:
            self.logger.info(f"파일 업로드 시도: {file_obj.file_path}")
            
            # 파일 존재 확인
            if not os.path.exists(file_obj.file_path):
                error_msg = f"파일이 존재하지 않습니다: {file_obj.file_path}"
                self.logger.error(error_msg)
                self._record_upload_failure(file_obj, error_msg)
                return False
            
            # 재시도 루프
            attempt = 0
            while attempt < self.max_retries:
                attempt += 1
                self.logger.info(f"파일 업로드 시도 ({attempt}/{self.max_retries}): {file_obj.file_path}")
                
                try:
                    # API 클라이언트를 통한 파일 업로드
                    upload_result = self.api_client.upload_file(file_obj.file_path)
                    
                    # 업로드 결과 처리
                    if upload_result['success']:
                        return self._handle_upload_success(file_obj, upload_result, attempt)
                    else:
                        # 실패 시 재시도 여부 결정
                        if attempt < self.max_retries:
                            self.logger.warning(f"파일 업로드 실패 ({attempt}/{self.max_retries}): {file_obj.file_name}, 응답: {upload_result.get('error', '알 수 없는 오류')}")
                            self.logger.info(f"재시도 대기 중 ({self.retry_delay}초): {file_obj.file_name}")
                            time.sleep(self.retry_delay)
                            continue
                        else:
                            self.logger.error(f"최대 재시도 횟수 초과, 파일 업로드 최종 실패: {file_obj.file_name}")
                            return self._handle_upload_failure(file_obj, upload_result, attempt)
                except Exception as e:
                    error_msg = f"API 업로드 중 예외 발생: {str(e)}"
                    self.logger.error(f"업로드 시도 {attempt} 실패: {error_msg}")
                    
                    if attempt < self.max_retries:
                        self.logger.info(f"재시도 대기 중 ({self.retry_delay}초): {file_obj.file_name}")
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        self.logger.error(f"최대 재시도 횟수 초과, 파일 업로드 최종 실패: {file_obj.file_name}")
                        return self._record_upload_failure(file_obj, error_msg)
            
            return False
                
        except Exception as e:
            error_msg = f"파일 업로드 중 예상치 못한 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            self._record_upload_failure(file_obj, error_msg)
            return False
    
    def _handle_upload_success(self, file_obj: FileInfo, upload_result: Dict, attempt: int = 1) -> bool:
        """업로드 성공 처리"""
        try:
            self.logger.info(f"파일 업로드 성공: {file_obj.file_name}")
            
            # API 응답 데이터 추출
            api_data = upload_result.get('data', {})
            
            # UploadResult 생성 및 저장
            with self.db_manager.get_session() as session:
                # FileInfo를 새 세션에서 조회하여 업데이트
                file_info = session.query(FileInfo).filter(
                    FileInfo.id == file_obj.id
                ).first()
                
                if file_info:
                    # FileInfo 상태 업데이트
                    file_info.processing_status = 'uploaded'
                    file_info.updated_at = datetime.utcnow()
                    session.add(file_info)
                
                # UploadResult 생성
                upload_record = UploadResult.create_from_file_info(
                    session=session,
                    file_path=file_obj.file_path,
                    folder_name=file_obj.folder_name
                )
                
                # API 응답 정보로 업데이트
                upload_record.update_api_response(session, api_data)
                
                # 재시도 정보 추가
                upload_record.upload_attempts = attempt
                upload_record.last_upload_attempt = datetime.utcnow()
                
                # 세션 커밋
                session.commit()
                
                self.logger.info(f"업로드 결과 DB 저장 완료: {file_obj.file_name}")
                return True
                
        except Exception as e:
            error_msg = f"업로드 성공 처리 중 오류: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            return False
    
    def _handle_upload_failure(self, file_obj: FileInfo, upload_result: Dict, attempt: int = 1) -> bool:
        """업로드 실패 처리"""
        try:
            error_msg = upload_result.get('error', '알 수 없는 오류')
            self.logger.error(f"파일 업로드 실패: {file_obj.file_name} - {error_msg}")
            
            # UploadResult 생성 및 실패 상태 저장
            with self.db_manager.get_session() as session:
                # FileInfo를 새 세션에서 조회하여 업데이트
                file_info = session.query(FileInfo).filter(
                    FileInfo.id == file_obj.id
                ).first()
                
                if file_info:
                    # FileInfo 상태 업데이트
                    file_info.processing_status = 'error'
                    file_info.error_message = error_msg
                    file_info.updated_at = datetime.utcnow()
                    session.add(file_info)
                
                # UploadResult 생성
                upload_record = UploadResult.create_from_file_info(
                    session=session,
                    file_path=file_obj.file_path,
                    folder_name=file_obj.folder_name
                )
                
                # 실패 상태로 표시
                # upload_result에서 datetime 객체를 문자열로 변환
                serializable_result = {}
                for key, value in upload_result.items():
                    if isinstance(value, datetime):
                        serializable_result[key] = value.isoformat()
                    else:
                        serializable_result[key] = value
                
                upload_record.mark_upload_failed(
                    session=session,
                    error_message=error_msg,
                    error_details={
                        'api_response': serializable_result,
                        'status_code': upload_result.get('status_code'),
                        'timestamp': datetime.utcnow().isoformat(),
                        'attempt_number': attempt
                    }
                )
                
                # 세션 커밋
                session.commit()
                
                self.logger.info(f"업로드 실패 결과 DB 저장 완료: {file_obj.file_name}")
                return False
                
        except Exception as e:
            error_msg = f"업로드 실패 처리 중 오류: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            return False
    
    def _record_upload_failure(self, file_obj: FileInfo, error_msg: str):
        """업로드 실패를 DB에 기록합니다."""
        try:
            # UploadResult 생성 및 실패 상태 저장
            with self.db_manager.get_session() as session:
                # FileInfo를 새 세션에서 조회하여 업데이트
                file_info = session.query(FileInfo).filter(
                    FileInfo.id == file_obj.id
                ).first()
                
                if file_info:
                    # FileInfo 상태 업데이트
                    file_info.processing_status = 'error'
                    file_info.error_message = error_msg
                    file_info.updated_at = datetime.utcnow()
                    session.add(file_info)
                
                # UploadResult 생성
                upload_record = UploadResult.create_from_file_info(
                    session=session,
                    file_path=file_obj.file_path,
                    folder_name=file_obj.folder_name
                )
                
                # 실패 상태로 표시
                upload_record.mark_upload_failed(
                    session=session,
                    error_message=error_msg,
                    error_details={
                        'error_type': 'file_not_found',
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )
                
                # 세션 커밋
                session.commit()
                
                self.logger.info(f"업로드 실패 기록 완료: {file_obj.file_name}")
                
        except Exception as e:
            self.logger.error(f"업로드 실패 기록 중 오류: {str(e)}")
    
    def get_pending_uploads(self, limit: Optional[int] = None) -> list:
        """업로드 대기 중인 파일들을 조회합니다."""
        try:
            with self.db_manager.get_session() as session:
                return UploadResult.get_pending_uploads(session, limit)
        except Exception as e:
            self.logger.error(f"업로드 대기 파일 조회 중 오류: {str(e)}")
            return []
    
    def get_failed_uploads(self, limit: Optional[int] = None) -> list:
        """업로드 실패한 파일들을 조회합니다."""
        try:
            with self.db_manager.get_session() as session:
                return UploadResult.get_failed_uploads(session, limit)
        except Exception as e:
            self.logger.error(f"업로드 실패 파일 조회 중 오류: {str(e)}")
            return []
    
    def retry_failed_upload(self, upload_result: UploadResult) -> bool:
        """실패한 업로드를 재시도합니다."""
        try:
            self.logger.info(f"실패한 업로드 재시도: {upload_result.file_name}")
            
            # FileInfo 조회
            with self.db_manager.get_session() as session:
                file_info = session.query(FileInfo).filter(
                    FileInfo.file_path == upload_result.file_path
                ).first()
                
                if not file_info:
                    self.logger.error(f"FileInfo를 찾을 수 없습니다: {upload_result.file_path}")
                    return False
                
                # 재시도를 위해 상태 초기화
                upload_result.reset_for_retry(session)
                
                # 업로드 재시도
                return self.upload_and_record(file_info)
                
        except Exception as e:
            error_msg = f"업로드 재시도 중 오류: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            return False


# 전역 인스턴스
uploader_service = UploaderService()


# 테스트 코드
if __name__ == "__main__":
    print("UploaderService 테스트:")
    
    # 서비스 인스턴스 생성
    service = UploaderService()
    print(f"✅ UploaderService 인스턴스 생성 성공")
    
    # API 클라이언트 정보
    print(f"\nAPI 클라이언트 정보:")
    api_info = service.api_client.get_api_info()
    for key, value in api_info.items():
        print(f"  {key}: {value}")
    
    # 대기 중인 업로드 조회
    print(f"\n대기 중인 업로드:")
    pending_uploads = service.get_pending_uploads(limit=5)
    print(f"  총 {len(pending_uploads)}개 파일")
    
    # 실패한 업로드 조회
    print(f"\n실패한 업로드:")
    failed_uploads = service.get_failed_uploads(limit=5)
    print(f"  총 {len(failed_uploads)}개 파일")
    
    print("\n✅ UploaderService 테스트 완료!")
