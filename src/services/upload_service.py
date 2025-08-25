"""
업로드 서비스
API 클라이언트와 파일 처리 시스템을 연동하여 파일 업로드를 관리합니다.
"""

import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Callable, Union
from queue import Queue, Empty
from dataclasses import dataclass
from enum import Enum

# 프로젝트 루트를 Python 경로에 추가
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import LoggerManager
from src.services.api_client import APIClient
from src.models.upload_result import UploadResult
from src.db.connection import DatabaseManager
from config.settings import get_config


class UploadStatus(Enum):
    """업로드 상태"""
    PENDING = "pending"
    UPLOADING = "uploading"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"


@dataclass
class UploadTask:
    """업로드 작업 정보"""
    file_path: Path
    file_info_id: int
    upload_result_id: int
    status: UploadStatus
    priority: int = 0
    created_at: datetime = None
    updated_at: datetime = None
    started_at: datetime = None
    completed_at: datetime = None
    retry_count: int = 0
    max_retries: int = 3
    error_message: str = None
    api_response: Dict = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
    
    def update_status(self, status: UploadStatus, **kwargs):
        """상태를 업데이트합니다."""
        self.status = status
        self.updated_at = datetime.now()
        
        if status == UploadStatus.UPLOADING and self.started_at is None:
            self.started_at = datetime.now()
        elif status in [UploadStatus.SUCCESS, UploadStatus.FAILED, UploadStatus.CANCELLED]:
            self.completed_at = datetime.now()
        
        # 추가 필드 업데이트
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def increment_retry(self):
        """재시도 횟수를 증가시킵니다."""
        self.retry_count += 1
        self.updated_at = datetime.now()
    
    def can_retry(self) -> bool:
        """재시도 가능한지 확인합니다."""
        return self.retry_count < self.max_retries


class UploadService:
    """파일 업로드 서비스"""
    
    def __init__(self):
        # 설정 로드
        self.config = get_config()
        
        # 로거 설정
        self.logger_manager = LoggerManager()
        self.logger = self.logger_manager.get_logger(__name__)
        
        # API 클라이언트
        self.api_client = APIClient()
        
        # 데이터베이스 매니저
        self.db_manager = DatabaseManager()
        
        # 업로드 설정
        self.upload_config = self.config.get('upload', {})
        self.max_concurrent_uploads = self.upload_config.get('max_concurrent_uploads', 5)
        self.upload_timeout = self.upload_config.get('timeout_seconds', 60)
        self.retry_delay = self.upload_config.get('retry_delay_seconds', 10)
        
        # 업로드 큐
        self.upload_queue = Queue()
        self.priority_queue = Queue()
        
        # 업로드 중인 작업들
        self.active_uploads: Dict[str, UploadTask] = {}
        
        # 스레드 락
        self.upload_lock = threading.Lock()
        
        # 작업자 스레드들
        self.upload_threads: List[threading.Thread] = []
        
        # 시스템 상태
        self.is_running = False
        self.shutdown_event = threading.Event()
        
        # 통계 정보
        self.stats = {
            'total_uploads': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'retry_uploads': 0,
            'pending_uploads': 0
        }
        
        # 콜백 함수들
        self.on_upload_started_callback: Optional[Callable[[UploadTask], None]] = None
        self.on_upload_completed_callback: Optional[Callable[[UploadTask, Dict], None]] = None
        self.on_upload_failed_callback: Optional[Callable[[UploadTask, str], None]] = None
        self.on_upload_retry_callback: Optional[Callable[[UploadTask, str], None]] = None
        
        # 초기화
        self._start_worker_threads()
    
    def _start_worker_threads(self):
        """업로드 작업자 스레드들을 시작합니다."""
        for i in range(self.max_concurrent_uploads):
            thread = threading.Thread(
                target=self._upload_worker,
                name=f"UploadWorker-{i}",
                daemon=True
            )
            thread.start()
            self.upload_threads.append(thread)
        
        self.logger.info(f"업로드 작업자 스레드 시작 완료: {len(self.upload_threads)}개")
    
    def _upload_worker(self):
        """업로드 작업자 스레드"""
        while not self.shutdown_event.is_set():
            try:
                # 우선순위 큐에서 먼저 작업 가져오기
                try:
                    upload_task = self.priority_queue.get(timeout=1)
                except Empty:
                    # 일반 큐에서 작업 가져오기
                    try:
                        upload_task = self.upload_queue.get(timeout=1)
                    except Empty:
                        continue
                
                # 업로드 수행
                self._process_upload(upload_task)
                
                # 작업 완료 표시
                self.upload_queue.task_done()
                self.priority_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"업로드 작업자 오류: {str(e)}")
                self.logger.exception("상세 에러 정보:")
    
    def add_upload_task(self, file_path: Union[str, Path], 
                        file_info_id: int,
                        upload_result_id: int,
                        priority: int = 0) -> str:
        """업로드 작업을 큐에 추가합니다."""
        try:
            file_path = Path(file_path)
            
            # 업로드 작업 생성
            upload_task = UploadTask(
                file_path=file_path,
                file_info_id=file_info_id,
                upload_result_id=upload_result_id,
                status=UploadStatus.PENDING,
                priority=priority
            )
            
            # 우선순위에 따라 큐 선택
            if priority > 0:
                self.priority_queue.put(upload_task)
                self.logger.info(f"우선순위 업로드 작업 추가: {file_path} (우선순위: {priority})")
            else:
                self.upload_queue.put(upload_task)
                self.logger.info(f"업로드 작업 추가: {file_path}")
            
            # 통계 업데이트
            with self.upload_lock:
                self.stats['total_uploads'] += 1
                self.stats['pending_uploads'] += 1
            
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"업로드 작업 추가 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def _process_upload(self, upload_task: UploadTask):
        """업로드 작업을 처리합니다."""
        try:
            file_path = upload_task.file_path
            
            self.logger.info(f"업로드 시작: {file_path}")
            
            # 상태를 업로드 중으로 업데이트
            upload_task.update_status(UploadStatus.UPLOADING)
            
            # 활성 업로드에 추가
            with self.upload_lock:
                self.active_uploads[str(file_path)] = upload_task
                self.stats['pending_uploads'] -= 1
            
            # 시작 콜백 호출
            if self.on_upload_started_callback:
                self.on_upload_started_callback(upload_task)
            
            # API 클라이언트로 파일 업로드
            upload_result = self.api_client.upload_file(file_path)
            
            if upload_result['success']:
                # 업로드 성공
                self._handle_upload_success(upload_task, upload_result)
            else:
                # 업로드 실패
                self._handle_upload_failure(upload_task, upload_result)
            
        except Exception as e:
            error_msg = f"업로드 처리 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            
            upload_task.update_status(UploadStatus.FAILED, error_message=error_msg)
            self._handle_upload_failure(upload_task, {'error': error_msg})
        
        finally:
            # 활성 업로드에서 제거
            with self.upload_lock:
                self.active_uploads.pop(str(file_path), None)
    
    def _handle_upload_success(self, upload_task: UploadTask, upload_result: Dict):
        """업로드 성공을 처리합니다."""
        try:
            file_path = upload_task.file_path
            
            self.logger.info(f"업로드 성공: {file_path}")
            
            # 상태 업데이트
            upload_task.update_status(
                UploadStatus.SUCCESS,
                api_response=upload_result['data']
            )
            
            # 데이터베이스 업데이트
            self._update_upload_result(upload_task, upload_result)
            
            # 통계 업데이트
            with self.upload_lock:
                self.stats['successful_uploads'] += 1
            
            # 성공 콜백 호출
            if self.on_upload_completed_callback:
                self.on_upload_completed_callback(upload_task, upload_result['data'])
            
        except Exception as e:
            self.logger.error(f"업로드 성공 처리 중 오류: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _handle_upload_failure(self, upload_task: UploadTask, upload_result: Dict):
        """업로드 실패를 처리합니다."""
        try:
            file_path = upload_task.file_path
            error_msg = upload_result.get('error', '알 수 없는 오류')
            
            self.logger.error(f"업로드 실패: {file_path} - {error_msg}")
            
            # 재시도 가능한지 확인
            if upload_task.can_retry():
                self._handle_upload_retry(upload_task, error_msg)
            else:
                # 최종 실패로 처리
                upload_task.update_status(UploadStatus.FAILED, error_message=error_msg)
                
                # 데이터베이스 업데이트
                self._update_upload_result(upload_task, upload_result)
                
                # 통계 업데이트
                with self.upload_lock:
                    self.stats['failed_uploads'] += 1
                
                                # 실패 콜백 호출
                if self.on_upload_failed_callback:
                    self.on_upload_failed_callback(upload_task, error_msg)
                
                # 데이터베이스 업데이트
                self._update_upload_result(upload_task, upload_result)
                
                # 통계 업데이트
                with self.upload_lock:
                    self.stats['failed_uploads'] += 1
                
                # 실패 콜백 호출
                if self.on_upload_failed_callback:
                    self.on_upload_failed_callback(upload_task, error_msg)
            
        except Exception as e:
            self.logger.error(f"업로드 실패 처리 중 오류: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _handle_upload_retry(self, upload_task: UploadTask, error_msg: str):
        """업로드 재시도를 처리합니다."""
        try:
            file_path = upload_task.file_path
            
            # 재시도 횟수 증가
            upload_task.increment_retry()
            
            self.logger.info(f"업로드 재시도: {file_path} (재시도 {upload_task.retry_count}/{upload_task.max_retries})")
            
            # 상태를 재시도로 업데이트
            upload_task.update_status(UploadStatus.RETRY, error_message=error_msg)
            
            # 통계 업데이트
            with self.upload_lock:
                self.stats['retry_uploads'] += 1
            
            # 재시도 콜백 호출
            if self.on_upload_retry_callback:
                self.on_upload_retry_callback(upload_task, error_msg)
            
            # 재시도 지연 후 큐에 다시 추가
            time.sleep(self.retry_delay)
            
            if upload_task.priority > 0:
                self.priority_queue.put(upload_task)
            else:
                self.upload_queue.put(upload_task)
            
            self.logger.info(f"업로드 작업 재큐 추가: {file_path}")
            
        except Exception as e:
            self.logger.error(f"업로드 재시도 처리 중 오류: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _update_upload_result(self, upload_task: UploadTask, upload_result: Dict):
        """업로드 결과를 데이터베이스에 업데이트합니다."""
        try:
            session = self.db_manager.get_session()
            
            try:
                # 업로드 결과 조회
                upload_result_record = session.query(UploadResult).filter(
                    UploadResult.id == upload_task.upload_result_id
                ).first()
                
                if upload_result_record:
                    if upload_task.status == UploadStatus.SUCCESS:
                        # 성공 시 API 응답 업데이트
                        upload_result_record.update_api_response(
                            session,
                            upload_task.api_response
                        )
                    else:
                        # 실패 시 오류 정보 업데이트
                        upload_result_record.mark_upload_failed(
                            session,
                            upload_task.error_message
                        )
                    
                    self.logger.debug(f"업로드 결과 데이터베이스 업데이트 완료: {upload_task.upload_result_id}")
                else:
                    self.logger.warning(f"업로드 결과를 찾을 수 없습니다: {upload_task.upload_result_id}")
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"업로드 결과 데이터베이스 업데이트 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def get_upload_status(self, file_path: str) -> Optional[Dict]:
        """특정 파일의 업로드 상태를 반환합니다."""
        # 활성 업로드에서 찾기
        if file_path in self.active_uploads:
            task = self.active_uploads[file_path]
            return {
                'status': task.status.value,
                'priority': task.priority,
                'created_at': task.created_at.isoformat(),
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'retry_count': task.retry_count,
                'error_message': task.error_message
            }
        
        return None
    
    def get_queue_status(self) -> Dict:
        """업로드 큐 상태 정보를 반환합니다."""
        return {
            'upload_queue_size': self.upload_queue.qsize(),
            'priority_queue_size': self.priority_queue.qsize(),
            'active_uploads_count': len(self.active_uploads),
            'is_running': self.is_running,
            'stats': self.stats.copy()
        }
    
    def get_active_uploads(self) -> List[Dict]:
        """현재 활성 업로드 목록을 반환합니다."""
        active_uploads = []
        
        with self.upload_lock:
            for file_path, task in self.active_uploads.items():
                active_uploads.append({
                    'file_path': file_path,
                    'status': task.status.value,
                    'priority': task.priority,
                    'created_at': task.created_at.isoformat(),
                    'started_at': task.started_at.isoformat() if task.started_at else None,
                    'retry_count': task.retry_count,
                    'error_message': task.error_message
                })
        
        return active_uploads
    
    def cancel_upload(self, file_path: str) -> bool:
        """업로드를 취소합니다."""
        try:
            with self.upload_lock:
                if file_path in self.active_uploads:
                    task = self.active_uploads[file_path]
                    task.update_status(UploadStatus.CANCELLED)
                    
                    # 활성 업로드에서 제거
                    self.active_uploads.pop(file_path)
                    
                    self.logger.info(f"업로드 취소: {file_path}")
                    return True
                else:
                    self.logger.warning(f"취소할 업로드를 찾을 수 없습니다: {file_path}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"업로드 취소 중 오류: {str(e)}")
            return False
    
    def set_callbacks(self,
                     on_upload_started: Optional[Callable[[UploadTask], None]] = None,
                     on_upload_completed: Optional[Callable[[UploadTask, Dict], None]] = None,
                     on_upload_failed: Optional[Callable[[UploadTask, str], None]] = None,
                     on_upload_retry: Optional[Callable[[UploadTask, str], None]] = None):
        """콜백 함수들을 설정합니다."""
        if on_upload_started:
            self.on_upload_started_callback = on_upload_started
        if on_upload_completed:
            self.on_upload_completed_callback = on_upload_completed
        if on_upload_failed:
            self.on_upload_failed_callback = on_upload_failed
        if on_upload_retry:
            self.on_upload_retry_callback = on_upload_retry
        
        self.logger.info("업로드 서비스 콜백 함수 설정 완료")
    
    def start(self):
        """업로드 서비스를 시작합니다."""
        if self.is_running:
            self.logger.warning("업로드 서비스가 이미 실행 중입니다.")
            return
        
        self.is_running = True
        self.shutdown_event.clear()
        self.logger.info("업로드 서비스 시작")
    
    def stop(self):
        """업로드 서비스를 중지합니다."""
        if not self.is_running:
            self.logger.warning("업로드 서비스가 실행 중이 아닙니다.")
            return
        
        self.logger.info("업로드 서비스 중지")
        self.shutdown_event.set()
        self.is_running = False
        
        # 모든 큐 작업 완료 대기
        self.upload_queue.join()
        self.priority_queue.join()
        
        self.logger.info("업로드 서비스 중지 완료")
    
    def cleanup(self):
        """리소스를 정리합니다."""
        try:
            # API 클라이언트 정리
            self.api_client.close()
            
            self.logger.info("업로드 서비스 리소스 정리 완료")
            
        except Exception as e:
            self.logger.error(f"업로드 서비스 리소스 정리 실패: {str(e)}")


# 테스트 코드
if __name__ == "__main__":
    print("UploadService 테스트:")
    
    try:
        # 업로드 서비스 생성
        upload_service = UploadService()
        
        # 상태 출력
        status = upload_service.get_queue_status()
        print(f"큐 상태: {status}")
        
        print("✅ UploadService 설정 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
