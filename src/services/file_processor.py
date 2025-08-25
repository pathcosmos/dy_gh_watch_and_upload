"""
파일 처리 및 업로드 큐 관리 시스템
새로 감지된 파일을 처리하고 업로드 큐에 추가합니다.
"""

import os
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Callable, Tuple
from queue import Queue, Empty
from dataclasses import dataclass
from enum import Enum

# 프로젝트 루트를 Python 경로에 추가
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import LoggerManager
from src.models.file_info import FileInfo
from src.models.upload_result import UploadResult
from src.db.connection import DatabaseManager
from config.settings import get_config


class FileStatus(Enum):
    """파일 처리 상태"""
    PENDING = "pending"
    PROCESSING = "processing"
    READY_FOR_UPLOAD = "ready_for_upload"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class FileTask:
    """파일 처리 작업 정보"""
    file_path: Path
    file_info: FileInfo
    status: FileStatus
    priority: int = 0
    created_at: datetime = None
    updated_at: datetime = None
    error_message: str = None
    retry_count: int = 0
    max_retries: int = 3
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
    
    def update_status(self, status: FileStatus, error_message: str = None):
        """상태를 업데이트합니다."""
        self.status = status
        self.updated_at = datetime.now()
        if error_message:
            self.error_message = error_message
    
    def increment_retry(self):
        """재시도 횟수를 증가시킵니다."""
        self.retry_count += 1
        self.updated_at = datetime.now()
    
    def can_retry(self) -> bool:
        """재시도 가능한지 확인합니다."""
        return self.retry_count < self.max_retries


class FileProcessor:
    """파일 처리 및 업로드 큐 관리 시스템"""
    
    def __init__(self):
        # 설정 로드
        self.config = get_config()
        
        # 로거 설정
        self.logger_manager = LoggerManager()
        self.logger = self.logger_manager.get_logger(__name__)
        
        # 데이터베이스 매니저
        self.db_manager = DatabaseManager()
        
        # 파일 처리 설정
        self.processing_config = self.config.get('file_processing', {})
        self.max_concurrent_uploads = self.processing_config.get('max_concurrent_uploads', 5)
        self.chunk_size = self.processing_config.get('chunk_size', 8192)
        self.temp_directory = self.processing_config.get('temp_directory', '/tmp/file_monitor')
        self.cleanup_temp_files = self.processing_config.get('cleanup_temp_files', True)
        
        # 업로드 큐
        self.upload_queue = Queue()
        self.processing_queue = Queue()
        
        # 처리 중인 파일들
        self.processing_files: Dict[str, FileTask] = {}
        self.uploading_files: Dict[str, FileTask] = {}
        
        # 스레드 락
        self.processing_lock = threading.Lock()
        self.uploading_lock = threading.Lock()
        
        # 작업자 스레드들
        self.processing_threads: List[threading.Thread] = []
        self.upload_threads: List[threading.Thread] = []
        
        # 시스템 상태
        self.is_running = False
        self.shutdown_event = threading.Event()
        
        # 콜백 함수들
        self.on_file_processed_callback: Optional[Callable[[FileTask], None]] = None
        self.on_file_ready_for_upload_callback: Optional[Callable[[FileTask], None]] = None
        self.on_file_uploaded_callback: Optional[Callable[[FileTask, Dict], None]] = None
        self.on_file_failed_callback: Optional[Callable[[FileTask, str], None]] = None
        
        # 초기화
        self._setup_temp_directory()
        self._start_worker_threads()
    
    def _setup_temp_directory(self):
        """임시 디렉토리를 설정합니다."""
        try:
            temp_path = Path(self.temp_directory)
            temp_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"임시 디렉토리 설정 완료: {temp_path}")
        except Exception as e:
            self.logger.error(f"임시 디렉토리 설정 실패: {str(e)}")
            # 기본 임시 디렉토리 사용
            self.temp_directory = '/tmp'
    
    def _start_worker_threads(self):
        """작업자 스레드들을 시작합니다."""
        # 파일 처리 스레드들
        for i in range(self.max_concurrent_uploads):
            thread = threading.Thread(
                target=self._processing_worker,
                name=f"FileProcessor-{i}",
                daemon=True
            )
            thread.start()
            self.processing_threads.append(thread)
        
        # 업로드 스레드들
        for i in range(self.max_concurrent_uploads):
            thread = threading.Thread(
                target=self._upload_worker,
                name=f"UploadWorker-{i}",
                daemon=True
            )
            thread.start()
            self.upload_threads.append(thread)
        
        self.logger.info(f"작업자 스레드 시작 완료: 처리 {len(self.processing_threads)}개, 업로드 {len(self.upload_threads)}개")
    
    def _processing_worker(self):
        """파일 처리 작업자 스레드"""
        while not self.shutdown_event.is_set():
            try:
                # 처리 큐에서 작업 가져오기
                try:
                    file_task = self.processing_queue.get(timeout=1)
                except Empty:
                    continue
                
                # 파일 처리
                self._process_file(file_task)
                
                # 작업 완료 표시
                self.processing_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"파일 처리 작업자 오류: {str(e)}")
                self.logger.exception("상세 에러 정보:")
    
    def _upload_worker(self):
        """업로드 작업자 스레드"""
        while not self.shutdown_event.is_set():
            try:
                # 업로드 큐에서 작업 가져오기
                try:
                    file_task = self.upload_queue.get(timeout=1)
                except Empty:
                    continue
                
                # 파일 업로드
                self._upload_file(file_task)
                
                # 작업 완료 표시
                self.upload_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"업로드 작업자 오류: {str(e)}")
                self.logger.exception("상세 에러 정보:")
    
    def add_file(self, file_path: Path, priority: int = 0) -> str:
        """새 파일을 처리 큐에 추가합니다."""
        try:
            # 파일 정보 조회 또는 생성
            file_info = self._get_or_create_file_info(file_path)
            
            # 파일 작업 생성
            file_task = FileTask(
                file_path=file_path,
                file_info=file_info,
                status=FileStatus.PENDING,
                priority=priority
            )
            
            # 처리 큐에 추가
            self.processing_queue.put(file_task)
            
            # 처리 중인 파일로 표시
            with self.processing_lock:
                self.processing_files[str(file_path)] = file_task
            
            self.logger.info(f"파일 처리 큐에 추가: {file_path} (우선순위: {priority})")
            
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"파일 큐 추가 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def _get_or_create_file_info(self, file_path: Path) -> FileInfo:
        """파일 정보를 조회하거나 생성합니다."""
        session = self.db_manager.get_session()
        
        try:
            # 기존 파일 정보 조회
            existing_file = session.query(FileInfo).filter(
                FileInfo.file_path == str(file_path)
            ).first()
            
            if existing_file:
                return existing_file
            
            # 새 파일 정보 생성
            # 기본 폴더 찾기 (테스트 환경에서는 임시 폴더 사용)
            base_folder = self._find_base_folder(file_path)
            if not base_folder:
                base_folder = Path(self.temp_directory)
            
            file_info = FileInfo.create_from_path(
                session=session,
                file_path=str(file_path),
                base_folder=str(base_folder)
            )
            
            return file_info
            
        finally:
            session.close()
    
    def _find_base_folder(self, file_path: Path) -> Optional[Path]:
        """파일이 속한 기본 폴더를 찾습니다."""
        # 설정에서 모니터링 폴더들 가져오기
        monitor_config = self.config.get('monitor', {})
        base_folders = monitor_config.get('base_folders', [])
        
        for base_folder in base_folders:
            base_path = Path(base_folder)
            try:
                if file_path.resolve().is_relative_to(base_path.resolve()):
                    return base_path
            except ValueError:
                continue
        
        return None
    
    def _process_file(self, file_task: FileTask):
        """파일을 처리합니다."""
        try:
            file_path = file_task.file_path
            file_info = file_task.file_info
            
            self.logger.info(f"파일 처리 시작: {file_path}")
            
            # 상태를 처리 중으로 업데이트
            file_task.update_status(FileStatus.PROCESSING)
            file_info.mark_processing(self.db_manager.get_session())
            
            # 파일 유효성 검사
            if not self._validate_file(file_path):
                file_task.update_status(FileStatus.FAILED, "파일 유효성 검사 실패")
                file_info.mark_error(self.db_manager.get_session(), "파일 유효성 검사 실패")
                return
            
            # 파일 메타데이터 수집
            if file_info.is_image:
                # 이미지 메타데이터 추출
                success = file_info.extract_image_metadata(self.db_manager.get_session())
                if success:
                    self.logger.info(f"이미지 메타데이터 추출 완료: {file_path}")
                else:
                    self.logger.warning(f"이미지 메타데이터 추출 실패: {file_path}")
            
            # 파일 체크섬 계산
            checksum = file_info.calculate_checksum(self.db_manager.get_session())
            if checksum:
                self.logger.info(f"파일 체크섬 계산 완료: {file_path} -> {checksum[:8]}...")
            
            # 파일 처리 완료
            file_task.update_status(FileStatus.COMPLETED)
            file_info.mark_processed(self.db_manager.get_session())
            
            self.logger.info(f"파일 처리 완료: {file_path}")
            
            # 콜백 호출
            if self.on_file_processed:
                self.on_file_processed(file_task)
            
            # 업로드 준비 완료로 표시
            if self.on_file_ready_for_upload:
                self.on_file_ready_for_upload(file_task)
            
            # 업로드 큐에 추가
            self.upload_queue.put(file_task)
            
            # 업로드 중인 파일로 표시
            with self.uploading_lock:
                self.uploading_files[str(file_path)] = file_task
            
            self.logger.info(f"파일 업로드 큐에 추가: {file_path}")
            
        except Exception as e:
            error_msg = f"파일 처리 실패: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            
            file_task.update_status(FileStatus.FAILED, error_msg)
            file_info.mark_error(self.db_manager.get_session(), error_msg)
            
            # 실패 콜백 호출
            if self.on_file_failed_callback:
                self.on_file_failed_callback(file_task, error_msg)
    
    def _validate_file(self, file_path: Path) -> bool:
        """파일 유효성을 검사합니다."""
        try:
            # 파일 존재 확인
            if not file_path.exists():
                return False
            
            # 파일 크기 확인
            file_size = file_path.stat().st_size
            max_size = self.config.get('monitor', {}).get('max_file_size', 100 * 1024 * 1024)
            if file_size > max_size:
                self.logger.warning(f"파일 크기가 너무 큽니다: {file_path} ({file_size} bytes)")
                return False
            
            # 파일 확장자 확인
            allowed_extensions = self.config.get('monitor', {}).get('image_extensions', ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'])
            if file_path.suffix.lower() not in allowed_extensions:
                self.logger.warning(f"지원하지 않는 파일 확장자: {file_path}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"파일 유효성 검사 실패: {str(e)}")
            return False
    
    def _extract_file_metadata(self, file_path: Path) -> Optional[Dict]:
        """파일 메타데이터를 추출합니다."""
        try:
            # 기본 파일 정보
            metadata = {}
            
            # MIME 타입 추정
            extension = file_path.suffix.lower()
            mime_types = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.bmp': 'image/bmp',
                '.tiff': 'image/tiff'
            }
            metadata['mime_type'] = mime_types.get(extension, 'application/octet-stream')
            
            # 이미지 크기 정보 (Pillow 사용)
            try:
                from PIL import Image
                with Image.open(file_path) as img:
                    metadata['dimensions'] = f"{img.width}x{img.height}"
                    metadata['color_space'] = img.mode
            except ImportError:
                self.logger.debug("Pillow가 설치되지 않아 이미지 메타데이터를 추출할 수 없습니다.")
            except Exception as e:
                self.logger.debug(f"이미지 메타데이터 추출 실패: {str(e)}")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"파일 메타데이터 추출 실패: {str(e)}")
            return None
    
    def _upload_file(self, file_task: FileTask):
        """파일을 업로드합니다."""
        try:
            file_path = file_task.file_path
            file_info = file_task.file_info
            
            self.logger.info(f"파일 업로드 시작: {file_path}")
            
            # 상태를 업로드 중으로 업데이트
            file_task.update_status(FileStatus.UPLOADING)
            
            # 업로드 결과 생성
            upload_result = self._create_upload_result(file_task)
            
            # 실제 업로드 수행 (여기서는 시뮬레이션)
            upload_response = self._perform_upload(file_path, upload_result)
            
            if upload_response['success']:
                # 업로드 성공
                file_task.update_status(FileStatus.UPLOADED)
                upload_result.update_api_response(
                    self.db_manager.get_session(),
                    upload_response['data']
                )
                
                self.logger.info(f"파일 업로드 성공: {file_path}")
                
                # 성공 콜백 호출
                if self.on_file_uploaded_callback:
                    self.on_file_uploaded_callback(file_task, upload_response['data'])
                
            else:
                # 업로드 실패
                error_msg = upload_response.get('error', '알 수 없는 오류')
                file_task.update_status(FileStatus.FAILED, error_msg)
                upload_result.mark_upload_failed(
                    self.db_manager.get_session(),
                    error_msg
                )
                
                self.logger.error(f"파일 업로드 실패: {file_path} - {error_msg}")
                
                # 실패 콜백 호출
                if self.on_file_failed_callback:
                    self.on_file_failed_callback(file_task, error_msg)
                
                # 재시도 가능한 경우 처리 큐에 다시 추가
                if file_task.can_retry():
                    file_task.increment_retry()
                    file_task.update_status(FileStatus.PENDING)
                    self.processing_queue.put(file_task)
                    self.logger.info(f"파일 재처리 큐에 추가: {file_path} (재시도 {file_task.retry_count}/{file_task.max_retries})")
            
        except Exception as e:
            error_msg = f"파일 업로드 처리 실패: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            
            file_task.update_status(FileStatus.FAILED, error_msg)
            
            # 실패 콜백 호출
            if self.on_file_failed_callback:
                self.on_file_failed_callback(file_task, error_msg)
    
    def _create_upload_result(self, file_task: FileTask) -> UploadResult:
        """업로드 결과 레코드를 생성합니다."""
        session = self.db_manager.get_session()
        
        try:
            # 기존 업로드 결과 조회
            existing_result = session.query(UploadResult).filter(
                UploadResult.file_path == str(file_task.file_path)
            ).first()
            
            if existing_result:
                return existing_result
            
            # 새 업로드 결과 생성
            upload_result = UploadResult.create_from_file_info(
                session=session,
                file_path=str(file_task.file_path),
                file_name=file_task.file_path.name,
                file_size=file_task.file_path.stat().st_size,
                file_extension=file_task.file_path.suffix.lower(),
                folder_name=file_task.file_info.folder_name,
                scan_date=datetime.now()
            )
            
            return upload_result
            
        finally:
            session.close()
    
    def _perform_upload(self, file_path: Path, upload_result: UploadResult) -> Dict:
        """실제 파일 업로드를 수행합니다."""
        try:
            # 여기서는 시뮬레이션으로 성공 응답 반환
            # 실제 구현에서는 API 클라이언트를 사용하여 업로드
            
            # 파일 크기 확인
            file_size = file_path.stat().st_size
            
            # 시뮬레이션된 API 응답
            response_data = {
                'api_file_id': f"file_{int(time.time())}_{file_path.stem}",
                'api_filename': file_path.name,
                'api_file_size': file_size,
                'api_upload_time': datetime.now(),
                'download_url': f"http://example.com/download/{file_path.name}",
                'view_url': f"http://example.com/view/{file_path.name}",
                'api_message': 'Upload successful'
            }
            
            # 성공 응답
            return {
                'success': True,
                'data': response_data
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_queue_status(self) -> Dict:
        """큐 상태 정보를 반환합니다."""
        return {
            'processing_queue_size': self.processing_queue.qsize(),
            'upload_queue_size': self.upload_queue.qsize(),
            'processing_files_count': len(self.processing_files),
            'uploading_files_count': len(self.uploading_files),
            'is_running': self.is_running
        }
    
    def get_file_status(self, file_path: str) -> Optional[Dict]:
        """특정 파일의 상태 정보를 반환합니다."""
        # 처리 중인 파일에서 찾기
        if file_path in self.processing_files:
            task = self.processing_files[file_path]
            return {
                'status': task.status.value,
                'priority': task.priority,
                'created_at': task.created_at.isoformat(),
                'updated_at': task.updated_at.isoformat(),
                'retry_count': task.retry_count,
                'error_message': task.error_message
            }
        
        # 업로드 중인 파일에서 찾기
        if file_path in self.uploading_files:
            task = self.uploading_files[file_path]
            return {
                'status': task.status.value,
                'priority': task.priority,
                'created_at': task.created_at.isoformat(),
                'updated_at': task.updated_at.isoformat(),
                'retry_count': task.retry_count,
                'error_message': task.error_message
            }
        
        return None
    
    def set_callbacks(self, 
                     on_file_processed: Optional[Callable[[FileTask], None]] = None,
                     on_file_ready_for_upload: Optional[Callable[[FileTask], None]] = None,
                     on_file_uploaded: Optional[Callable[[FileTask, Dict], None]] = None,
                     on_file_failed: Optional[Callable[[FileTask, str], None]] = None):
        """콜백 함수들을 설정합니다."""
        if on_file_processed:
            self.on_file_processed_callback = on_file_processed
        if on_file_ready_for_upload:
            self.on_file_ready_for_upload_callback = on_file_ready_for_upload
        if on_file_uploaded:
            self.on_file_uploaded_callback = on_file_uploaded
        if on_file_failed:
            self.on_file_failed_callback = on_file_failed
        
        self.logger.info("콜백 함수 설정 완료")
    
    def start(self):
        """파일 처리 시스템을 시작합니다."""
        if self.is_running:
            self.logger.warning("파일 처리 시스템이 이미 실행 중입니다.")
            return
        
        self.is_running = True
        self.shutdown_event.clear()
        self.logger.info("파일 처리 시스템 시작")
    
    def stop(self):
        """파일 처리 시스템을 중지합니다."""
        if not self.is_running:
            self.logger.warning("파일 처리 시스템이 실행 중이 아닙니다.")
            return
        
        self.logger.info("파일 처리 시스템 중지")
        self.shutdown_event.set()
        self.is_running = False
        
        # 모든 큐 작업 완료 대기
        self.processing_queue.join()
        self.upload_queue.join()
        
        self.logger.info("파일 처리 시스템 중지 완료")
    
    def cleanup(self):
        """리소스를 정리합니다."""
        try:
            # 임시 파일 정리
            if self.cleanup_temp_files:
                temp_path = Path(self.temp_directory)
                if temp_path.exists():
                    for temp_file in temp_path.glob('*'):
                        try:
                            if temp_file.is_file():
                                temp_file.unlink()
                        except Exception as e:
                            self.logger.warning(f"임시 파일 삭제 실패: {temp_file} - {str(e)}")
            
            self.logger.info("리소스 정리 완료")
            
        except Exception as e:
            self.logger.error(f"리소스 정리 실패: {str(e)}")


# 테스트 코드
if __name__ == "__main__":
    print("FileProcessor 테스트:")
    
    try:
        # 파일 처리 시스템 생성
        processor = FileProcessor()
        
        # 상태 출력
        status = processor.get_queue_status()
        print(f"큐 상태: {status}")
        
        print("✅ FileProcessor 설정 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
