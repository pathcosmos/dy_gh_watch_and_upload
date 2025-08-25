"""
파일 시스템 모니터링 서비스
지정된 폴더들을 모니터링하여 새로운 이미지 파일을 탐지합니다.
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Callable
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent, FileDeletedEvent

# 프로젝트 루트를 Python 경로에 추가
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import LoggerManager
from src.models.file_info import FileInfo
from src.db.connection import DatabaseManager
from config.settings import get_config


class FileChangeHandler(FileSystemEventHandler):
    """파일 시스템 변경 이벤트를 처리하는 핸들러"""
    
    def __init__(self, monitor_service: 'FileMonitorService'):
        self.monitor_service = monitor_service
        self.logger = monitor_service.logger
        
        # 처리할 파일 확장자
        self.image_extensions = monitor_service.config.get('monitor', {}).get('image_extensions', ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'])
        
        # 최대 파일 크기 (바이트)
        self.max_file_size = monitor_service.config.get('monitor', {}).get('max_file_size', 100 * 1024 * 1024)  # 100MB
        
        # 처리 중인 파일들을 추적 (중복 처리 방지)
        self.processing_files = set()
    
    def on_created(self, event):
        """새 파일이 생성되었을 때 호출됩니다."""
        if not event.is_directory:
            self._handle_file_event(event.src_path, 'created')
    
    def on_modified(self, event):
        """파일이 수정되었을 때 호출됩니다."""
        if not event.is_directory:
            self._handle_file_event(event.src_path, 'modified')
    
    def on_deleted(self, event):
        """파일이 삭제되었을 때 호출됩니다."""
        if not event.is_directory:
            self._handle_file_event(event.src_path, 'deleted')
    
    def _handle_file_event(self, file_path: str, event_type: str):
        """파일 이벤트를 처리합니다."""
        try:
            # 이미 처리 중인 파일인지 확인
            if file_path in self.processing_files:
                self.logger.debug(f"파일 {file_path} 이미 처리 중입니다.")
                return
            
            # 파일 경로를 Path 객체로 변환
            path_obj = Path(file_path)
            
            # 파일이 실제로 존재하는지 확인
            if not path_obj.exists():
                self.logger.debug(f"파일 {file_path}이 존재하지 않습니다.")
                return
            
            # 이미지 파일인지 확인
            if not self._is_image_file(path_obj):
                self.logger.debug(f"파일 {file_path}은 이미지 파일이 아닙니다.")
                return
            
            # 파일 크기 확인
            if not self._is_valid_file_size(path_obj):
                self.logger.warning(f"파일 {file_path}의 크기가 너무 큽니다.")
                return
            
            # 처리 중인 파일로 표시
            self.processing_files.add(file_path)
            
            try:
                if event_type == 'created':
                    self._handle_new_file(path_obj)
                elif event_type == 'modified':
                    self._handle_modified_file(path_obj)
                elif event_type == 'deleted':
                    self._handle_deleted_file(path_obj)
                
            finally:
                # 처리 완료 후 추적에서 제거
                self.processing_files.discard(file_path)
                
        except Exception as e:
            self.logger.error(f"파일 이벤트 처리 중 오류 발생: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _is_image_file(self, path_obj: Path) -> bool:
        """파일이 이미지 파일인지 확인합니다."""
        return path_obj.suffix.lower() in self.image_extensions
    
    def _is_valid_file_size(self, path_obj: Path) -> bool:
        """파일 크기가 유효한지 확인합니다."""
        try:
            file_size = path_obj.stat().st_size
            return file_size <= self.max_file_size
        except OSError:
            return False
    
    def _handle_new_file(self, path_obj: Path):
        """새 파일을 처리합니다."""
        self.logger.info(f"새 파일 감지: {path_obj}")
        
        # 파일이 완전히 쓰여졌는지 확인 (파일 크기가 안정화될 때까지 대기)
        self._wait_for_file_stable(path_obj)
        
        # 데이터베이스에 파일 정보 저장
        self.monitor_service.add_file_to_database(path_obj)
        
        # 콜백 함수 호출 (파일 처리 서비스에 알림)
        if self.monitor_service.on_new_file_callback:
            self.monitor_service.on_new_file_callback(path_obj)
    
    def _handle_modified_file(self, path_obj: Path):
        """수정된 파일을 처리합니다."""
        self.logger.debug(f"파일 수정 감지: {path_obj}")
        
        # 수정된 파일도 새 파일과 동일하게 처리
        self._handle_new_file(path_obj)
    
    def _handle_deleted_file(self, path_obj: Path):
        """삭제된 파일을 처리합니다."""
        self.logger.info(f"파일 삭제 감지: {path_obj}")
        
        # 데이터베이스에서 파일 정보 업데이트
        self.monitor_service.mark_file_deleted(path_obj)
    
    def _wait_for_file_stable(self, path_obj: Path, max_wait_time: int = 10):
        """파일이 안정화될 때까지 대기합니다."""
        start_time = time.time()
        last_size = -1
        
        while time.time() - start_time < max_wait_time:
            try:
                current_size = path_obj.stat().st_size
                
                # 파일 크기가 안정화되었는지 확인
                if current_size == last_size and current_size > 0:
                    self.logger.debug(f"파일 {path_obj} 안정화 완료 (크기: {current_size} bytes)")
                    return
                
                last_size = current_size
                time.sleep(0.5)  # 0.5초 대기
                
            except OSError:
                # 파일에 접근할 수 없는 경우 대기
                time.sleep(0.5)
        
        self.logger.warning(f"파일 {path_obj} 안정화 대기 시간 초과")


class FileMonitorService:
    """파일 시스템 모니터링 서비스"""
    
    def __init__(self):
        # 설정 로드
        self.config = get_config()
        
        # 로거 설정
        self.logger_manager = LoggerManager()
        self.logger = self.logger_manager.get_logger(__name__)
        
        # 데이터베이스 매니저
        self.db_manager = DatabaseManager()
        
        # 모니터링 설정
        self.monitor_config = self.config.get('monitor', {})
        self.base_folders = self.monitor_config.get('base_folders', [])
        self.scan_interval = self.monitor_config.get('scan_interval', 60)  # 초
        
        # 파일 변경 핸들러
        self.event_handler = FileChangeHandler(self)
        
        # Observer 객체
        self.observer = Observer()
        
        # 모니터링 상태
        self.is_monitoring = False
        self.monitored_paths = set()
        
        # 콜백 함수
        self.on_new_file_callback: Optional[Callable[[Path], None]] = None
        
        # 초기화
        self._validate_folders()
    
    def _validate_folders(self):
        """모니터링할 폴더들이 유효한지 확인합니다."""
        valid_folders = []
        
        for folder in self.base_folders:
            folder_path = Path(folder)
            
            if not folder_path.exists():
                self.logger.warning(f"폴더가 존재하지 않습니다: {folder}")
                continue
            
            if not folder_path.is_dir():
                self.logger.warning(f"경로가 폴더가 아닙니다: {folder}")
                continue
            
            # 폴더 내의 하위 폴더들 확인 (Sega_1, Sega_2, Sega_3)
            subfolders = [f for f in folder_path.iterdir() if f.is_dir()]
            if not subfolders:
                self.logger.warning(f"폴더 내에 하위 폴더가 없습니다: {folder}")
                continue
            
            valid_folders.append(folder)
            self.logger.info(f"유효한 모니터링 폴더: {folder}")
            self.logger.info(f"  하위 폴더: {[f.name for f in subfolders]}")
        
        self.base_folders = valid_folders
        
        if not self.base_folders:
            raise ValueError("모니터링할 유효한 폴더가 없습니다.")
    
    def start_monitoring(self):
        """파일 모니터링을 시작합니다."""
        if self.is_monitoring:
            self.logger.warning("파일 모니터링이 이미 실행 중입니다.")
            return
        
        try:
            self.logger.info("파일 모니터링 시작")
            
            # 각 기본 폴더에 대해 Observer 등록
            for base_folder in self.base_folders:
                base_path = Path(base_folder)
                
                # 기본 폴더와 모든 하위 폴더를 모니터링
                self.observer.schedule(self.event_handler, str(base_path), recursive=True)
                self.monitored_paths.add(str(base_path))
                
                self.logger.info(f"모니터링 시작: {base_path} (재귀적)")
            
            # Observer 시작
            self.observer.start()
            self.is_monitoring = True
            
            self.logger.info(f"✅ 파일 모니터링 시작 완료 (모니터링 폴더: {len(self.monitored_paths)}개)")
            
        except Exception as e:
            self.logger.error(f"파일 모니터링 시작 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def stop_monitoring(self):
        """파일 모니터링을 중지합니다."""
        if not self.is_monitoring:
            self.logger.warning("파일 모니터링이 실행 중이 아닙니다.")
            return
        
        try:
            self.logger.info("파일 모니터링 중지")
            
            # Observer 중지
            self.observer.stop()
            self.observer.join()
            
            self.is_monitoring = False
            self.monitored_paths.clear()
            
            self.logger.info("✅ 파일 모니터링 중지 완료")
            
        except Exception as e:
            self.logger.error(f"파일 모니터링 중지 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def add_file_to_database(self, file_path: Path):
        """새 파일을 데이터베이스에 추가합니다."""
        try:
            # 데이터베이스 세션 생성
            session = self.db_manager.get_session()
            
            try:
                # 기본 폴더 찾기
                base_folder = self._find_base_folder(file_path)
                if not base_folder:
                    self.logger.warning(f"파일 {file_path}의 기본 폴더를 찾을 수 없습니다.")
                    return
                
                # 파일 정보 생성
                file_info = FileInfo.create_from_path(
                    session=session,
                    file_path=str(file_path),
                    base_folder=str(base_folder)
                )
                
                self.logger.info(f"파일 정보 데이터베이스 저장 완료: {file_path}")
                self.logger.debug(f"  - ID: {file_info.id}")
                self.logger.debug(f"  - 폴더: {file_info.folder_name}")
                self.logger.debug(f"  - 크기: {file_info.file_size} bytes")
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"파일 정보 데이터베이스 저장 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def mark_file_deleted(self, file_path: Path):
        """삭제된 파일을 데이터베이스에 표시합니다."""
        try:
            # 데이터베이스 세션 생성
            session = self.db_manager.get_session()
            
            try:
                # 파일 정보 조회
                file_info = session.query(FileInfo).filter(
                    FileInfo.file_path == str(file_path)
                ).first()
                
                if file_info:
                    # 소프트 삭제로 표시
                    file_info.delete(session)
                    self.logger.info(f"파일 삭제 표시 완료: {file_path}")
                else:
                    self.logger.debug(f"데이터베이스에 없는 파일 삭제: {file_path}")
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"파일 삭제 표시 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _find_base_folder(self, file_path: Path) -> Optional[Path]:
        """파일이 속한 기본 폴더를 찾습니다."""
        for base_folder in self.base_folders:
            base_path = Path(base_folder)
            try:
                # 파일이 기본 폴더의 하위에 있는지 확인
                if file_path.resolve().is_relative_to(base_path.resolve()):
                    return base_path
            except ValueError:
                continue
        
        return None
    
    def set_new_file_callback(self, callback: Callable[[Path], None]):
        """새 파일 감지 시 호출될 콜백 함수를 설정합니다."""
        self.on_new_file_callback = callback
        self.logger.info("새 파일 콜백 함수 설정 완료")
    
    def check_and_update_monitored_folders(self):
        """새로운 날짜 폴더를 확인하고 모니터링 목록에 추가합니다."""
        try:
            self.logger.info("새로운 날짜 폴더 확인 및 모니터링 목록 업데이트 시작")
            
            new_folders_added = 0
            
            for base_folder in self.base_folders:
                base_path = Path(base_folder)
                
                if not base_path.exists():
                    self.logger.warning(f"기본 폴더가 존재하지 않습니다: {base_folder}")
                    continue
                
                # 하위 폴더들 확인 (Sega_1, Sega_2, Sega_3)
                subfolders = [f for f in base_path.iterdir() if f.is_dir()]
                
                for subfolder in subfolders:
                    # 날짜 형식 폴더인지 확인 (YYYY-MM-DD)
                    try:
                        datetime.strptime(subfolder.name, '%Y-%m-%d')
                        
                        # 이미 모니터링 중인지 확인
                        if str(subfolder) not in self.monitored_paths:
                            # 새로운 날짜 폴더를 모니터링에 추가
                            self.observer.schedule(self.event_handler, str(subfolder), recursive=True)
                            self.monitored_paths.add(str(subfolder))
                            new_folders_added += 1
                            
                            self.logger.info(f"새로운 날짜 폴더 모니터링 추가: {subfolder}")
                            
                    except ValueError:
                        # 날짜 형식이 아닌 폴더는 무시
                        continue
            
            if new_folders_added > 0:
                self.logger.info(f"새로운 날짜 폴더 {new_folders_added}개가 모니터링에 추가되었습니다.")
            else:
                self.logger.info("새로운 날짜 폴더가 없습니다.")
            
            return {
                'status': 'success',
                'new_folders_added': new_folders_added,
                'total_monitored_paths': len(self.monitored_paths),
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"새로운 날짜 폴더 확인 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def get_monitoring_status(self) -> Dict:
        """모니터링 상태 정보를 반환합니다."""
        return {
            'is_monitoring': self.is_monitoring,
            'monitored_paths': list(self.monitored_paths),
            'base_folders': self.base_folders,
            'scan_interval': self.scan_interval,
            'observer_alive': self.observer.is_alive() if hasattr(self.observer, 'is_alive') else False
        }
    
    def scan_existing_files(self):
        """기존 파일들을 스캔하여 데이터베이스에 추가합니다."""
        try:
            self.logger.info("기존 파일 스캔 시작")
            
            # 데이터베이스 세션 생성
            session = self.db_manager.get_session()
            
            try:
                total_files = 0
                added_files = 0
                
                for base_folder in self.base_folders:
                    base_path = Path(base_folder)
                    
                    # 모든 하위 폴더를 재귀적으로 스캔
                    for file_path in base_path.rglob('*'):
                        if file_path.is_file():
                            total_files += 1
                            
                            # 이미지 파일인지 확인
                            if file_path.suffix.lower() in self.monitor_config.get('image_extensions', ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff']):
                                # 파일 크기 확인
                                try:
                                    if file_path.stat().st_size <= self.monitor_config.get('max_file_size', 100 * 1024 * 1024):
                                        # 데이터베이스에 이미 존재하는지 확인
                                        existing_file = session.query(FileInfo).filter(
                                            FileInfo.file_path == str(file_path)
                                        ).first()
                                        
                                        if not existing_file:
                                            # 새 파일 정보 생성
                                            FileInfo.create_from_path(
                                                session=session,
                                                file_path=str(file_path),
                                                base_folder=str(base_path)
                                            )
                                            added_files += 1
                                            
                                            self.logger.debug(f"기존 파일 추가: {file_path}")
                                    
                                except OSError:
                                    self.logger.warning(f"파일 접근 실패: {file_path}")
                
                self.logger.info(f"기존 파일 스캔 완료: 총 {total_files}개 파일, {added_files}개 추가")
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"기존 파일 스캔 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")


# 테스트 코드
if __name__ == "__main__":
    print("FileMonitorService 테스트:")
    
    try:
        # 서비스 생성
        monitor_service = FileMonitorService()
        
        # 모니터링 상태 출력
        status = monitor_service.get_monitoring_status()
        print(f"모니터링 상태: {status}")
        
        # 기존 파일 스캔
        print("\n기존 파일 스캔 중...")
        monitor_service.scan_existing_files()
        
        print("✅ FileMonitorService 설정 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
