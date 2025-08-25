"""
파일 모니터링 및 자동 업로드 시스템 메인 애플리케이션
모든 서비스를 통합하여 전체 시스템을 관리합니다.
"""

import os
import sys
import time
import signal
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import LoggerManager
from src.services.file_monitor import FileMonitorService
from src.services.scheduler import MonitoringScheduler
from src.services.file_processor import FileProcessor
from src.services.upload_service import UploadService
from src.db.connection import DatabaseManager
from config.settings import get_config


class FileMonitorApp:
    """파일 모니터링 및 자동 업로드 시스템 메인 애플리케이션"""
    
    def __init__(self):
        # 설정 로드
        self.config = get_config()
        
        # 로거 설정
        self.logger_manager = LoggerManager()
        self.logger = self.logger_manager.get_logger(__name__)
        
        # 시스템 상태
        self.is_running = False
        self.shutdown_event = threading.Event()
        
        # 서비스들
        self.file_monitor: Optional[FileMonitorService] = None
        self.scheduler: Optional[MonitoringScheduler] = None
        self.file_processor: Optional[FileProcessor] = None
        self.upload_service: Optional[UploadService] = None
        self.db_manager: Optional[DatabaseManager] = None
        
        # 메인 스레드
        self.main_thread: Optional[threading.Thread] = None
        
        # 시그널 핸들러 설정
        self._setup_signal_handlers()
        
        # 초기화
        self._initialize_services()
        self._setup_callbacks()
    
    def _setup_signal_handlers(self):
        """시스템 시그널 핸들러를 설정합니다."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("시그널 핸들러 설정 완료")
    
    def _signal_handler(self, signum, frame):
        """시스템 시그널을 처리합니다."""
        signal_name = signal.Signals(signum).name
        self.logger.info(f"시그널 수신: {signal_name}")
        
        if signum in [signal.SIGINT, signal.SIGTERM]:
            self.logger.info("시스템 종료 요청 수신")
            self.stop()
    
    def _initialize_services(self):
        """모든 서비스를 초기화합니다."""
        try:
            self.logger.info("서비스 초기화 시작")
            
            # 데이터베이스 매니저 초기화
            self.db_manager = DatabaseManager()
            self.logger.info("데이터베이스 매니저 초기화 완료")
            
            # 파일 모니터 서비스 초기화
            self.file_monitor = FileMonitorService()
            self.logger.info("파일 모니터 서비스 초기화 완료")
            
            # 스케줄러 서비스 초기화
            self.scheduler = MonitoringScheduler(self.file_monitor)
            self.logger.info("스케줄러 서비스 초기화 완료")
            
            # 파일 처리 서비스 초기화
            self.file_processor = FileProcessor()
            self.logger.info("파일 처리 서비스 초기화 완료")
            
            # 업로드 서비스 초기화
            self.upload_service = UploadService()
            self.logger.info("업로드 서비스 초기화 완료")
            
            self.logger.info("✅ 모든 서비스 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"서비스 초기화 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def _setup_callbacks(self):
        """서비스 간 콜백 함수들을 설정합니다."""
        try:
            self.logger.info("콜백 함수 설정 시작")
            
            # 파일 모니터 → 파일 처리 서비스
            if self.file_monitor and self.file_processor:
                self.file_monitor.set_new_file_callback(
                    self._on_file_detected
                )
                self.logger.info("파일 감지 콜백 설정 완료")
            
            # 파일 처리 → 업로드 서비스
            if self.file_processor and self.upload_service:
                self.file_processor.set_callbacks(
                    on_file_processed=self._on_file_processed,
                    on_file_ready_for_upload=self._on_file_ready_for_upload,
                    on_file_failed=self._on_file_processing_failed
                )
                self.logger.info("파일 처리 콜백 설정 완료")
            
            # 업로드 서비스 콜백
            if self.upload_service:
                self.upload_service.set_callbacks(
                    on_upload_started=self._on_upload_started,
                    on_upload_completed=self._on_upload_completed,
                    on_upload_failed=self._on_upload_failed,
                    on_upload_retry=self._on_upload_retry
                )
                self.logger.info("업로드 서비스 콜백 설정 완료")
            
            # 스케줄러 콜백
            if self.scheduler:
                self.scheduler.set_scan_complete_callback(
                    self._on_scan_complete
                )
                self.scheduler.set_error_callback(
                    self._on_scheduler_error
                )
                self.logger.info("스케줄러 콜백 설정 완료")
            
            self.logger.info("✅ 모든 콜백 함수 설정 완료")
            
        except Exception as e:
            self.logger.error(f"콜백 함수 설정 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def _on_file_detected(self, file_path: str, file_info: Dict):
        """새 파일이 감지되었을 때 호출되는 콜백"""
        try:
            self.logger.info(f"새 파일 감지: {file_path}")
            
            # 파일 처리 서비스에 추가
            if self.file_processor:
                self.file_processor.add_file(Path(file_path))
                self.logger.info(f"파일 처리 큐에 추가: {file_path}")
            
        except Exception as e:
            self.logger.error(f"파일 감지 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_file_processed(self, file_task):
        """파일 처리가 완료되었을 때 호출되는 콜백"""
        try:
            file_path = file_task.file_path
            self.logger.info(f"파일 처리 완료: {file_path}")
            
            # 여기서 추가적인 후처리 작업을 수행할 수 있습니다.
            # 예: 알림 전송, 로그 기록 등
            
        except Exception as e:
            self.logger.error(f"파일 처리 완료 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_file_ready_for_upload(self, file_task):
        """파일이 업로드 준비가 되었을 때 호출되는 콜백"""
        try:
            file_path = file_task.file_path
            self.logger.info(f"파일 업로드 준비 완료: {file_path}")
            
            # 업로드 서비스에 추가
            if self.upload_service:
                # 임시로 더미 ID 사용 (실제로는 데이터베이스에서 가져와야 함)
                file_info_id = 1
                upload_result_id = 1
                
                self.upload_service.add_upload_task(
                    file_path=file_path,
                    file_info_id=file_info_id,
                    upload_result_id=upload_result_id
                )
                self.logger.info(f"업로드 큐에 추가: {file_path}")
            
        except Exception as e:
            self.logger.error(f"파일 업로드 준비 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_file_processing_failed(self, file_task, error_message: str):
        """파일 처리에 실패했을 때 호출되는 콜백"""
        try:
            file_path = file_task.file_path
            self.logger.error(f"파일 처리 실패: {file_path} - {error_message}")
            
            # 여기서 실패 처리 작업을 수행할 수 있습니다.
            # 예: 오류 로그 기록, 관리자 알림 등
            
        except Exception as e:
            self.logger.error(f"파일 처리 실패 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_upload_started(self, upload_task):
        """업로드가 시작되었을 때 호출되는 콜백"""
        try:
            file_path = upload_task.file_path
            self.logger.info(f"업로드 시작: {file_path}")
            
            # 여기서 업로드 시작 관련 작업을 수행할 수 있습니다.
            # 예: 진행률 표시, 상태 업데이트 등
            
        except Exception as e:
            self.logger.error(f"업로드 시작 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_upload_completed(self, upload_task, api_response: Dict):
        """업로드가 완료되었을 때 호출되는 콜백"""
        try:
            file_path = upload_task.file_path
            self.logger.info(f"업로드 완료: {file_path}")
            self.logger.debug(f"API 응답: {api_response}")
            
            # 여기서 업로드 완료 관련 작업을 수행할 수 있습니다.
            # 예: 성공 알림, 통계 업데이트 등
            
        except Exception as e:
            self.logger.error(f"업로드 완료 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_upload_failed(self, upload_task, error_message: str):
        """업로드에 실패했을 때 호출되는 콜백"""
        try:
            file_path = upload_task.file_path
            self.logger.error(f"업로드 실패: {file_path} - {error_message}")
            
            # 여기서 업로드 실패 관련 작업을 수행할 수 있습니다.
            # 예: 오류 로그 기록, 재시도 스케줄링 등
            
        except Exception as e:
            self.logger.error(f"업로드 실패 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_upload_retry(self, upload_task, error_message: str):
        """업로드 재시도가 발생했을 때 호출되는 콜백"""
        try:
            file_path = upload_task.file_path
            retry_count = upload_task.retry_count
            self.logger.info(f"업로드 재시도: {file_path} (재시도 {retry_count})")
            
            # 여기서 재시도 관련 작업을 수행할 수 있습니다.
            # 예: 재시도 로그 기록, 지연 시간 조정 등
            
        except Exception as e:
            self.logger.error(f"업로드 재시도 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_scan_complete(self, scan_result: Dict):
        """스캔이 완료되었을 때 호출되는 콜백"""
        try:
            self.logger.info("스캔 완료 콜백 호출")
            self.logger.debug(f"스캔 결과: {scan_result}")
            
            # 여기서 스캔 완료 관련 작업을 수행할 수 있습니다.
            # 예: 통계 업데이트, 성능 모니터링 등
            
        except Exception as e:
            self.logger.error(f"스캔 완료 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _on_scheduler_error(self, job_id: str, exception: Exception):
        """스케줄러 오류가 발생했을 때 호출되는 콜백"""
        try:
            self.logger.error(f"스케줄러 오류: {job_id} - {str(exception)}")
            
            # 여기서 스케줄러 오류 관련 작업을 수행할 수 있습니다.
            # 예: 오류 로그 기록, 서비스 재시작 등
            
        except Exception as e:
            self.logger.error(f"스케줄러 오류 콜백 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def start(self):
        """애플리케이션을 시작합니다."""
        if self.is_running:
            self.logger.warning("애플리케이션이 이미 실행 중입니다.")
            return
        
        try:
            self.logger.info("애플리케이션 시작")
            
            # 모든 서비스 시작
            if self.file_monitor:
                self.file_monitor.start_monitoring()
                self.logger.info("파일 모니터링 시작")
            
            if self.scheduler:
                self.scheduler.start()
                self.logger.info("스케줄러 시작")
            
            if self.file_processor:
                self.file_processor.start()
                self.logger.info("파일 처리 서비스 시작")
            
            if self.upload_service:
                self.upload_service.start()
                self.logger.info("업로드 서비스 시작")
            
            # 메인 스레드 시작
            self.is_running = True
            self.shutdown_event.clear()
            
            self.main_thread = threading.Thread(
                target=self._main_loop,
                name="MainLoop",
                daemon=False
            )
            self.main_thread.start()
            
            self.logger.info("✅ 애플리케이션 시작 완료")
            
        except Exception as e:
            self.logger.error(f"애플리케이션 시작 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def _main_loop(self):
        """메인 애플리케이션 루프"""
        try:
            self.logger.info("메인 루프 시작")
            
            while not self.shutdown_event.is_set():
                try:
                    # 시스템 상태 점검
                    self._check_system_health()
                    
                    # 1초 대기
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"메인 루프 오류: {str(e)}")
                    self.logger.exception("상세 에러 정보:")
                    time.sleep(5)  # 오류 발생 시 5초 대기
            
            self.logger.info("메인 루프 종료")
            
        except Exception as e:
            self.logger.error(f"메인 루프 실행 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _check_system_health(self):
        """시스템 상태를 점검합니다."""
        try:
            # 각 서비스의 상태 확인
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'file_monitor': self._get_service_status(self.file_monitor),
                'scheduler': self._get_service_status(self.scheduler),
                'file_processor': self._get_service_status(self.file_processor),
                'upload_service': self._get_service_status(self.upload_service)
            }
            
            # 상태 로깅 (디버그 레벨)
            self.logger.debug(f"시스템 상태: {health_status}")
            
            # 문제가 있는 경우 경고
            for service_name, status in health_status.items():
                if service_name != 'timestamp' and status and not status.get('healthy', True):
                    self.logger.warning(f"서비스 상태 문제: {service_name} - {status}")
            
        except Exception as e:
            self.logger.error(f"시스템 상태 점검 실패: {str(e)}")
    
    def _get_service_status(self, service) -> Optional[Dict]:
        """서비스의 상태를 반환합니다."""
        if not service:
            return None
        
        try:
            if hasattr(service, 'get_status'):
                return service.get_status()
            elif hasattr(service, 'get_monitoring_status'):
                return service.get_monitoring_status()
            elif hasattr(service, 'get_queue_status'):
                return service.get_queue_status()
            else:
                return {'healthy': True, 'status': 'unknown'}
        except Exception as e:
            return {'healthy': False, 'error': str(e)}
    
    def stop(self):
        """애플리케이션을 중지합니다."""
        if not self.is_running:
            self.logger.warning("애플리케이션이 실행 중이 아닙니다.")
            return
        
        try:
            self.logger.info("애플리케이션 중지")
            
            # 종료 이벤트 설정
            self.shutdown_event.set()
            
            # 모든 서비스 중지
            if self.upload_service:
                self.upload_service.stop()
                self.logger.info("업로드 서비스 중지")
            
            if self.file_processor:
                self.file_processor.stop()
                self.logger.info("파일 처리 서비스 중지")
            
            if self.scheduler:
                self.scheduler.stop()
                self.logger.info("스케줄러 중지")
            
            if self.file_monitor:
                self.file_monitor.stop_monitoring()
                self.logger.info("파일 모니터링 중지")
            
            # 메인 스레드 종료 대기
            if self.main_thread and self.main_thread.is_alive():
                self.main_thread.join(timeout=10)
                if self.main_thread.is_alive():
                    self.logger.warning("메인 스레드가 10초 내에 종료되지 않았습니다.")
            
            self.is_running = False
            self.logger.info("✅ 애플리케이션 중지 완료")
            
        except Exception as e:
            self.logger.error(f"애플리케이션 중지 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def cleanup(self):
        """리소스를 정리합니다."""
        try:
            self.logger.info("리소스 정리 시작")
            
            # 각 서비스의 리소스 정리
            if self.upload_service:
                self.upload_service.cleanup()
            
            if self.file_processor:
                self.file_processor.cleanup()
            
            if self.db_manager:
                self.db_manager.close()
            
            self.logger.info("✅ 리소스 정리 완료")
            
        except Exception as e:
            self.logger.error(f"리소스 정리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def get_status(self) -> Dict:
        """애플리케이션 상태 정보를 반환합니다."""
        return {
            'is_running': self.is_running,
            'timestamp': datetime.now().isoformat(),
            'services': {
                'file_monitor': self._get_service_status(self.file_monitor),
                'scheduler': self._get_service_status(self.scheduler),
                'file_processor': self._get_service_status(self.file_processor),
                'upload_service': self._get_service_status(self.upload_service)
            }
        }
    
    def run(self):
        """애플리케이션을 실행합니다."""
        try:
            # 애플리케이션 시작
            self.start()
            
            # 메인 스레드가 종료될 때까지 대기
            if self.main_thread:
                self.main_thread.join()
            
        except KeyboardInterrupt:
            self.logger.info("키보드 인터럽트 수신")
        except Exception as e:
            self.logger.error(f"애플리케이션 실행 중 오류: {str(e)}")
            self.logger.exception("상세 에러 정보:")
        finally:
            # 정리 작업
            self.stop()
            self.cleanup()


# 메인 실행 코드
if __name__ == "__main__":
    print("파일 모니터링 및 자동 업로드 시스템")
    print("=" * 50)
    
    try:
        # 애플리케이션 생성 및 실행
        app = FileMonitorApp()
        app.run()
        
    except Exception as e:
        print(f"❌ 애플리케이션 실행 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
