"""
스케줄러 서비스
주기적으로 파일을 스캔하고 모니터링 상태를 점검합니다.
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# 프로젝트 루트를 Python 경로에 추가
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import LoggerManager
from src.services.file_monitor import FileMonitorService
from src.db.connection import DatabaseManager
from src.models.file_info import FileInfo
from src.uploader.service import uploader_service
from config.settings import get_config


class MonitoringScheduler:
    """파일 모니터링을 위한 스케줄러 서비스"""
    
    def __init__(self, file_monitor_service: FileMonitorService):
        # 설정 로드
        self.config = get_config()
        
        # 로거 설정
        self.logger_manager = LoggerManager()
        self.logger = self.logger_manager.get_logger(__name__)
        
        # 파일 모니터 서비스
        self.file_monitor = file_monitor_service
        
        # 데이터베이스 매니저
        self.db_manager = DatabaseManager()
        
        # 스케줄러 설정
        self.scheduler_config = self.config.get('scheduler', {})
        self.scan_interval = self.scheduler_config.get('scan_interval_minutes', 1)
        self.enable_periodic_scan = self.scheduler_config.get('enable_periodic_scan', True)
        self.enable_health_check = self.scheduler_config.get('enable_health_check', True)
        self.enable_cleanup = self.scheduler_config.get('enable_cleanup', True)
        
        # APScheduler 인스턴스
        self.scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,
                'max_instances': 1,
                'misfire_grace_time': 60
            }
        )
        
        # 스케줄러 상태
        self.is_running = False
        self.jobs = {}
        
        # 콜백 함수들
        self.on_scan_complete_callback: Optional[Callable[[Dict], None]] = None
        self.on_error_callback: Optional[Callable[[str, Exception], None]] = None
        
        # 이벤트 리스너 설정
        self._setup_event_listeners()
        
        # 작업 등록
        self._register_jobs()
    
    def _setup_event_listeners(self):
        """스케줄러 이벤트 리스너를 설정합니다."""
        self.scheduler.add_listener(self._job_executed_listener, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
    
    def _job_executed_listener(self, event):
        """작업 실행 완료 이벤트를 처리합니다."""
        job_id = event.job_id
        job = self.scheduler.get_job(job_id)
        
        if job:
            self.logger.debug(f"작업 실행 완료: {job_id} - {job.name}")
            
            # 콜백 함수 호출
            if self.on_scan_complete_callback:
                result = {
                    'job_id': job_id,
                    'job_name': job.name,
                    'execution_time': event.scheduled_run_time,
                    'return_value': event.retval
                }
                self.on_scan_complete_callback(result)
    
    def _job_error_listener(self, event):
        """작업 실행 오류 이벤트를 처리합니다."""
        job_id = event.job_id
        job = self.scheduler.get_job(job_id)
        exception = event.exception
        
        self.logger.error(f"작업 실행 오류: {job_id} - {job.name if job else 'Unknown'}")
        self.logger.error(f"오류 내용: {str(exception)}")
        
        # 콜백 함수 호출
        if self.on_error_callback:
            self.on_error_callback(job_id, exception)
    
    def _register_jobs(self):
        """스케줄러 작업들을 등록합니다."""
        try:
            # 주기적 파일 스캔 작업
            if self.enable_periodic_scan:
                self._add_periodic_scan_job()
            
            # 새로운 날짜 폴더 확인 작업
            self._add_check_new_folders_job()
            
            # 업로드 대기 중인 파일 처리 작업
            self._add_process_pending_uploads_job()
            
            # 상태 점검 작업
            if self.enable_health_check:
                self._add_health_check_job()
            
            # 정리 작업
            if self.enable_cleanup:
                self._add_cleanup_job()
            
            self.logger.info("스케줄러 작업 등록 완료")
            
        except Exception as e:
            self.logger.error(f"스케줄러 작업 등록 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
    
    def _add_periodic_scan_job(self):
        """주기적 파일 스캔 작업을 추가합니다."""
        job_id = 'periodic_file_scan'
        
        # 분 단위로 실행
        trigger = IntervalTrigger(
            minutes=self.scan_interval,
            start_date=datetime.now() + timedelta(seconds=10)  # 10초 후 시작
        )
        
        job = self.scheduler.add_job(
            func=self._periodic_file_scan,
            trigger=trigger,
            id=job_id,
            name='주기적 파일 스캔',
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        self.logger.info(f"주기적 파일 스캔 작업 등록: {self.scan_interval}분마다 실행")
    
    def _add_check_new_folders_job(self):
        """새로운 날짜 폴더 확인 작업을 추가합니다."""
        job_id = 'check_new_folders'
        
        # 분 단위로 실행
        trigger = IntervalTrigger(
            minutes=self.scan_interval,
            start_date=datetime.now() + timedelta(seconds=5)  # 5초 후 시작
        )
        
        job = self.scheduler.add_job(
            func=self.file_monitor.check_and_update_monitored_folders,
            trigger=trigger,
            id=job_id,
            name='새로운 날짜 폴더 확인',
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        self.logger.info(f"새로운 날짜 폴더 확인 작업 등록: {self.scan_interval}분마다 실행")
    
    def _add_process_pending_uploads_job(self):
        """업로드 대기 중인 파일 처리 작업을 추가합니다."""
        job_id = 'process_pending_uploads'
        
        # 분 단위로 실행
        trigger = IntervalTrigger(
            minutes=self.scan_interval,
            start_date=datetime.now() + timedelta(seconds=15)  # 15초 후 시작
        )
        
        job = self.scheduler.add_job(
            func=self.process_pending_uploads,
            trigger=trigger,
            id=job_id,
            name='업로드 대기 중인 파일 처리',
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        self.logger.info(f"업로드 대기 중인 파일 처리 작업 등록: {self.scan_interval}분마다 실행")
    
    def _add_health_check_job(self):
        """상태 점검 작업을 추가합니다."""
        job_id = 'health_check'
        
        # 5분마다 실행
        trigger = IntervalTrigger(minutes=5)
        
        job = self.scheduler.add_job(
            func=self._health_check,
            trigger=trigger,
            id=job_id,
            name='모니터링 상태 점검',
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        self.logger.info("상태 점검 작업 등록: 5분마다 실행")
    
    def _add_cleanup_job(self):
        """정리 작업을 추가합니다."""
        job_id = 'cleanup'
        
        # 매일 새벽 2시에 실행
        trigger = CronTrigger(hour=2, minute=0)
        
        job = self.scheduler.add_job(
            func=self._cleanup_old_records,
            trigger=trigger,
            id=job_id,
            name='오래된 레코드 정리',
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        self.logger.info("정리 작업 등록: 매일 새벽 2시 실행")
    
    def start(self):
        """스케줄러를 시작합니다."""
        if self.is_running:
            self.logger.warning("스케줄러가 이미 실행 중입니다.")
            return
        
        try:
            self.logger.info("스케줄러 시작")
            
            # 스케줄러 시작
            self.scheduler.start()
            self.is_running = True
            
            # 등록된 작업 정보 출력
            self._log_job_info()
            
            self.logger.info("✅ 스케줄러 시작 완료")
            
        except Exception as e:
            self.logger.error(f"스케줄러 시작 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def stop(self):
        """스케줄러를 중지합니다."""
        if not self.is_running:
            self.logger.warning("스케줄러가 실행 중이 아닙니다.")
            return
        
        try:
            self.logger.info("스케줄러 중지")
            
            # 스케줄러 중지
            self.scheduler.shutdown(wait=True)
            self.is_running = False
            
            self.logger.info("✅ 스케줄러 중지 완료")
            
        except Exception as e:
            self.logger.error(f"스케줄러 중지 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            raise
    
    def _periodic_file_scan(self):
        """주기적으로 파일을 스캔합니다."""
        try:
            self.logger.info("주기적 파일 스캔 시작")
            start_time = time.time()
            
            # 기존 파일 스캔
            self.file_monitor.scan_existing_files()
            
            # 스캔 결과 통계
            scan_stats = self._get_scan_statistics()
            
            execution_time = time.time() - start_time
            self.logger.info(f"주기적 파일 스캔 완료 (소요시간: {execution_time:.2f}초)")
            self.logger.info(f"스캔 통계: {scan_stats}")
            
            return {
                'status': 'success',
                'execution_time': execution_time,
                'scan_stats': scan_stats,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"주기적 파일 스캔 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def _health_check(self):
        """모니터링 상태를 점검합니다."""
        try:
            self.logger.info("모니터링 상태 점검 시작")
            
            # 파일 모니터 상태 확인
            monitor_status = self.file_monitor.get_monitoring_status()
            
            # 데이터베이스 연결 상태 확인
            db_status = self._check_database_health()
            
            # 전체 상태 점검
            health_status = {
                'monitor': monitor_status,
                'database': db_status,
                'scheduler': {
                    'is_running': self.is_running,
                    'job_count': len(self.jobs),
                    'next_run': self._get_next_run_times()
                },
                'timestamp': datetime.now()
            }
            
            # 상태 로깅
            self._log_health_status(health_status)
            
            # 문제가 있는 경우 경고
            if not monitor_status['is_monitoring']:
                self.logger.warning("⚠️ 파일 모니터링이 중지되었습니다.")
            
            if not db_status['connected']:
                self.logger.error("❌ 데이터베이스 연결에 문제가 있습니다.")
            
            return health_status
            
        except Exception as e:
            self.logger.error(f"상태 점검 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def process_pending_uploads(self):
        """업로드 대기 중인 파일을 처리합니다."""
        try:
            self.logger.info("업로드 대기 중인 파일 확인 및 처리 시작...")
            
            # PENDING 상태의 파일 조회
            session = self.db_manager.get_session()
            try:
                pending_files = session.query(FileInfo).filter(
                    FileInfo.processing_status == 'pending'
                ).all()
                
                if not pending_files:
                    self.logger.info("업로드 대기 중인 파일 없음.")
                    return {
                        'status': 'success',
                        'processed_count': 0,
                        'message': '업로드 대기 중인 파일 없음'
                    }
                
                self.logger.info(f"업로드 대기 중인 파일 {len(pending_files)}개 발견")
                
                # 각 파일에 대해 업로드 시도
                processed_count = 0
                for file_obj in pending_files:
                    try:
                        self.logger.info(f"업로드 대기 중인 파일 처리: {file_obj.file_name}")
                        result = uploader_service.upload_and_record(file_obj)
                        
                        if result:
                            self.logger.info(f"파일 업로드 성공: {file_obj.file_name}")
                        else:
                            self.logger.warning(f"파일 업로드 실패: {file_obj.file_name}")
                        
                        processed_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"파일 업로드 처리 중 오류: {file_obj.file_name} - {str(e)}")
                        continue
                
                self.logger.info(f"업로드 대기 중인 파일 처리 완료: {processed_count}/{len(pending_files)}개 처리됨")
                
                return {
                    'status': 'success',
                    'processed_count': processed_count,
                    'total_count': len(pending_files),
                    'timestamp': datetime.now()
                }
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"업로드 대기 중인 파일 처리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def _cleanup_old_records(self):
        """오래된 레코드를 정리합니다."""
        try:
            self.logger.info("오래된 레코드 정리 시작")
            
            # 설정에서 보관 기간 가져오기
            retention_days = self.config.get('database', {}).get('retention_days', 90)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # 데이터베이스 세션 생성
            session = self.db_manager.get_session()
            
            try:
                # 오래된 파일 정보 삭제
                old_file_infos = session.query(FileInfo).filter(
                    FileInfo.scan_date < cutoff_date,
                    FileInfo.is_deleted == False
                ).all()
                
                deleted_file_count = 0
                for file_info in old_file_infos:
                    file_info.delete(session)
                    deleted_file_count += 1
                
                # 오래된 업로드 결과 삭제 (선택적)
                cleanup_upload_results = self.config.get('database', {}).get('cleanup_upload_results', False)
                if cleanup_upload_results:
                    from src.models.upload_result import UploadResult
                    old_upload_results = session.query(UploadResult).filter(
                        UploadResult.created_at < cutoff_date,
                        UploadResult.is_deleted == False
                    ).all()
                    
                    deleted_upload_count = 0
                    for upload_result in old_upload_results:
                        upload_result.delete(session)
                        deleted_upload_count += 1
                    
                    self.logger.info(f"오래된 업로드 결과 {deleted_upload_count}개 삭제")
                
                self.logger.info(f"오래된 파일 정보 {deleted_file_count}개 삭제 완료")
                
                return {
                    'status': 'success',
                    'deleted_file_count': deleted_file_count,
                    'retention_days': retention_days,
                    'timestamp': datetime.now()
                }
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"오래된 레코드 정리 실패: {str(e)}")
            self.logger.exception("상세 에러 정보:")
            
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def _check_database_health(self) -> Dict:
        """데이터베이스 상태를 점검합니다."""
        try:
            # 연결 테스트
            connected = self.db_manager.test_connection()
            
            if connected:
                # 간단한 쿼리 테스트
                session = self.db_manager.get_session()
                try:
                    # 파일 개수 조회
                    file_count = session.query(FileInfo).count()
                    
                    return {
                        'connected': True,
                        'file_count': file_count,
                        'status': 'healthy'
                    }
                finally:
                    session.close()
            else:
                return {
                    'connected': False,
                    'status': 'connection_failed'
                }
                
        except Exception as e:
            return {
                'connected': False,
                'status': 'error',
                'error': str(e)
            }
    
    def _get_scan_statistics(self) -> Dict:
        """스캔 통계 정보를 반환합니다."""
        try:
            session = self.db_manager.get_session()
            
            try:
                # 전체 파일 개수
                total_files = session.query(FileInfo).count()
                
                # 상태별 파일 개수
                status_counts = FileInfo.get_file_count_by_status(session)
                
                # 폴더별 파일 개수
                folder_counts = {}
                for folder_name in ['Sega_1', 'Sega_2', 'Sega_3']:
                    count = session.query(FileInfo).filter(
                        FileInfo.folder_name == folder_name
                    ).count()
                    folder_counts[folder_name] = count
                
                return {
                    'total_files': total_files,
                    'status_counts': status_counts,
                    'folder_counts': folder_counts
                }
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"스캔 통계 수집 실패: {str(e)}")
            return {'error': str(e)}
    
    def _get_job_next_run(self, job) -> str:
        """작업의 다음 실행 시간을 안전하게 가져옵니다."""
        try:
            next_run = job.next_run_time
            if next_run:
                return next_run.isoformat()
            else:
                return 'Not scheduled'
        except AttributeError:
            return 'Unknown'
    
    def _get_next_run_times(self) -> Dict:
        """다음 실행 시간들을 반환합니다."""
        next_runs = {}
        
        for job_id, job in self.jobs.items():
            next_runs[job_id] = self._get_job_next_run(job)
        
        return next_runs
    
    def _log_job_info(self):
        """등록된 작업 정보를 로깅합니다."""
        self.logger.info("등록된 스케줄러 작업:")
        for job_id, job in self.jobs.items():
            try:
                next_run = job.next_run_time
                next_run_str = next_run.isoformat() if next_run else 'Not scheduled'
                self.logger.info(f"  - {job_id}: {job.name} (다음 실행: {next_run_str})")
            except AttributeError:
                self.logger.info(f"  - {job_id}: {job.name} (다음 실행: Unknown)")
    
    def _log_health_status(self, health_status: Dict):
        """상태 점검 결과를 로깅합니다."""
        monitor_status = health_status['monitor']
        db_status = health_status['database']
        scheduler_status = health_status['scheduler']
        
        self.logger.info("모니터링 상태 점검 결과:")
        self.logger.info(f"  - 파일 모니터: {'실행 중' if monitor_status['is_monitoring'] else '중지됨'}")
        self.logger.info(f"  - 데이터베이스: {'연결됨' if db_status['connected'] else '연결 안됨'}")
        self.logger.info(f"  - 스케줄러: {'실행 중' if scheduler_status['is_running'] else '중지됨'}")
        
        if db_status['connected'] and 'file_count' in db_status:
            self.logger.info(f"  - 총 파일 수: {db_status['file_count']}")
    
    def get_status(self) -> Dict:
        """스케줄러 상태 정보를 반환합니다."""
        return {
            'is_running': self.is_running,
            'job_count': len(self.jobs),
            'jobs': {job_id: {
                'name': job.name,
                'next_run': self._get_job_next_run(job),
                'trigger': str(job.trigger)
            } for job_id, job in self.jobs.items()},
            'config': {
                'scan_interval': self.scan_interval,
                'enable_periodic_scan': self.enable_periodic_scan,
                'enable_health_check': self.enable_health_check,
                'enable_cleanup': self.enable_cleanup
            }
        }
    
    def set_scan_complete_callback(self, callback: Callable[[Dict], None]):
        """스캔 완료 시 호출될 콜백 함수를 설정합니다."""
        self.on_scan_complete_callback = callback
        self.logger.info("스캔 완료 콜백 함수 설정 완료")
    
    def set_error_callback(self, callback: Callable[[str, Exception], None]):
        """오류 발생 시 호출될 콜백 함수를 설정합니다."""
        self.on_error_callback = callback
        self.logger.info("오류 콜백 함수 설정 완료")
    
    def pause_job(self, job_id: str):
        """특정 작업을 일시 중지합니다."""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                job.pause()
                self.logger.info(f"작업 일시 중지: {job_id}")
            else:
                self.logger.warning(f"작업을 찾을 수 없습니다: {job_id}")
        except Exception as e:
            self.logger.error(f"작업 일시 중지 실패: {str(e)}")
    
    def resume_job(self, job_id: str):
        """일시 중지된 작업을 재개합니다."""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                job.resume()
                self.logger.info(f"작업 재개: {job_id}")
            else:
                self.logger.warning(f"작업을 찾을 수 없습니다: {job_id}")
        except Exception as e:
            self.logger.error(f"작업 재개 실패: {str(e)}")


# 테스트 코드
if __name__ == "__main__":
    print("MonitoringScheduler 테스트:")
    
    try:
        # 파일 모니터 서비스 생성
        from src.services.file_monitor import FileMonitorService
        file_monitor = FileMonitorService()
        
        # 스케줄러 생성
        scheduler = MonitoringScheduler(file_monitor)
        
        # 상태 출력
        status = scheduler.get_status()
        print(f"스케줄러 상태: {status}")
        
        print("✅ MonitoringScheduler 설정 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
