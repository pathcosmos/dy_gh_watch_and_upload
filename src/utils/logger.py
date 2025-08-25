"""
로깅 시스템 모듈
파일 모니터링 및 자동 업로드 시스템의 로깅을 담당합니다.
"""

import logging
import logging.handlers
import os
import sys
import gzip
import shutil
import json
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 설정 모듈 임포트
try:
    from config.settings import get_logging_config
except ImportError:
    # 설정 모듈이 없는 경우 기본값 사용
    def get_logging_config():
        return {
            'level': 'INFO',
            'file': 'logs/app.log',
            'max_bytes': 10485760,
            'backup_count': 5,
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'console_level': 'INFO'
        }


class LogMonitor:
    """로그 파일 모니터링 및 성능 최적화 클래스"""
    
    def __init__(self, log_file: str, max_size_mb: int = 100):
        self.log_file = log_file
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.monitoring = False
        self.monitor_thread = None
        self.stats = {
            'total_logs': 0,
            'error_count': 0,
            'warning_count': 0,
            'last_check': None
        }
    
    def start_monitoring(self):
        """로그 모니터링을 시작합니다."""
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
    
    def stop_monitoring(self):
        """로그 모니터링을 중지합니다."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
    
    def _monitor_loop(self):
        """로그 모니터링 루프"""
        while self.monitoring:
            try:
                self._check_log_file()
                time.sleep(60)  # 1분마다 체크
            except Exception as e:
                print(f"로그 모니터링 오류: {e}")
    
    def _check_log_file(self):
        """로그 파일 상태를 체크합니다."""
        if not os.path.exists(self.log_file):
            return
        
        # 파일 크기 체크
        file_size = os.path.getsize(self.log_file)
        if file_size > self.max_size_bytes:
            print(f"⚠️  로그 파일이 너무 큽니다: {file_size / (1024*1024):.2f}MB")
        
        # 로그 통계 업데이트
        self._update_stats()
        self.stats['last_check'] = datetime.now()
    
    def _update_stats(self):
        """로그 통계를 업데이트합니다."""
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                self.stats['total_logs'] = len(lines)
                
                # 에러 및 경고 카운트
                error_count = sum(1 for line in lines if 'ERROR' in line or 'CRITICAL' in line)
                warning_count = sum(1 for line in lines if 'WARNING' in line)
                
                self.stats['error_count'] = error_count
                self.stats['warning_count'] = warning_count
        except Exception:
            pass
    
    def get_stats(self) -> Dict[str, Any]:
        """현재 로그 통계를 반환합니다."""
        return self.stats.copy()


class StructuredFormatter(logging.Formatter):
    """구조화된 로그 포맷터 (JSON 형식)"""
    
    def format(self, record):
        # 기본 로그 정보
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # 추가 컨텍스트 정보
        if hasattr(record, 'extra_data'):
            log_entry.update(record.extra_data)
        
        # 예외 정보가 있으면 추가
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False, indent=2)


class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """압축 기능이 포함된 로테이팅 파일 핸들러"""
    
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False, errors=None, compress=True):
        # RotatingFileHandler의 올바른 매개변수 순서로 전달
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay, errors)
        self.compress = compress
    
    def doRollover(self):
        """로그 파일 로테이션을 수행합니다."""
        if self.stream:
            self.stream.close()
            self.stream = None
        
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = self.rotation_filename(self.baseFilename + "." + str(i))
                dfn = self.rotation_filename(self.baseFilename + "." + str(i + 1))
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            
            dfn = self.rotation_filename(self.baseFilename + ".1")
            if os.path.exists(dfn):
                if self.compress:
                    # 기존 백업 파일을 압축
                    self._compress_file(dfn)
                else:
                    os.rename(dfn, dfn)
        
        if not self.delay:
            self.stream = self._open()
    
    def _compress_file(self, filename):
        """파일을 gzip으로 압축합니다."""
        try:
            with open(filename, 'rb') as f_in:
                compressed_filename = filename + '.gz'
                with gzip.open(compressed_filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # 원본 파일 제거
            os.remove(filename)
        except Exception as e:
            # 압축 실패 시 원본 파일 유지
            pass
    
    def rotation_filename(self, filename):
        """로테이션된 파일명을 생성합니다."""
        return filename


class ColoredFormatter(logging.Formatter):
    """컬러 콘솔 출력을 위한 포맷터"""
    
    # ANSI 색상 코드
    COLORS = {
        'DEBUG': '\033[36m',      # 청록색
        'INFO': '\033[32m',       # 녹색
        'WARNING': '\033[33m',    # 노란색
        'ERROR': '\033[31m',      # 빨간색
        'CRITICAL': '\033[35m',   # 자주색
        'RESET': '\033[0m'        # 리셋
    }
    
    def format(self, record):
        # 원본 메시지 저장
        original_msg = record.getMessage()
        
        # 로그 레벨에 따른 색상 적용
        if record.levelname in self.COLORS:
            colored_level = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
            record.levelname = colored_level
        
        # 포맷팅된 메시지 반환
        formatted = super().format(record)
        
        # 원본 메시지 복원 (다른 핸들러에 영향 주지 않도록)
        record.msg = original_msg
        
        return formatted


class LoggerManager:
    """로거 관리를 담당하는 클래스"""
    
    def __init__(self, name: str = "file_monitor"):
        self.name = name
        self.logger = None
        self.log_monitor = None
        self._setup_logger()
        self._setup_log_monitor()
    
    def _setup_logger(self):
        """로거를 설정합니다."""
        # 로깅 설정 가져오기
        config = get_logging_config()
        
        # 로거 생성
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)  # 최소 레벨을 DEBUG로 설정
        
        # 기존 핸들러 제거 (중복 방지)
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # 콘솔 핸들러 추가
        self._add_console_handler(config)
        
        # 파일 핸들러 추가
        self._add_file_handler(config)
        
        # 로거 전파 비활성화 (루트 로거로 전파되지 않도록)
        self.logger.propagate = False
    
    def _setup_log_monitor(self):
        """로그 모니터를 설정합니다."""
        try:
            config = get_logging_config()
            log_file = config.get('file', 'logs/app.log')
            self.log_monitor = LogMonitor(log_file)
            self.log_monitor.start_monitoring()
        except Exception as e:
            print(f"로그 모니터 설정 실패: {e}")
    
    def _add_console_handler(self, config: Dict[str, Any]):
        """콘솔 핸들러를 추가합니다."""
        console_level = getattr(logging, config.get('console_level', 'INFO').upper())
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(console_level)
        
        # 컬러 포맷터 사용 (터미널에서만)
        if sys.stdout.isatty():
            formatter = ColoredFormatter(
                config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
        else:
            formatter = logging.Formatter(
                config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
        
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def _add_file_handler(self, config: Dict[str, Any]):
        """파일 핸들러를 추가합니다."""
        log_file = config.get('file', 'logs/app.log')
        log_level = getattr(logging, config.get('level', 'INFO').upper())
        max_bytes = config.get('max_bytes', 10485760)  # 10MB
        backup_count = config.get('backup_count', 5)
        
        # 로그 디렉토리 생성
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 압축 기능이 포함된 RotatingFileHandler 사용
        file_handler = CompressedRotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
            compress=True
        )
        file_handler.setLevel(log_level)
        
        # 파일용 상세 포맷터 (컬러 코드 제외)
        file_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
        formatter = logging.Formatter(file_format)
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        로거를 반환합니다.
        
        Args:
            name: 로거 이름 (None인 경우 기본 이름 사용)
            
        Returns:
            설정된 로거 객체
        """
        if name:
            return logging.getLogger(f"{self.name}.{name}")
        return self.logger
    
    def set_level(self, level: str):
        """
        로거 레벨을 설정합니다.
        
        Args:
            level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        try:
            log_level = getattr(logging, level.upper())
            self.logger.setLevel(log_level)
        except AttributeError:
            self.logger.warning(f"잘못된 로그 레벨: {level}. 기본값 INFO를 사용합니다.")
            self.logger.setLevel(logging.INFO)
    
    def add_filter(self, filter_func):
        """
        로그 필터를 추가합니다.
        
        Args:
            filter_func: 필터 함수 (record를 받아서 True/False 반환)
        """
        for handler in self.logger.handlers:
            handler.addFilter(filter_func)
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """
        오래된 로그 파일을 정리합니다.
        
        Args:
            days_to_keep: 보관할 로그 파일의 일수
        """
        try:
            config = get_logging_config()
            log_file = config.get('file', 'logs/app.log')
            log_dir = Path(log_file).parent
            
            cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
            
            for log_file_path in log_dir.glob("*.log*"):
                if log_file_path.stat().st_mtime < cutoff_time:
                    log_file_path.unlink()
                    print(f"오래된 로그 파일 삭제: {log_file_path}")
        except Exception as e:
            print(f"로그 정리 중 오류 발생: {e}")
    
    def log_with_context(self, level: str, message: str, **context):
        """
        컨텍스트 정보와 함께 로그를 기록합니다.
        
        Args:
            level: 로그 레벨
            message: 로그 메시지
            **context: 추가 컨텍스트 정보
        """
        # 로그 레벨을 정수로 변환
        try:
            level_no = getattr(logging, level.upper())
        except AttributeError:
            level_no = logging.INFO
        
        # 로그 레코드에 추가 정보 추가
        record = self.logger.makeRecord(
            self.logger.name, level_no, __file__, 0, message, (), None
        )
        record.extra_data = context
        
        # 로그 기록
        self.logger.handle(record)
    
    def get_log_stats(self) -> Dict[str, Any]:
        """로그 통계를 반환합니다."""
        if self.log_monitor:
            return self.log_monitor.get_stats()
        return {}
    
    def __del__(self):
        """소멸자: 로그 모니터링 중지"""
        if self.log_monitor:
            self.log_monitor.stop_monitoring()


# 전역 로거 매니저 인스턴스
_logger_manager = LoggerManager()

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    로거를 반환하는 편의 함수입니다.
    
    Args:
        name: 로거 이름 (None인 경우 기본 이름 사용)
        
    Returns:
        설정된 로거 객체
    """
    return _logger_manager.get_logger(name)


def setup_logging(name: str = "file_monitor") -> LoggerManager:
    """
    새로운 로거 매니저를 설정합니다.
    
    Args:
        name: 로거 이름
        
    Returns:
        설정된 로거 매니저 객체
    """
    return LoggerManager(name)


def set_log_level(level: str):
    """
    전역 로거 레벨을 설정합니다.
    
    Args:
        level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    _logger_manager.set_level(level)


def cleanup_old_logs(days_to_keep: int = 30):
    """
    오래된 로그 파일을 정리합니다.
    
    Args:
        days_to_keep: 보관할 로그 파일의 일수
    """
    _logger_manager.cleanup_old_logs(days_to_keep)


def log_with_context(level: str, message: str, **context):
    """
    컨텍스트 정보와 함께 로그를 기록합니다.
    
    Args:
        level: 로그 레벨
        message: 로그 메시지
        **context: 추가 컨텍스트 정보
    """
    _logger_manager.log_with_context(level, message, **context)


def get_log_stats() -> Dict[str, Any]:
    """로그 통계를 반환합니다."""
    return _logger_manager.get_log_stats()


# 로거 테스트
if __name__ == "__main__":
    # 로거 매니저 직접 테스트
    logger_manager = LoggerManager("test_logger")
    logger = logger_manager.get_logger()
    
    # 로그 레벨별 테스트
    logger.debug("디버그 메시지입니다.")
    logger.info("정보 메시지입니다.")
    logger.warning("경고 메시지입니다.")
    logger.error("오류 메시지입니다.")
    logger.critical("치명적 오류 메시지입니다.")
    
    print(f"\n로거 정보:")
    print(f"로거 이름: {logger.name}")
    print(f"로거 레벨: {logger.level}")
    print(f"핸들러 수: {len(logger.handlers)}")
    
    for i, handler in enumerate(logger.handlers):
        print(f"핸들러 {i+1}: {type(handler).__name__}, 레벨: {handler.level}")
        if hasattr(handler, 'formatter') and handler.formatter:
            print(f"  포맷터: {type(handler.formatter).__name__}")
    
    # 로그 레벨 변경 테스트
    print(f"\n로그 레벨을 DEBUG로 변경:")
    logger_manager.set_level("DEBUG")
    logger.debug("이제 디버그 메시지가 보일 것입니다.")
    
    # 컨텍스트 로깅 테스트
    print(f"\n컨텍스트 로깅 테스트:")
    log_with_context("INFO", "파일 업로드 시작", 
                    file_path="/path/to/file.jpg", 
                    file_size=1024, 
                    user_id="user123")
    
    # 로그 통계 테스트
    print(f"\n로그 통계:")
    stats = get_log_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # 로그 파일 확인
    log_file = get_logging_config().get('file', 'logs/app.log')
    if os.path.exists(log_file):
        print(f"\n로그 파일 정보:")
        print(f"파일 경로: {log_file}")
        print(f"파일 크기: {os.path.getsize(log_file)} bytes")
        
        # 로그 파일 내용 일부 출력
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print(f"로그 라인 수: {len(lines)}")
            if lines:
                print("마지막 로그 라인:")
                print(lines[-1].strip())
    
    # 압축 기능 테스트
    print(f"\n압축 기능 테스트:")
    print(f"사용된 핸들러: {type(logger.handlers[1]).__name__}")
    if hasattr(logger.handlers[1], 'compress'):
        print(f"압축 기능 활성화: {logger.handlers[1].compress}")
    
    # 로그 정리 기능 테스트
    print(f"\n로그 정리 기능 테스트:")
    logger_manager.cleanup_old_logs(1)  # 1일 이상 된 로그 파일 정리
    
    # 로그 모니터링 중지
    if logger_manager.log_monitor:
        logger_manager.log_monitor.stop_monitoring()
        print("로그 모니터링이 중지되었습니다.")
