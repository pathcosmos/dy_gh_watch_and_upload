"""
설정 관리 모듈
YAML 설정 파일을 로드하고 환경 변수로 오버라이드하는 기능을 제공합니다.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


def load_env_file(env_path: Optional[str] = None) -> None:
    """
    환경 변수 파일을 로드합니다.
    
    Args:
        env_path: 환경 변수 파일 경로 (None인 경우 기본 경로 사용)
    """
    if env_path is None:
        # 프로젝트 루트 디렉토리 기준으로 환경 변수 파일 경로 결정
        project_root = Path(__file__).parent.parent
        env_path = project_root / "config" / "app.env"
    
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        # 기본 .env 파일도 시도
        project_root = Path(__file__).parent.parent
        default_env = project_root / ".env"
        if os.path.exists(default_env):
            load_dotenv(default_env)


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    YAML 설정 파일을 로드하고 Python 딕셔너리로 반환합니다.
    
    Args:
        config_path: 설정 파일 경로 (None인 경우 기본 경로 사용)
        
    Returns:
        로드된 설정 딕셔너리
        
    Raises:
        FileNotFoundError: 설정 파일을 찾을 수 없는 경우
        yaml.YAMLError: YAML 파싱 오류가 발생한 경우
    """
    if config_path is None:
        # 프로젝트 루트 디렉토리 기준으로 설정 파일 경로 결정
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "settings.yaml"
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if config is None:
            raise yaml.YAMLError("설정 파일이 비어있습니다.")
            
        return config
        
    except FileNotFoundError:
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {config_path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"YAML 파싱 오류: {e}")


def apply_environment_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    환경 변수를 사용하여 설정을 오버라이드합니다.
    
    Args:
        config: 기본 설정 딕셔너리
        
    Returns:
        환경 변수가 적용된 설정 딕셔너리
    """
    # 데이터베이스 설정 오버라이드
    if 'database' in config:
        db_config = config['database']
        db_config['type'] = os.getenv('DB_TYPE', db_config.get('type', 'sqlite'))
        db_config['host'] = os.getenv('DB_HOST', db_config.get('host', 'localhost'))
        db_config['port'] = int(os.getenv('DB_PORT', db_config.get('port', 5432)))
        db_config['user'] = os.getenv('DB_USER', db_config.get('user', 'postgres'))
        db_config['password'] = os.getenv('DB_PASSWORD', db_config.get('password', ''))
        db_config['dbname'] = os.getenv('DB_NAME', db_config.get('dbname', 'file_monitor_db'))
        db_config['sqlite_path'] = os.getenv('DB_SQLITE_PATH', db_config.get('sqlite_path', 'data/app.db'))
    
    # API 설정 오버라이드
    if 'api' in config:
        api_config = config['api']
        api_config['endpoint'] = os.getenv('API_ENDPOINT', api_config.get('endpoint', ''))
        api_config['timeout_seconds'] = int(os.getenv('API_TIMEOUT_SECONDS', api_config.get('timeout_seconds', 30)))
        api_config['retry_attempts'] = int(os.getenv('API_RETRY_ATTEMPTS', api_config.get('retry_attempts', 3)))
        api_config['retry_delay_seconds'] = int(os.getenv('API_RETRY_DELAY_SECONDS', api_config.get('retry_delay_seconds', 5)))
    
    # 모니터링 설정 오버라이드
    if 'monitor' in config:
        monitor_config = config['monitor']
        monitor_config['scan_interval_minutes'] = int(os.getenv('MONITOR_SCAN_INTERVAL_MINUTES', monitor_config.get('scan_interval_minutes', 1)))
        monitor_config['max_file_size'] = int(os.getenv('MONITOR_MAX_FILE_SIZE', monitor_config.get('max_file_size', 10485760)))
    
    # 로깅 설정 오버라이드
    if 'logging' in config:
        logging_config = config['logging']
        logging_config['level'] = os.getenv('LOG_LEVEL', logging_config.get('level', 'INFO'))
        logging_config['file'] = os.getenv('LOG_FILE', logging_config.get('file', 'logs/app.log'))
    
    # 시스템 설정 오버라이드
    if 'system' in config:
        system_config = config['system']
        system_config['user'] = os.getenv('SYSTEM_USER', system_config.get('user', 'filemonitor'))
        system_config['group'] = os.getenv('SYSTEM_GROUP', system_config.get('group', 'filemonitor'))
        system_config['working_directory'] = os.getenv('SYSTEM_WORKING_DIRECTORY', system_config.get('working_directory', '/opt/file-monitor'))
    
    return config


def validate_config(config: Dict[str, Any]) -> None:
    """
    설정 값의 유효성을 검사합니다.
    
    Args:
        config: 검사할 설정 딕셔너리
        
    Raises:
        ValueError: 유효하지 않은 설정이 발견된 경우
    """
    errors = []
    
    # 모니터링 설정 검증
    if 'monitor' in config:
        monitor = config['monitor']
        
        # base_folders 검증
        if 'base_folders' not in monitor or not isinstance(monitor['base_folders'], list):
            errors.append("monitor.base_folders는 리스트여야 합니다.")
        elif len(monitor['base_folders']) == 0:
            errors.append("monitor.base_folders는 비어있을 수 없습니다.")
        
        # scan_interval_minutes 검증
        if 'scan_interval_minutes' in monitor:
            try:
                interval = int(monitor['scan_interval_minutes'])
                if interval <= 0:
                    errors.append("monitor.scan_interval_minutes는 양의 정수여야 합니다.")
            except (ValueError, TypeError):
                errors.append("monitor.scan_interval_minutes는 정수여야 합니다.")
        
        # max_file_size 검증
        if 'max_file_size' in monitor:
            try:
                max_size = int(monitor['max_file_size'])
                if max_size <= 0:
                    errors.append("monitor.max_file_size는 양의 정수여야 합니다.")
            except (ValueError, TypeError):
                errors.append("monitor.max_file_size는 정수여야 합니다.")
    
    # 데이터베이스 설정 검증
    if 'database' in config:
        db = config['database']
        
        if 'type' not in db:
            errors.append("database.type는 필수 설정입니다.")
        elif db['type'] not in ['sqlite', 'postgresql']:
            errors.append("database.type는 'sqlite' 또는 'postgresql'이어야 합니다.")
        
        if db.get('type') == 'sqlite' and 'sqlite_path' not in db:
            errors.append("database.sqlite_path는 SQLite 사용 시 필수입니다.")
    
    # API 설정 검증
    if 'api' in config:
        api = config['api']
        
        if 'endpoint' not in api or not api['endpoint']:
            errors.append("api.endpoint는 필수 설정입니다.")
        
        if 'timeout_seconds' in api:
            try:
                timeout = int(api['timeout_seconds'])
                if timeout <= 0:
                    errors.append("api.timeout_seconds는 양의 정수여야 합니다.")
            except (ValueError, TypeError):
                errors.append("api.timeout_seconds는 정수여야 합니다.")
        
        if 'retry_attempts' in api:
            try:
                retries = int(api['retry_attempts'])
                if retries < 0:
                    errors.append("api.retry_attempts는 0 이상의 정수여야 합니다.")
            except (ValueError, TypeError):
                errors.append("api.retry_attempts는 정수여야 합니다.")
    
    # 로깅 설정 검증
    if 'logging' in config:
        logging = config['logging']
        
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if 'level' in logging and logging['level'] not in valid_levels:
            errors.append(f"logging.level는 다음 중 하나여야 합니다: {', '.join(valid_levels)}")
    
    # 오류가 있으면 예외 발생
    if errors:
        error_msg = "설정 검증 오류:\n" + "\n".join(f"- {error}" for error in errors)
        raise ValueError(error_msg)


def get_config(env: Optional[str] = None) -> Dict[str, Any]:
    """
    환경 변수를 고려하여 최종 설정을 반환합니다.
    
    Args:
        env: 환경 이름 (None인 경우 APP_ENV 환경 변수 사용)
        
    Returns:
        환경 변수가 적용된 최종 설정 딕셔너리
    """
    # 환경 변수 파일 로드
    load_env_file()
    
    if env is None:
        env = os.getenv('APP_ENV', 'development')
    
    # 기본 설정 로드
    config = load_config()
    
    # 환경별 설정 적용
    if env in config.get('environment', {}):
        env_config = config['environment'][env]
        if isinstance(env_config, dict):
            config.update(env_config)
    
    # 환경 변수 오버라이드 적용
    config = apply_environment_overrides(config)
    
    # 설정 유효성 검사
    validate_config(config)
    
    return config


# 전역 설정 객체
try:
    CONFIG = get_config()
except Exception as e:
    print(f"⚠️  설정 로딩 오류: {e}")
    print("기본 설정을 사용합니다.")
    CONFIG = {
        'monitor': {'base_folders': ['/tmp'], 'scan_interval_minutes': 1},
        'database': {'type': 'sqlite', 'sqlite_path': 'data/app.db'},
        'api': {'endpoint': 'http://localhost:8000/upload'},
        'logging': {'level': 'INFO', 'file': 'logs/app.log'}
    }

# 설정 접근을 위한 편의 함수들
def get_monitor_config() -> Dict[str, Any]:
    """모니터링 설정을 반환합니다."""
    return CONFIG.get('monitor', {})

def get_database_config() -> Dict[str, Any]:
    """데이터베이스 설정을 반환합니다."""
    return CONFIG.get('database', {})

def get_api_config() -> Dict[str, Any]:
    """API 설정을 반환합니다."""
    return CONFIG.get('api', {})

def get_logging_config() -> Dict[str, Any]:
    """로깅 설정을 반환합니다."""
    return CONFIG.get('logging', {})

def get_file_processing_config() -> Dict[str, Any]:
    """파일 처리 설정을 반환합니다."""
    return CONFIG.get('file_processing', {})

def get_system_config() -> Dict[str, Any]:
    """시스템 설정을 반환합니다."""
    return CONFIG.get('system', {})


if __name__ == "__main__":
    # 설정 테스트
    print("로드된 설정:")
    print(yaml.dump(CONFIG, default_flow_style=False, allow_unicode=True))
    
    print("\n환경 변수 오버라이드 테스트:")
    print(f"DB_TYPE: {os.getenv('DB_TYPE', 'Not set')}")
    print(f"API_ENDPOINT: {os.getenv('API_ENDPOINT', 'Not set')}")
    print(f"LOG_LEVEL: {os.getenv('LOG_LEVEL', 'Not set')}")
    
    print("\n설정 유효성 검사:")
    try:
        validate_config(CONFIG)
        print("✅ 모든 설정이 유효합니다.")
    except ValueError as e:
        print(f"❌ 설정 검증 오류: {e}")
