"""
API 클라이언트 서비스
외부 API 엔드포인트와 통신하여 파일을 업로드합니다.
"""

import os
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse
import mimetypes

# 프로젝트 루트를 Python 경로에 추가
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import LoggerManager
from config.settings import get_config


class APIClient:
    """API 엔드포인트와 통신하는 클라이언트"""
    
    def __init__(self):
        # 설정 로드
        self.config = get_config()
        
        # 로거 설정
        self.logger_manager = LoggerManager()
        self.logger = self.logger_manager.get_logger(__name__)
        
        # API 설정
        self.api_config = self.config.get('api', {})
        self.base_url = self.api_config.get('endpoint', 'http://211.231.137.111:18000')
        self.upload_endpoint = self.api_config.get('upload_endpoint', '/upload')
        self.timeout = self.api_config.get('timeout_seconds', 30)
        self.max_retries = self.api_config.get('retry_attempts', 3)
        self.retry_delay = self.api_config.get('retry_delay_seconds', 5)
        self.max_file_size = self.api_config.get('max_file_size', 10 * 1024 * 1024)  # 10MB
        
        # HTTP 세션
        self.session = requests.Session()
        
        # 기본 헤더 설정
        self.session.headers.update({
            'User-Agent': 'FileMonitor/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        # 연결 풀 설정
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0  # 자체 재시도 로직 사용
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # API 상태
        self.is_available = False
        self.last_check = None
        self.check_interval = 300  # 5분마다 상태 확인
        
        # 초기화
        self._validate_config()
        self._check_api_availability()
    
    def _validate_config(self):
        """API 설정을 검증합니다."""
        if not self.base_url:
            raise ValueError("API base_url이 설정되지 않았습니다.")
        
        if not self.upload_endpoint:
            raise ValueError("API upload_endpoint가 설정되지 않았습니다.")
        
        if self.timeout <= 0:
            raise ValueError("API timeout은 양수여야 합니다.")
        
        if self.max_retries < 0:
            raise ValueError("API max_retries는 0 이상이어야 합니다.")
        
        if self.retry_delay < 0:
            raise ValueError("API retry_delay는 0 이상이어야 합니다.")
        
        self.logger.info(f"API 설정 검증 완료: {self.base_url}{self.upload_endpoint}")
    
    def _check_api_availability(self) -> bool:
        """API 서버의 가용성을 확인합니다."""
        try:
            # 현재 시간 확인
            current_time = time.time()
            if (self.last_check and 
                current_time - self.last_check < self.check_interval):
                return self.is_available
            
            self.logger.debug("API 가용성 확인 중...")
            
            # 간단한 HEAD 요청으로 서버 상태 확인
            response = self.session.head(
                self.base_url,
                timeout=10,
                allow_redirects=True
            )
            
            if response.status_code in [200, 301, 302, 307, 308]:
                self.is_available = True
                self.logger.debug("API 서버 가용성 확인 완료")
            else:
                self.is_available = False
                self.logger.warning(f"API 서버 응답 코드: {response.status_code}")
            
            self.last_check = current_time
            return self.is_available
            
        except requests.exceptions.RequestException as e:
            self.is_available = False
            self.logger.warning(f"API 서버 연결 실패: {str(e)}")
            self.last_check = current_time
            return False
    
    def upload_file(self, file_path: Union[str, Path], 
                   metadata: Optional[Dict] = None) -> Dict:
        """파일을 API 엔드포인트에 업로드합니다."""
        try:
            file_path = Path(file_path)
            
            # 파일 존재 확인
            if not file_path.exists():
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
            
            # 파일 크기 확인
            file_size = file_path.stat().st_size
            if file_size > self.max_file_size:
                raise ValueError(f"파일 크기가 너무 큽니다: {file_size} bytes (최대: {self.max_file_size} bytes)")
            
            # API 가용성 확인
            if not self._check_api_availability():
                raise ConnectionError("API 서버에 연결할 수 없습니다.")
            
            self.logger.info(f"파일 업로드 시작: {file_path} ({file_size} bytes)")
            
            # 파일 업로드 수행
            response = self._perform_upload(file_path, metadata)
            
            # 응답 처리
            if response['success']:
                self.logger.info(f"파일 업로드 성공: {file_path}")
                return {
                    'success': True,
                    'data': response['data'],
                    'file_path': str(file_path),
                    'file_size': file_size,
                    'upload_time': datetime.now()
                }
            else:
                error_msg = response.get('error', '알 수 없는 오류')
                self.logger.error(f"파일 업로드 실패: {file_path} - {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'file_path': str(file_path),
                    'file_size': file_size,
                    'upload_time': datetime.now()
                }
            
        except Exception as e:
            error_msg = f"파일 업로드 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("상세 에러 정보:")
            
            return {
                'success': False,
                'error': error_msg,
                'file_path': str(file_path) if 'file_path' in locals() else None,
                'upload_time': datetime.now()
            }
    
    def _perform_upload(self, file_path: Path, 
                       metadata: Optional[Dict] = None) -> Dict:
        """실제 파일 업로드를 수행합니다."""
        upload_url = urljoin(self.base_url, self.upload_endpoint)
        
        # 파일 MIME 타입 확인
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            mime_type = 'application/octet-stream'
        
        # 파일 데이터 준비
        files = {
            'file': (
                file_path.name,
                open(file_path, 'rb'),
                mime_type
            )
        }
        
        # 추가 메타데이터
        data = {}
        if metadata:
            data.update(metadata)
        
        # 파일 정보 추가
        data.update({
            'filename': file_path.name,
            'file_size': file_path.stat().st_size,
            'upload_time': datetime.now().isoformat()
        })
        
        # 재시도 로직
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.debug(f"업로드 시도 {attempt + 1}/{self.max_retries + 1}: {file_path}")
                
                response = self.session.post(
                    upload_url,
                    files=files,
                    data=data,
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                # 응답 처리
                return self._process_response(response, file_path)
                
            except requests.exceptions.Timeout:
                error_msg = f"업로드 타임아웃 (시도 {attempt + 1}/{self.max_retries + 1})"
                self.logger.warning(error_msg)
                
                if attempt < self.max_retries:
                    self.logger.info(f"{self.retry_delay}초 후 재시도...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return {
                        'success': False,
                        'error': '업로드 타임아웃 - 최대 재시도 횟수 초과'
                    }
                
            except requests.exceptions.ConnectionError as e:
                error_msg = f"연결 오류 (시도 {attempt + 1}/{self.max_retries + 1}): {str(e)}"
                self.logger.warning(error_msg)
                
                if attempt < self.max_retries:
                    self.logger.info(f"{self.retry_delay}초 후 재시도...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return {
                        'success': False,
                        'error': f'연결 오류 - 최대 재시도 횟수 초과: {str(e)}'
                    }
                
            except requests.exceptions.RequestException as e:
                error_msg = f"요청 오류 (시도 {attempt + 1}/{self.max_retries + 1}): {str(e)}"
                self.logger.warning(error_msg)
                
                if attempt < self.max_retries:
                    self.logger.info(f"{self.retry_delay}초 후 재시도...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return {
                        'success': False,
                        'error': f'요청 오류 - 최대 재시도 횟수 초과: {str(e)}'
                    }
                
            finally:
                # 파일 핸들 정리
                if 'file' in files:
                    files['file'][1].close()
        
        return {
            'success': False,
            'error': '알 수 없는 오류로 업로드 실패'
        }
    
    def _process_response(self, response: requests.Response, file_path: Path) -> Dict:
        """API 응답을 처리합니다."""
        try:
            # HTTP 상태 코드 확인
            if response.status_code == 200:
                # 성공 응답 처리
                try:
                    response_data = response.json()
                    return {
                        'success': True,
                        'data': response_data,
                        'http_status': response.status_code,
                        'headers': dict(response.headers)
                    }
                except ValueError:
                    # JSON 파싱 실패 시 텍스트 응답 처리
                    text_response = response.text.strip()
                    self.logger.warning(f"JSON 응답 파싱 실패, 텍스트 응답: {text_response}")
                    
                    # 텍스트 응답에서 정보 추출 시도
                    parsed_data = self._parse_text_response(text_response)
                    return {
                        'success': True,
                        'data': parsed_data,
                        'http_status': response.status_code,
                        'headers': dict(response.headers),
                        'raw_response': text_response
                    }
            
            elif response.status_code in [201, 202]:
                # 성공 응답 (생성됨, 수락됨)
                try:
                    response_data = response.json()
                    return {
                        'success': True,
                        'data': response_data,
                        'http_status': response.status_code,
                        'headers': dict(response.headers)
                    }
                except ValueError:
                    return {
                        'success': True,
                        'data': {'message': 'File uploaded successfully'},
                        'http_status': response.status_code,
                        'headers': dict(response.headers)
                    }
            
            elif response.status_code == 400:
                # 잘못된 요청
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', 'Bad Request')
                except ValueError:
                    error_msg = response.text.strip() or 'Bad Request'
                
                return {
                    'success': False,
                    'error': f'잘못된 요청: {error_msg}',
                    'http_status': response.status_code,
                    'headers': dict(response.headers)
                }
            
            elif response.status_code == 401:
                # 인증 실패
                return {
                    'success': False,
                    'error': '인증 실패 - API 키 또는 인증 정보가 올바르지 않습니다.',
                    'http_status': response.status_code,
                    'headers': dict(response.headers)
                }
            
            elif response.status_code == 403:
                # 권한 없음
                return {
                    'success': False,
                    'error': '권한 없음 - 이 작업을 수행할 권한이 없습니다.',
                    'http_status': response.status_code,
                    'headers': dict(response.headers)
                }
            
            elif response.status_code == 413:
                # 파일 크기 초과
                return {
                    'success': False,
                    'error': '파일 크기 초과 - 업로드하려는 파일이 너무 큽니다.',
                    'http_status': response.status_code,
                    'headers': dict(response.headers)
                }
            
            elif response.status_code == 429:
                # 요청 빈도 초과
                return {
                    'success': False,
                    'error': '요청 빈도 초과 - 너무 많은 요청을 보내고 있습니다.',
                    'http_status': response.status_code,
                    'headers': dict(response.headers)
                }
            
            elif response.status_code >= 500:
                # 서버 오류
                return {
                    'success': False,
                    'error': f'서버 오류 (HTTP {response.status_code}) - 서버에 문제가 있습니다.',
                    'http_status': response.status_code,
                    'headers': dict(response.headers)
                }
            
            else:
                # 기타 상태 코드
                return {
                    'success': False,
                    'error': f'예상치 못한 응답 (HTTP {response.status_code})',
                    'http_status': response.status_code,
                    'headers': dict(response.headers),
                    'response_text': response.text[:500]  # 처음 500자만
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f'응답 처리 중 오류 발생: {str(e)}',
                'http_status': getattr(response, 'status_code', None),
                'headers': dict(response.headers) if hasattr(response, 'headers') else {}
            }
    
    def _parse_text_response(self, text: str) -> Dict:
        """텍스트 응답에서 정보를 추출합니다."""
        parsed_data = {}
        
        # 일반적인 응답 패턴들 확인
        if 'success' in text.lower() or 'uploaded' in text.lower():
            parsed_data['message'] = 'File uploaded successfully'
            parsed_data['status'] = 'success'
        
        elif 'error' in text.lower() or 'fail' in text.lower():
            parsed_data['message'] = text.strip()
            parsed_data['status'] = 'error'
        
        else:
            # 기본 정보 설정
            parsed_data['message'] = text.strip()
            parsed_data['status'] = 'unknown'
        
        return parsed_data
    
    def get_api_info(self) -> Dict:
        """API 정보를 반환합니다."""
        return {
            'base_url': self.base_url,
            'upload_endpoint': self.upload_endpoint,
            'full_upload_url': urljoin(self.base_url, self.upload_endpoint),
            'timeout': self.timeout,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay,
            'max_file_size': self.max_file_size,
            'is_available': self.is_available,
            'last_check': self.last_check,
            'session_headers': dict(self.session.headers)
        }
    
    def test_connection(self) -> Dict:
        """API 연결을 테스트합니다."""
        try:
            self.logger.info("API 연결 테스트 시작")
            
            # API 가용성 확인
            is_available = self._check_api_availability()
            
            if is_available:
                # 간단한 HEAD 요청으로 응답 시간 측정
                start_time = time.time()
                response = self.session.head(
                    self.base_url,
                    timeout=10,
                    allow_redirects=True
                )
                response_time = (time.time() - start_time) * 1000  # 밀리초
                
                return {
                    'success': True,
                    'is_available': True,
                    'response_time_ms': round(response_time, 2),
                    'http_status': response.status_code,
                    'server_info': response.headers.get('Server', 'Unknown'),
                    'message': 'API 서버에 성공적으로 연결되었습니다.'
                }
            else:
                return {
                    'success': False,
                    'is_available': False,
                    'message': 'API 서버에 연결할 수 없습니다.'
                }
                
        except Exception as e:
            return {
                'success': False,
                'is_available': False,
                'error': str(e),
                'message': f'API 연결 테스트 중 오류 발생: {str(e)}'
            }
    
    def update_config(self, **kwargs):
        """API 설정을 동적으로 업데이트합니다."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
                self.logger.info(f"API 설정 업데이트: {key} = {value}")
            else:
                self.logger.warning(f"알 수 없는 설정 키: {key}")
        
        # 설정 변경 후 API 가용성 재확인
        self._check_api_availability()
    
    def close(self):
        """API 클라이언트를 종료합니다."""
        try:
            self.session.close()
            self.logger.info("API 클라이언트 세션 종료")
        except Exception as e:
            self.logger.error(f"API 클라이언트 종료 중 오류: {str(e)}")


# 테스트 코드
if __name__ == "__main__":
    print("APIClient 테스트:")
    
    try:
        # API 클라이언트 생성
        api_client = APIClient()
        
        # API 정보 출력
        api_info = api_client.get_api_info()
        print(f"API 정보: {api_info}")
        
        # 연결 테스트
        print("\nAPI 연결 테스트 중...")
        test_result = api_client.test_connection()
        print(f"연결 테스트 결과: {test_result}")
        
        print("✅ APIClient 설정 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
