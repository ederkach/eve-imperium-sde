import time
import requests
import urllib3
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class RetryableHTTPClient:
    def __init__(self, max_retries: int = 5, retry_delay: float = 3.0,
                 default_timeout: int = 30, verify: bool = False):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.default_timeout = default_timeout
        self.verify = verify
        self.session = requests.Session()
        self.session.verify = verify

    def get(self, url: str, **kwargs) -> requests.Response:
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.default_timeout
        if 'verify' not in kwargs:
            kwargs['verify'] = self.verify

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    print(f"[-] Request failed (attempt {attempt + 1}/{self.max_retries}): {url}")
                    print(f"[-] Error: {str(e)}")
                    print(f"[+] Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"[x] Request failed, max retries reached ({self.max_retries}): {url}")
        raise last_exception

    def head(self, url: str, **kwargs) -> requests.Response:
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.default_timeout
        if 'verify' not in kwargs:
            kwargs['verify'] = self.verify

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.head(url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    print(f"[-] Request failed (attempt {attempt + 1}/{self.max_retries}): {url}")
                    time.sleep(self.retry_delay)
                else:
                    print(f"[x] Request failed, max retries reached ({self.max_retries}): {url}")
        raise last_exception

    def post(self, url: str, **kwargs) -> requests.Response:
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.default_timeout
        if 'verify' not in kwargs:
            kwargs['verify'] = self.verify

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    print(f"[-] Request failed (attempt {attempt + 1}/{self.max_retries}): {url}")
                    time.sleep(self.retry_delay)
                else:
                    print(f"[x] Request failed, max retries reached ({self.max_retries}): {url}")
        raise last_exception

    def close(self):
        self.session.close()


_default_client = RetryableHTTPClient()


def get(url: str, **kwargs) -> requests.Response:
    return _default_client.get(url, **kwargs)


def head(url: str, **kwargs) -> requests.Response:
    return _default_client.head(url, **kwargs)


def post(url: str, **kwargs) -> requests.Response:
    return _default_client.post(url, **kwargs)


def create_session(max_retries: int = 5, retry_delay: float = 3.0,
                   default_timeout: int = 30, verify: bool = False) -> RetryableHTTPClient:
    return RetryableHTTPClient(
        max_retries=max_retries,
        retry_delay=retry_delay,
        default_timeout=default_timeout,
        verify=verify
    )
