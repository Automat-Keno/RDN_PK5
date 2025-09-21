"""
Downloader z retry, rozszerzony o pobieranie JSON z API PSE.
"""

import requests
import time
import re
from datetime import datetime
from typing import Optional
from requests.exceptions import RequestException


class OptimizedFileDownloader:
    """Downloader do pobierania plików/JSON z prostym retry."""

    RETRY_DELAY = 5 * 60  # 5 minut
    MAX_RETRIES = 10      # maksymalnie 10 prób

    def __init__(self, url_template: str, data_start: str, data_end: Optional[str] = None):
        self.url_template = url_template
        # CSV-owe rzeczy zostawiamy zgodnie z wcześniejszą wersją:
        self.data_start = self.format_date_for_url(data_start)
        # Zapamiętujemy też oryginał YYYY-MM-DD pod JSON:
        self.formatted_data_start = data_start
        # Dla JSON NIE konwertujemy na %Y%m%d
        self.data_end = data_end if data_end else None

    @property
    def url(self) -> str:
        """Generuje pełny URL (CSV wariant)."""
        if self.data_end:
            return self.url_template.format(data_start=self.data_start, data_end=self.data_end)
        return self.url_template.format(data_start=self.data_start)

    @staticmethod
    def format_date_for_url(date_string: str) -> str:
        """Konwertuje datę z 'yyyy-MM-dd' na '%Y%m%d', jeśli potrzeba (CSV wariant)."""
        if re.match(r"\d{8}", date_string or ""):
            return date_string
        try:
            from datetime import datetime as _dt
            date_obj = _dt.strptime(date_string, "%Y-%m-%d")
            return date_obj.strftime("%Y%m%d")
        except Exception:
            return "Nieprawidłowy format daty"

    def download(self) -> Optional[bytes]:
        """Pobiera zawartość (CSV) pod self.url i zwraca bytes."""
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                response = requests.get(self.url)
                response.raise_for_status()
                return response.content
            except RequestException as e:
                retries += 1
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"❌ Błąd pobierania ({retries}/{self.MAX_RETRIES}) o {current_time}: {e}")
                if retries >= self.MAX_RETRIES:
                    raise Exception(
                        f"Przekroczono maksymalną ilość prób pobierania pliku z {self.url}. Ostatni błąd: {e}"
                    )
                time.sleep(self.RETRY_DELAY)

    def download_json(self) -> dict:
        """Pobiera JSON z API PSE (używa dat w formacie YYYY-MM-DD w url_template)."""
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                url = self.url_template.format(
                    data_start=self.formatted_data_start,
                    data_end=self.data_end
                )
                response = requests.get(url)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                retries += 1
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"❌ Błąd pobierania JSON ({retries}/{self.MAX_RETRIES}) o {current_time}: {e}")
                if retries >= self.MAX_RETRIES:
                    raise Exception(
                        f"Przekroczono maksymalną ilość prób pobierania z API. Ostatni błąd: {e}"
                    )
                time.sleep(self.RETRY_DELAY)
