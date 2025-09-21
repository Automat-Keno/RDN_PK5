"""
Prosty downloader z retry – zgodny z wcześniejszą, działającą wersją.
"""

import requests
import time
import re
from datetime import datetime
from typing import Optional
from requests.exceptions import RequestException


class OptimizedFileDownloader:
    """Downloader do pobierania plików CSV z prostym retry."""

    RETRY_DELAY = 5 * 60  # 5 minut
    MAX_RETRIES = 10      # maksymalnie 10 prób

    def __init__(self, url_template: str, data_start: str, data_end: Optional[str] = None):
        self.url_template = url_template
        self.data_start = self.format_date_for_url(data_start)
        self.formatted_data_start = data_start
        self.data_end = self.format_date_for_url(
            data_end) if data_end else None

    @property
    def url(self) -> str:
        """Generuje pełny URL na podstawie podanych danych."""
        if self.data_end:
            return self.url_template.format(data_start=self.data_start, data_end=self.data_end)
        return self.url_template.format(data_start=self.data_start)

    @staticmethod
    def format_date_for_url(date_string: str) -> str:
        """Konwertuje datę z 'yyyy-MM-dd' na '%Y%m%d', jeśli potrzeba."""
        if re.match(r"\d{8}", date_string):
            return date_string
        try:
            date_obj = datetime.strptime(date_string, "%Y-%m-%d")
            return date_obj.strftime("%Y%m%d")
        except ValueError:
            # Zwróć komunikat jak w poprzedniej wersji (działała na tych danych)
            return "Nieprawidłowy format daty"

    def download(self) -> Optional[bytes]:
        """Pobiera plik z określonego URL i zwraca jego zawartość."""
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                response = requests.get(self.url)
                response.raise_for_status()
                return response.content
            except RequestException as e:
                retries += 1
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"❌ Błąd pobierania ({retries}/{self.MAX_RETRIES}) o {current_time}: {e}")
                if retries >= self.MAX_RETRIES:
                    raise Exception(
                        f"Przekroczono maksymalną ilość prób pobierania pliku z {self.url}. Ostatni błąd: {e}"
                    )
                time.sleep(self.RETRY_DELAY)
