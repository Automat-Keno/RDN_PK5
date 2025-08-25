"""
Zoptymalizowany downloader do pobierania plików CSV z obsługą retry i walidacją
"""

import requests
import time
import re
from datetime import datetime, timedelta
from typing import Optional, Union
from requests.exceptions import RequestException, Timeout, ConnectionError

class OptimizedFileDownloader:
    """Zoptymalizowany downloader do pobierania plików CSV."""

    INITIAL_RETRY_DELAY = 300  # 5 minut na początku
    MAX_RETRY_DELAY = 1800     # Maksymalnie 30 minut między próbami
    MAX_RETRIES = 18           # Maksymalnie 18 prób (do 14:30)
    TIMEOUT = 60               # 60 sekund timeout

    def __init__(self, url_template: str, data_start: str, data_end: Optional[str] = None):
        self.url_template = url_template
        self.data_start = self.format_date_for_url(data_start)
        self.data_end = self.format_date_for_url(data_end) if data_end else None

    @property
    def url(self) -> str:
        """Generuje pełny URL na podstawie podanych danych."""
        if self.data_end:
            return self.url_template.format(data_start=self.data_start, data_end=self.data_end)
        return self.url_template.format(data_start=self.data_start)

    @staticmethod
    def format_date_for_url(date_string: str) -> str:
        """Konwertuje datę z formatu 'yyyy-MM-dd' na '%Y%m%d'."""
        if re.match(r'\d{8}', date_string):
            return date_string
        try:
            date_obj = datetime.strptime(date_string, '%Y-%m-%d')
            return date_obj.strftime('%Y%m%d')
        except ValueError:
            raise ValueError(f"Nieprawidłowy format daty: {date_string}")

    def validate_response(self, response: requests.Response) -> bool:
        """Waliduje odpowiedź serwera."""
        if response.status_code == 404:
            print(f"📭 Plik jeszcze niedostępny (404) - prawdopodobnie PSE jeszcze nie opublikowało danych")
            return False
        elif response.status_code != 200:
            print(f"❌ Błąd HTTP: {response.status_code}")
            return False
        content_type = response.headers.get('content-type', '').lower()
        if 'csv' not in content_type and 'text' not in content_type:
            print(f"⚠️  Ostrzeżenie: Nieoczekiwany content-type: {content_type}")
        if len(response.content) < 100:
            print(f"⚠️  Ostrzeżenie: Plik wydaje się być pusty lub niekompletny")
            return False
        return True
