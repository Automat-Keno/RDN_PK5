#!/usr/bin/env python3
"""
Zoptymalizowana wersja skryptu do pobierania i przetwarzania danych PSE (API JSON)
Przeznaczona do uruchamiania lokalnie i w GitHub Actions CI/CD
"""

import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import pytz

from data_processor import OptimizedDataProcessor
from file_downloader import OptimizedFileDownloader
from mongo_connector import OptimizedMongoConnector


def load_config(config_path: str = 'config.json') -> Dict[str, Any]:
    """Ładuje konfigurację z pliku JSON z obsługą zmiennych środowiskowych."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # Podmiana zmiennych środowiskowych
        config_str = json.dumps(config)
        for key, value in os.environ.items():
            if key.startswith('MONGODB_'):
                config_str = config_str.replace(f'${{{key}}}', value)

        parsed_config = json.loads(config_str)

        # Konwersja port na int jeśli zostało podmienione
        if 'database' in parsed_config and 'port' in parsed_config['database']:
            if isinstance(parsed_config['database']['port'], str):
                try:
                    parsed_config['database']['port'] = int(parsed_config['database']['port'])
                except (ValueError, TypeError):
                    print("⚠️  Błędny format portu, używam 27017")
                    parsed_config['database']['port'] = 27017

        return parsed_config
    except FileNotFoundError:
        print(f"Błąd: Plik konfiguracyjny {config_path} nie został znaleziony")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Błąd: Nieprawidłowy format JSON w pliku {config_path}: {e}")
        sys.exit(1)


def get_target_date() -> str:
    """Zwraca datę docelową (dzisiaj w strefie Europe/Warsaw) w formacie YYYY-MM-DD."""
    warsaw = pytz.timezone('Europe/Warsaw')
    today_warsaw = datetime.now(warsaw).date()
    return today_warsaw.strftime('%Y-%m-%d')


def main():
    """Główna funkcja aplikacji."""
    print("🚀 Uruchamianie skryptu PSE (API JSON)...")

    # Ładowanie konfiguracji
    config = load_config()

    # Ustawienie daty docelowej i zakresu
    data_start = get_target_date()
    data_end = (datetime.strptime(data_start, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"📅 Zakres: {data_start} 00:00 → {data_end} 00:00 (czas lokalny Warszawa)")

    # Konfiguracja bazy danych
    mongo_config = config["database"]
    mongo_connector = OptimizedMongoConnector(
        host=mongo_config['host'],
        port=mongo_config['port'],
        username=mongo_config['username'],
        password=mongo_config['password'],
        db_name=mongo_config['db_name']
    )

    # Konfiguracja pobierania danych
    file_key = "file_5"
    file_config = config["pobierz"][file_key]

    try:
        # Pobieranie danych
        print("📥 Pobieranie danych z API PSE...")
        downloader = OptimizedFileDownloader(
            url_template=file_config["url_template"],
            data_start=data_start,
            data_end=data_end
        )

        # Procesor: mapowanie, typy, Mongo
        processor = OptimizedDataProcessor(
            url_template=file_config["url_template"],
            data_start=data_start,
            int_cols=file_config["int_cols"],
            float_cols=file_config["float_cols"],
            date_cols=file_config["date_cols"],
            fields_to_utc=file_config.get("fields_to_utc", []),
            fields_to_add_hour=file_config.get("fields_to_add_hour", {}),
            mongo_connector=mongo_connector,
            kolekcja_mongo=file_config["kolekcja_mongo"],
            date_format=file_config.get("date_format", "%Y-%m-%d")
        )

        # JSON flow
        json_data = downloader.download_json()
        if json_data:
            processed_data = processor.process_json_value(json_data)
            print(f"✅ Pobrano i przetworzono {len(processed_data)} wierszy danych")
            return 0
        else:
            print("❌ Nie udało się pobrać danych")
            return 1

    except Exception as e:
        print(f"❌ Błąd krytyczny: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
