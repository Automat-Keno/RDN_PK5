#!/usr/bin/env python3
"""
Zoptymalizowana wersja skryptu do pobierania i przetwarzania danych PSE
Przeznaczona do uruchamiania w GitHub Actions CI/CD
"""

import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from processor.data_processor import OptimizedDataProcessor
from downloader.file_downloader import OptimizedFileDownloader
from database.mongo_connector import OptimizedMongoConnector


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
        
        return json.loads(config_str)
    except FileNotFoundError:
        print(f"Błąd: Plik konfiguracyjny {config_path} nie został znaleziony")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Błąd: Nieprawidłowy format JSON w pliku {config_path}: {e}")
        sys.exit(1)


def get_target_date() -> str:
    """Zwraca datę docelową (jutro) w formacie YYYY-MM-DD."""
    tomorrow = datetime.now() + timedelta(days=1)
    return tomorrow.strftime('%Y-%m-%d')


def main():
    """Główna funkcja aplikacji."""
    print("🚀 Uruchamianie zoptymalizowanego skryptu PSE...")

    # Ładowanie konfiguracji
    config = load_config()

    # Ustawienie daty docelowej
    target_date = get_target_date()
    print(f"📅 Pobieranie danych dla daty: {target_date}")

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
    file_key = "file_2"
    file_config = config["pobierz"][file_key]

    try:
        # Pobieranie danych
        print("📥 Pobieranie danych z PSE...")
        downloader = OptimizedFileDownloader(
            url_template=file_config["url_template"],
            data_start=target_date
        )

        # Pobieranie i przetwarzanie danych w jednym kroku
        processor = OptimizedDataProcessor(
            url_template=file_config["url_template"],
            data_start=target_date,
            int_cols=file_config["int_cols"],
            float_cols=file_config["float_cols"],
            date_cols=file_config["date_cols"],
            fields_to_utc=file_config.get("fields_to_utc", []),
            fields_to_add_hour=file_config.get("fields_to_add_hour", {}),
            mongo_connector=mongo_connector,
            kolekcja_mongo=file_config["kolekcja_mongo"],
            date_format=file_config.get("date_format", "%Y%m%d")
        )

        # Uruchomienie przetwarzania
        success = processor.process_and_save()

        if success:
            print("✅ Dane zostały pomyślnie pobrane i zapisane do bazy danych")
            return 0
        else:
            print("❌ Wystąpił błąd podczas przetwarzania danych")
            return 1

    except Exception as e:
        print(f"❌ Błąd krytyczny: {e}")
        return 1
    finally:
        # Zamykanie połączenia z bazą danych
        mongo_connector.disconnect()


if __name__ == "__main__":
    sys.exit(main())
