import json
import sys
import os
from datetime import datetime, timedelta
from processor.data_processor import OptimizedDataProcessor
from downloader.file_downloader import OptimizedFileDownloader
from database.mongo_connector import OptimizedMongoConnector

def load_config(config_path: str = 'config.json'):
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Błąd ładowania konfiguracji: {e}")
        sys.exit(1)

def main():
    print("🚀 Uruchamianie automatu...")
    config = load_config()
    time_delta = 9
    date_now = datetime.now()
    data_start = (date_now + timedelta(days=1)).strftime('%Y-%m-%d')
    data_end = (date_now + timedelta(days=time_delta)).strftime('%Y-%m-%d')
    file_key = "file_5"
    file_config = config["pobierz"][file_key]
    mongo_config = config["database"]
    # Inicjalizacja połączenia z MongoDB
    mongo_connector = OptimizedMongoConnector(
        host=mongo_config['host'],
        port=mongo_config['port'],
        username=mongo_config['username'],
        password=mongo_config['password'],
        db_name=mongo_config['db_name']
    )
    if not mongo_connector.connect():
        print("Nie udało się połączyć z MongoDB!")
        sys.exit(1)
    # Pobieranie pliku
    downloader = OptimizedFileDownloader(
        url_template=file_config['url_template'],
        data_start=data_start,
        data_end=data_end
    )
    # Tu można dodać logikę pobierania i przetwarzania pliku, np.:
    # response = requests.get(downloader.url)
    # if downloader.validate_response(response):
    #     processor = OptimizedDataProcessor(...)
    #     processor.process_csv_content(response.content)
    print("Automat zakończył działanie.")

if __name__ == "__main__":
    main()
