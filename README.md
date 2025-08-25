# Automat do przetwarzania danych PSE

Automat oparty na wzorcu, przetwarzający dane zgodnie z logiką starego skryptu (`old`).

## Struktura
- `database/` — obsługa bazy MongoDB
- `downloader/` — pobieranie plików
- `processor/` — przetwarzanie danych
- `config.json` — konfiguracja
- `main.py` — główny skrypt uruchamiający

## Uruchomienie
```bash
python main.py
```
