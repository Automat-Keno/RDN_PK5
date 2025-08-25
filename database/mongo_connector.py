"""
Zoptymalizowany łącznik MongoDB z connection pooling i lepszą obsługą błędów
"""

import datetime
from typing import Dict, Any, Optional, List
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure

class OptimizedMongoConnector:
    """Zoptymalizowany łącznik MongoDB z connection pooling."""

    def __init__(self, host: str = 'localhost', port: int = 27017, 
                 username: Optional[str] = None, password: Optional[str] = None, 
                 db_name: Optional[str] = None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        
        self.client = None
        self.db = None
        self._connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Buduje connection string dla MongoDB."""
        if self.username and self.password:
            return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}?authSource={self.db_name}"
        else:
            return f"mongodb://{self.host}:{self.port}/{self.db_name}"

    def connect(self) -> bool:
        """Nawiązuje połączenie z bazą MongoDB z connection pooling."""
        try:
            self.client = MongoClient(
                self._connection_string,
                serverSelectionTimeoutMS=5000,  # 5 sekund timeout
                connectTimeoutMS=10000,         # 10 sekund timeout połączenia
                socketTimeoutMS=30000,          # 30 sekund timeout socket
                maxPoolSize=10,                 # Maksymalny rozmiar pool
                minPoolSize=1,                  # Minimalny rozmiar pool
                maxIdleTimeMS=30000,            # 30 sekund idle time
                retryWrites=True,               # Retry dla operacji zapisu
                retryReads=True                 # Retry dla operacji odczytu
            )
            # Test połączenia
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print("✅ Połączenie z MongoDB nawiązane pomyślnie")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            print(f"❌ Błąd połączenia z MongoDB: {e}")
            return False
        except Exception as e:
            print(f"❌ Nieoczekiwany błąd podczas łączenia z MongoDB: {e}")
            return False
