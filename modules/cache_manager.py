"""
Cache manager para armazenar resultados localmente
"""
import sqlite3
import json
from pathlib import Path
from typing import Optional, Dict, List
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)

class CacheManager:
    """Gerencia cache local em SQLite"""
    
    def __init__(self, db_path: str = "cache.db"):
        """
        Inicializa o gerenciador de cache
        
        Args:
            db_path: Caminho do banco de dados SQLite
        """
        self.db_path = Path(db_path)
        self._init_db()
    
    def _init_db(self):
        """Inicializa banco de dados se não existir"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Tabela de cache de CEP
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cep_cache (
                    cep TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela de cache de geocoding
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS geocode_cache (
                    address_hash TEXT PRIMARY KEY,
                    address TEXT,
                    latitude REAL,
                    longitude REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela de processamentos
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT,
                    total_rows INTEGER,
                    processed_rows INTEGER,
                    fixed_ceps INTEGER,
                    found_coords INTEGER,
                    errors INTEGER,
                    status TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP
                )
            """)
            
            conn.commit()
    
    def get_cep(self, cep: str) -> Optional[Dict]:
        """Recupera CEP do cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT data FROM cep_cache WHERE cep = ?",
                (cep,)
            )
            result = cursor.fetchone()
            
            if result:
                try:
                    return json.loads(result[0])
                except json.JSONDecodeError:
                    return None
        
        return None
    
    def save_cep(self, cep: str, data: Dict):
        """Salva CEP no cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO cep_cache (cep, data) VALUES (?, ?)",
                (cep, json.dumps(data))
            )
            conn.commit()
    
    def get_coordinates(self, address: str) -> Optional[tuple]:
        """Recupera coordenadas do cache"""
        # Cria hash do endereço para usar como chave
        address_hash = hash(address)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT latitude, longitude FROM geocode_cache WHERE address_hash = ?",
                (address_hash,)
            )
            result = cursor.fetchone()
            
            if result:
                return (float(result[0]), float(result[1]))
        
        return None
    
    def save_coordinates(self, address: str, latitude: float, longitude: float):
        """Salva coordenadas no cache"""
        address_hash = hash(address)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO geocode_cache (address_hash, address, latitude, longitude) VALUES (?, ?, ?, ?)",
                (address_hash, address, latitude, longitude)
            )
            conn.commit()
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas do cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM cep_cache")
            cep_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM geocode_cache")
            geocode_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM processing_log WHERE status = 'completed'")
            completed_jobs = cursor.fetchone()[0]
        
        return {
            'cep_cache_entries': cep_count,
            'geocode_cache_entries': geocode_count,
            'completed_jobs': completed_jobs
        }
    
    def clear_old_cache(self, days: int = 30):
        """Remove entradas de cache antigas"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "DELETE FROM cep_cache WHERE created_at < ?",
                (cutoff_date,)
            )
            
            cursor.execute(
                "DELETE FROM geocode_cache WHERE created_at < ?",
                (cutoff_date,)
            )
            
            deleted = cursor.rowcount
            conn.commit()
        
        logger.info(f"Removidas {deleted} entradas de cache antigas")
