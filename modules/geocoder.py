"""
Geocoder para buscar latitude e longitude usando Nominatim (OpenStreetMap)
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from typing import Optional, Tuple, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Geocoder:
    """Busca coordenadas usando Nominatim (OpenStreetMap)"""
    
    BASE_URL = "https://nominatim.openstreetmap.org/search"
    TIMEOUT = 20  # Aumentado para 20s
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 3  # Nominatim é mais restritivo
    
    def __init__(self, rate_limit_delay: float = 1.5, app_name: str = "GeoGrafi"):
        """
        Inicializa o geocoder
        
        Args:
            rate_limit_delay: Delay mínimo entre requisições (Nominatim: 1.5s recomendado)
            app_name: Nome da aplicação para User-Agent
        """
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        self.app_name = app_name
        self.cache = {}
        self.headers = {
            'User-Agent': f'{app_name}/1.0 (GeoGrafi Address Geocoding Application)',
            'Accept': 'application/json',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'
        }
        
        # Cria session com retry automático
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(self.headers)
    
    def _apply_rate_limit(self):
        """Aplica rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def search_by_cep(self, cep: str, city: str = "", state: str = "BR") -> Optional[Tuple[float, float]]:
        """
        Busca coordenadas usando CEP
        
        Args:
            cep: CEP (com ou sem formatação)
            city: Cidade (opcional)
            state: Estado/País
            
        Returns:
            Tupla (latitude, longitude) ou None
        """
        cep_clean = ''.join(filter(str.isdigit, str(cep)))
        
        # Cria chave de cache
        cache_key = f"cep:{cep_clean}:{city}:{state}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Monta query
        if city:
            query = f"{cep_clean}, {city}, {state}"
        else:
            query = f"{cep_clean}, {state}"
        
        result = self._search(query)
        self.cache[cache_key] = result
        return result
    
    def search_by_address(self, street: str, number: str = "", neighborhood: str = "", 
                         city: str = "", state: str = "BR") -> Optional[Tuple[float, float]]:
        """
        Busca coordenadas usando endereço completo
        
        Args:
            street: Rua/Logradouro
            number: Número
            neighborhood: Bairro
            city: Cidade
            state: Estado
            
        Returns:
            Tupla (latitude, longitude) ou None
        """
        # Constrói query
        parts = [street]
        if number:
            parts.append(str(number))
        if neighborhood:
            parts.append(neighborhood)
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        
        query = ", ".join(parts)
        
        # Cria chave de cache
        cache_key = f"address:{query}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        result = self._search(query)
        self.cache[cache_key] = result
        return result
    
    def _search(self, query: str) -> Optional[Tuple[float, float]]:
        """
        Realiza busca genérica no Nominatim
        
        Args:
            query: String de busca
            
        Returns:
            Tupla (latitude, longitude) ou None
        """
        if not query or len(query.strip()) < 3:
            return None
        
        # Tenta várias vezes com retry
        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                self._apply_rate_limit()
                
                params = {
                    'q': query,
                    'format': 'json',
                    'limit': 1
                }
                
                # Usa session para melhor performance
                try:
                    response = self.session.get(
                        self.BASE_URL,
                        params=params,
                        timeout=(10, 30),  # (connect timeout, read timeout)
                        allow_redirects=True
                    )
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as conn_err:
                    # Fallback para requests direto
                    logger.warning(f"Session falhou, tentando requests direto: {type(conn_err).__name__}")
                    response = requests.get(
                        self.BASE_URL,
                        params=params,
                        headers=self.headers,
                        timeout=(10, 30),
                        allow_redirects=True
                    )
                
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        result = (float(data[0]['lat']), float(data[0]['lon']))
                        logger.info(f"Encontrado: {query} -> {result}")
                        return result
                    else:
                        logger.warning(f"Nenhum resultado para: {query}")
                        return None
                else:
                    logger.warning(f"Status {response.status_code} para: {query}")
                    if attempt < self.RETRY_ATTEMPTS - 1:
                        time.sleep(self.RETRY_DELAY)
                        continue
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro ao buscar {query}: {str(e)}")
                if attempt < self.RETRY_ATTEMPTS - 1:
                    time.sleep(self.RETRY_DELAY)
                    continue
        
        return None
