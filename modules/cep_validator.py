"""
Validador e buscador de CEP usando ViaCEP
"""
import requests
import time
from typing import Optional, Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CEPValidator:
    """Valida e busca informações de CEP"""
    
    BASE_URL = "https://viacep.com.br/ws"
    TIMEOUT = 10
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1
    
    def __init__(self, rate_limit_delay: float = 0.1):
        """
        Inicializa o validador de CEP
        
        Args:
            rate_limit_delay: Delay em segundos entre requisições
        """
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        self.cache = {}
    
    def _apply_rate_limit(self):
        """Aplica rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def search_cep(self, cep: str) -> Optional[Dict]:
        """
        Busca informações de um CEP
        
        Args:
            cep: CEP sem formatação (8 dígitos)
            
        Returns:
            Dict com informações do CEP ou None se inválido
        """
        if not cep:
            return None
        
        # Remove caracteres especiais
        cep_clean = ''.join(filter(str.isdigit, str(cep)))
        
        if len(cep_clean) != 8:
            logger.warning(f"CEP inválido (tamanho): {cep}")
            return None
        
        # Verifica cache
        if cep_clean in self.cache:
            return self.cache[cep_clean]
        
        # Busca na API com retry
        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                self._apply_rate_limit()
                
                url = f"{self.BASE_URL}/{cep_clean}/json/"
                response = requests.get(url, timeout=self.TIMEOUT)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Verifica se é um erro da API
                    if data.get('erro'):
                        logger.warning(f"CEP não encontrado: {cep}")
                        self.cache[cep_clean] = None
                        return None
                    
                    # Cache o resultado
                    self.cache[cep_clean] = data
                    return data
                else:
                    if attempt < self.RETRY_ATTEMPTS - 1:
                        time.sleep(self.RETRY_DELAY)
                        continue
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro ao buscar CEP {cep}: {str(e)}")
                if attempt < self.RETRY_ATTEMPTS - 1:
                    time.sleep(self.RETRY_DELAY)
                    continue
        
        return None
    
    def validate_cep_format(self, cep: str) -> bool:
        """Valida formato do CEP"""
        cep_clean = ''.join(filter(str.isdigit, str(cep)))
        return len(cep_clean) == 8
    
    def format_cep(self, cep: str) -> str:
        """Formata CEP para padrão XXXXX-XXX"""
        cep_clean = ''.join(filter(str.isdigit, str(cep)))
        if len(cep_clean) == 8:
            return f"{cep_clean[:5]}-{cep_clean[5:]}"
        return cep
