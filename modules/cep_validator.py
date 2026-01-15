"""
Validador e buscador de CEP usando ViaCEP
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import subprocess
import json as json_lib
import time
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)

class CEPValidator:
    """Valida e busca informações de CEP"""
    
    BASE_URL = "https://viacep.com.br/ws"
    TIMEOUT = 30  # Aumentado para 30s
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # Aumentado para 2s
    
    def __init__(self, rate_limit_delay: float = 0.1):
        """
        Inicializa o validador de CEP
        
        Args:
            rate_limit_delay: Delay em segundos entre requisições
        """
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        self.cache = {}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        # Cria session com retry automático
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
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
    
    def _get_via_curl(self, url: str) -> Optional[Dict]:
        """Fallback usando curl via subprocess"""
        try:
            cmd = [
                'curl', '-s', '--connect-timeout', '10', '--max-time', '30',
                '-H', 'User-Agent: GeoGrafi/1.0',
                '-H', 'Accept: application/json',
                url
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=35)
            if result.returncode == 0 and result.stdout:
                return json_lib.loads(result.stdout)
        except Exception as e:
            logger.warning(f"Curl fallback falhou: {e}")
        return None
    
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
                
                # Tenta com session primeiro
                try:
                    response = self.session.get(
                        url, 
                        timeout=(10, 30),  # (connect timeout, read timeout)
                        allow_redirects=True,
                        verify=True
                    )
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as conn_err:
                    # Se falhar, tenta com requests simples
                    logger.warning(f"Tentativa {attempt + 1}/{self.RETRY_ATTEMPTS} - Session falhou, tentando requests direto: {type(conn_err).__name__}")
                    response = requests.get(
                        url,
                        headers=self.headers,
                        timeout=(10, 30),
                        allow_redirects=True,
                        verify=True
                    )
                
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
                error_type = type(e).__name__
                logger.warning(f"Tentativa {attempt + 1}/{self.RETRY_ATTEMPTS} - Erro ao buscar CEP {cep}: {error_type}: {str(e)[:100]}")
                
                # Tenta curl como último recurso na primeira tentativa
                if attempt == 0:
                    logger.info(f"🔄 Tentando curl fallback para CEP {cep}...")
                    data = self._get_via_curl(url)
                    if data and not data.get('erro'):
                        self.cache[cep_clean] = data
                        logger.info(f"✅ CEP {cep} obtido via curl")
                        return data
                
                if attempt < self.RETRY_ATTEMPTS - 1:
                    time.sleep(self.RETRY_DELAY)
                    continue
                else:
                    logger.error(f"CEP {cep} falhou após {self.RETRY_ATTEMPTS} tentativas (requests + curl)")
        
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
