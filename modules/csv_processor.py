"""
Processador de CSV em chunks para dados de alta volume
"""
import pandas as pd
from typing import Iterator, Callable, Optional, Dict, List
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import chardet

from .cep_validator import CEPValidator
from .geocoder import Geocoder
from .cache_manager import CacheManager

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)

class CSVProcessor:
    """Processa arquivos CSV em chunks com enriquecimento de dados geográficos"""
    
    def __init__(
        self,
        chunk_size: int = 1000,
        max_workers: int = 3,
        use_cache: bool = True,
        cache_db: str = "cache.db",
        col_mapping: Optional[Dict[str, str]] = None
    ):
        """
        Inicializa o processador
        
        Args:
            chunk_size: Número de linhas por chunk
            max_workers: Número de workers para processamento paralelo
            use_cache: Usar cache local
            cache_db: Caminho do banco de cache
            col_mapping: Mapeamento de colunas alternativas -> nomes esperados
        """
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.cep_validator = CEPValidator(rate_limit_delay=0.15)
        self.geocoder = Geocoder(rate_limit_delay=1.5)
        self.cache_manager = CacheManager(cache_db) if use_cache else None
        self.col_mapping = col_mapping or {}
        self.detected_encoding = None
        self.detected_delimiter = None
        
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'fixed_ceps': 0,
            'found_coordinates': 0,
            'errors': []
        }
    
    def _detect_encoding(self, file_path: str, sample_size: int = 100000) -> str:
        """Detecta o encoding do arquivo automaticamente"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)
                result = chardet.detect(raw_data)
                detected = result.get('encoding')
                confidence = result.get('confidence', 0.0)
                
                # Se não detectar com confiança, usar latin-1 (preserva todos bytes)
                if not detected or confidence < 0.5:
                    logger.warning(f"Encoding não confiável ({detected}, conf.: {confidence:.2%}). Usando latin-1")
                    return 'latin-1'
                
                logger.info(f"Encoding detectado: {detected} (confiança: {confidence:.2%})")
                return detected
        except Exception as e:
            logger.warning(f"Erro ao detectar encoding: {e}. Usando latin-1")
            return 'latin-1'
    
    def _detect_delimiter(self, file_path: str, encoding: str) -> str:
        """Detecta o delimitador do CSV automaticamente"""
        import csv
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                sample = f.read(8192)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                logger.info(f"Delimitador detectado: '{delimiter}'")
                return delimiter
        except Exception as e:
            logger.warning(f"Erro ao detectar delimitador: {e}. Usando vírgula")
            return ','
    
    def _normalize_address(self, text: str) -> str:
        """Normaliza e padroniza endereços"""
        if not text or pd.isna(text):
            return ''
        
        import re
        
        # Converte para string e remove espaços extras
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        
        # Remove caracteres especiais no início/fim
        text = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', text)
        
        # Padroniza abreviações comuns de logradouro
        abbreviations = {
            r'\bR\b\.?': 'Rua',
            r'\bRUA\b': 'Rua',
            r'\bAV\b\.?': 'Avenida',
            r'\bAVENIDA\b': 'Avenida',
            r'\bAVDA\b\.?': 'Avenida',
            r'\bALM\b\.?': 'Alameda',
            r'\bALAMEDA\b': 'Alameda',
            r'\bTRAV\b\.?': 'Travessa',
            r'\bTRAVESSA\b': 'Travessa',
            r'\bPÇ\b\.?': 'Praça',
            r'\bPC\b\.?': 'Praça',
            r'\bPRACA\b': 'Praça',
            r'\bPRAÇA\b': 'Praça',
            r'\bROD\b\.?': 'Rodovia',
            r'\bRODOVIA\b': 'Rodovia',
            r'\bEST\b\.?': 'Estrada',
            r'\bESTRADA\b': 'Estrada',
            r'\bLARGO\b': 'Largo',
            r'\bLGO\b\.?': 'Largo',
            r'\bVIA\b': 'Via',
            r'\bBECO\b': 'Beco',
            r'\bVILA\b': 'Vila',
            r'\bPARQUE\b': 'Parque',
            r'\bJARDIM\b': 'Jardim',
            r'\bCONJ\b\.?': 'Conjunto',
            r'\bCONJUNTO\b': 'Conjunto',
        }
        
        for pattern, replacement in abbreviations.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        # Capitaliza corretamente (Title Case), mas mantém algumas palavras em minúsculo
        words = text.split()
        lowercase_words = {'de', 'da', 'do', 'das', 'dos', 'e', 'a', 'o'}
        
        formatted_words = []
        for i, word in enumerate(words):
            # Primeira palavra sempre em maiúscula
            if i == 0 or word.lower() not in lowercase_words:
                formatted_words.append(word.capitalize())
            else:
                formatted_words.append(word.lower())
        
        text = ' '.join(formatted_words)
        
        return text.strip()
    
    def process_file(
        self,
        file_path: str,
        output_path: Optional[str] = None,
        progress_callback: Optional[Callable] = None
    ) -> Dict:
        """
        Processa arquivo CSV completo
        
        Args:
            file_path: Caminho do arquivo CSV
            output_path: Caminho para salvar arquivo processado
            progress_callback: Função para reportar progresso
            
        Returns:
            Dicionário com estatísticas
        """
        # Lê arquivo em chunks
        chunks = self._read_csv_chunks(file_path)
        
        results = []
        for i, chunk in enumerate(chunks):
            logger.info(f"Processando chunk {i+1}...")
            
            processed_chunk = self._process_chunk(chunk)
            results.append(processed_chunk)
            
            self.stats['processed_rows'] += len(chunk)
            
            if progress_callback:
                progress = (self.stats['processed_rows'] / self.stats['total_rows']) * 100
                progress_callback(progress)
        
        # Combina resultados
        final_df = pd.concat(results, ignore_index=True)
        
        # Salva arquivo
        if output_path:
            final_df.to_csv(output_path, index=False, encoding='utf-8')
            logger.info(f"Arquivo salvo em: {output_path}")
        
        return {
            'dataframe': final_df,
            'stats': self.stats
        }
    
    def _read_csv_chunks(self, file_path: str) -> Iterator[pd.DataFrame]:
        """Lê arquivo CSV em chunks"""
        # Detecta encoding automaticamente
        if not self.detected_encoding:
            self.detected_encoding = self._detect_encoding(file_path)
        
        # Detecta delimitador automaticamente
        if not self.detected_delimiter:
            self.detected_delimiter = self._detect_delimiter(file_path, self.detected_encoding)
        
        logger.info(f"Lendo arquivo com encoding: {self.detected_encoding}, delimitador: '{self.detected_delimiter}'")
        
        try:
            # Primeiro, obtém o número total de linhas
            with open(file_path, 'r', encoding=self.detected_encoding, errors='replace') as f:
                self.stats['total_rows'] = sum(1 for _ in f) - 1  # -1 para header
            
            # Lê chunks
            for chunk in pd.read_csv(
                file_path,
                chunksize=self.chunk_size,
                encoding=self.detected_encoding,
                encoding_errors='replace',
                on_bad_lines='warn',
                delimiter=self.detected_delimiter,
                quotechar='"',
                skipinitialspace=True
            ):
                yield chunk
        
        except Exception as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            # Tenta com latin-1 como último recurso
            logger.warning("Tentando com latin-1...")
            self.detected_encoding = 'latin-1'
            
            with open(file_path, 'r', encoding='latin-1') as f:
                self.stats['total_rows'] = sum(1 for _ in f) - 1
            
            for chunk in pd.read_csv(
                file_path,
                chunksize=self.chunk_size,
                encoding='latin-1',
                on_bad_lines='warn',
                delimiter=self.detected_delimiter,
                quotechar='"',
                skipinitialspace=True
            ):
                yield chunk
    
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Processa um chunk do CSV"""
        chunk = chunk.copy()
        
        # Aplicar mapeamento de colunas se existir
        if self.col_mapping:
            # Inverter o mapeamento: self.col_mapping tem {nome_esperado: nome_real}
            chunk = chunk.rename(columns={v: k for k, v in self.col_mapping.items() if v in chunk.columns})
        
        # Adiciona coluna para CEP corrigido se não existir
        if 'CD_CEP_CORRETO' not in chunk.columns:
            chunk['CD_CEP_CORRETO'] = None
        if 'NM_LOGRADOURO_CORRETO' not in chunk.columns:
            chunk['NM_LOGRADOURO_CORRETO'] = None
        if 'NM_BAIRRO_CORRETO' not in chunk.columns:
            chunk['NM_BAIRRO_CORRETO'] = None
        if 'NM_MUNICIPIO_CORRETO' not in chunk.columns:
            chunk['NM_MUNICIPIO_CORRETO'] = None
        if 'NM_UF_CORRETO' not in chunk.columns:
            chunk['NM_UF_CORRETO'] = None
        if 'DS_LATITUDE' not in chunk.columns:
            chunk['DS_LATITUDE'] = None
        if 'DS_LONGITUDE' not in chunk.columns:
            chunk['DS_LONGITUDE'] = None
        
        # Processa cada linha
        for idx, row in chunk.iterrows():
            try:
                cep = str(row.get('CD_CEP', '')).strip()
                
                # Criterio 1: Validar CEP existente
                if cep and self.cep_validator.validate_cep_format(cep):
                    cep_data = self.cep_validator.search_cep(cep)
                    
                    if cep_data:
                        # CEP válido - marca como correto
                        chunk.at[idx, 'CD_CEP_CORRETO'] = cep
                        
                        # Preenche dados do endereço correto
                        chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] = cep_data.get('logradouro', '')
                        chunk.at[idx, 'NM_BAIRRO_CORRETO'] = cep_data.get('bairro', '')
                        chunk.at[idx, 'NM_MUNICIPIO_CORRETO'] = cep_data.get('localidade', '')
                        chunk.at[idx, 'NM_UF_CORRETO'] = cep_data.get('uf', '')
                        
                        # Busca coordenadas
                        if pd.isna(row.get('DS_LATITUDE')) or pd.isna(row.get('DS_LONGITUDE')):
                            coords = self._get_coordinates_from_cep(cep_data)
                            if coords:
                                chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                                chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                                self.stats['found_coordinates'] += 1
                    else:
                        # CEP inválido - Criterio 2: Buscar CEP por endereço
                        cep_corrigido = self._search_cep_by_address(row)
                        if cep_corrigido:
                            chunk.at[idx, 'CD_CEP_CORRETO'] = cep_corrigido
                            self.stats['fixed_ceps'] += 1
                            
                            # Busca dados do CEP corrigido
                            cep_data = self.cep_validator.search_cep(cep_corrigido)
                            if cep_data:
                                # Preenche dados do endereço correto
                                chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] = cep_data.get('logradouro', '')
                                chunk.at[idx, 'NM_BAIRRO_CORRETO'] = cep_data.get('bairro', '')
                                chunk.at[idx, 'NM_MUNICIPIO_CORRETO'] = cep_data.get('localidade', '')
                                chunk.at[idx, 'NM_UF_CORRETO'] = cep_data.get('uf', '')
                                
                                # Busca coordenadas pelo CEP corrigido
                                coords = self._get_coordinates_from_cep(cep_data)
                                if coords:
                                    chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                                    chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                                    self.stats['found_coordinates'] += 1
                
                # Se ainda não tem coordenadas, tenta por endereço
                if pd.isna(chunk.at[idx, 'DS_LATITUDE']) or pd.isna(chunk.at[idx, 'DS_LONGITUDE']):
                    coords = self._get_coordinates_by_address(row)
                    if coords:
                        chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                        chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                        self.stats['found_coordinates'] += 1
                
                # Replica dados originais para colunas corretas se estiverem vazias
                if pd.isna(chunk.at[idx, 'NM_LOGRADOURO_CORRETO']) or chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] == '':
                    logradouro_original = row.get('NM_LOGRADOURO', '')
                    if logradouro_original and str(logradouro_original).strip():
                        chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] = str(logradouro_original).strip()
                
                if pd.isna(chunk.at[idx, 'NM_BAIRRO_CORRETO']) or chunk.at[idx, 'NM_BAIRRO_CORRETO'] == '':
                    bairro_original = row.get('NM_BAIRRO', '')
                    if bairro_original and str(bairro_original).strip():
                        chunk.at[idx, 'NM_BAIRRO_CORRETO'] = str(bairro_original).strip()
                
                if pd.isna(chunk.at[idx, 'NM_MUNICIPIO_CORRETO']) or chunk.at[idx, 'NM_MUNICIPIO_CORRETO'] == '':
                    municipio_original = row.get('NM_MUNICIPIO', '')
                    if municipio_original and str(municipio_original).strip():
                        chunk.at[idx, 'NM_MUNICIPIO_CORRETO'] = str(municipio_original).strip()
                
                if pd.isna(chunk.at[idx, 'NM_UF_CORRETO']) or chunk.at[idx, 'NM_UF_CORRETO'] == '':
                    uf_original = row.get('NM_UF', '')
                    if uf_original and str(uf_original).strip():
                        chunk.at[idx, 'NM_UF_CORRETO'] = str(uf_original).strip()
                
                # Padroniza formato dos endereços corretos
                if chunk.at[idx, 'NM_LOGRADOURO_CORRETO']:
                    chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] = self._normalize_address(chunk.at[idx, 'NM_LOGRADOURO_CORRETO'])
                
                if chunk.at[idx, 'NM_BAIRRO_CORRETO']:
                    chunk.at[idx, 'NM_BAIRRO_CORRETO'] = self._normalize_address(chunk.at[idx, 'NM_BAIRRO_CORRETO'])
                
                # Se ainda não tem coordenadas, tenta buscar usando dados corretos
                if pd.isna(chunk.at[idx, 'DS_LATITUDE']) or pd.isna(chunk.at[idx, 'DS_LONGITUDE']):
                    coords = self._get_coordinates_with_fallback(chunk.loc[idx])
                    if coords:
                        chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                        chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                        self.stats['found_coordinates'] += 1
            
            except Exception as e:
                logger.error(f"Erro ao processar linha {idx}: {str(e)}")
                self.stats['errors'].append({
                    'row': idx,
                    'error': str(e)
                })
        
        return chunk
    
    def _search_cep_by_address(self, row: pd.Series) -> Optional[str]:
        """Busca CEP correto usando endereço através de busca na ViaCEP"""
        street = str(row.get('NM_LOGRADOURO', '')).strip()
        city = str(row.get('NM_MUNICIPIO', '')).strip()
        state = str(row.get('NM_UF', '')).strip()
        
        if not street or not city or not state:
            return None
        
        try:
            # Usa API ViaCEP para buscar endereços (formato: UF/cidade/logradouro)
            # Exemplo: https://viacep.com.br/ws/SP/São Paulo/Paulista/json/
            import requests
            
            # Normaliza strings para URL
            from urllib.parse import quote
            state_encoded = quote(state)
            city_encoded = quote(city)
            street_encoded = quote(street)
            
            url = f"https://viacep.com.br/ws/{state_encoded}/{city_encoded}/{street_encoded}/json/"
            
            self.cep_validator._apply_rate_limit()
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # ViaCEP retorna array de resultados
                if isinstance(data, list) and len(data) > 0:
                    # Pega o primeiro resultado
                    cep = data[0].get('cep', '').replace('-', '')
                    if cep:
                        logger.info(f"CEP encontrado para {street}, {city}/{state}: {cep}")
                        return cep
        except Exception as e:
            logger.warning(f"Erro ao buscar CEP por endereço: {str(e)}")
        
        return None
    
    def _get_coordinates_from_cep(self, cep_data: Dict) -> Optional[tuple]:
        """Extrai coordenadas de dados do ViaCEP"""
        try:
            # ViaCEP não retorna coordenadas diretas
            # Precisamos buscar via geocoding do endereço
            street = cep_data.get('logradouro', '')
            neighborhood = cep_data.get('bairro', '')
            city = cep_data.get('localidade', '')
            state = cep_data.get('uf', '')
            
            return self.geocoder.search_by_address(street, "", neighborhood, city, state)
        
        except Exception as e:
            logger.warning(f"Erro ao extrair coordenadas: {str(e)}")
            return None
    
    def _get_coordinates_by_address(self, row: pd.Series) -> Optional[tuple]:
        """Busca coordenadas usando endereço completo"""
        try:
            street = str(row.get('NM_LOGRADOURO', '')).strip()
            neighborhood = str(row.get('NM_BAIRRO', '')).strip()
            city = str(row.get('NM_MUNICIPIO', '')).strip()
            state = str(row.get('NM_UF', '')).strip()
            
            if not street or not city:
                return None
            
            return self.geocoder.search_by_address(street, "", neighborhood, city, state)
        
        except Exception as e:
            logger.warning(f"Erro ao buscar coordenadas por endereço: {str(e)}")
            return None
    
    def _get_coordinates_with_fallback(self, row: pd.Series) -> Optional[tuple]:
        """
        Busca coordenadas usando múltiplas estratégias de fallback
        Prioriza dados corretos e tenta combinações mais específicas primeiro
        """
        try:
            # Pega campos corretos primeiro, depois originais como fallback
            cep_correto = row.get('CD_CEP_CORRETO', '')
            logradouro = str(row.get('NM_LOGRADOURO_CORRETO', '') or row.get('NM_LOGRADOURO', '')).strip()
            bairro = str(row.get('NM_BAIRRO_CORRETO', '') or row.get('NM_BAIRRO', '')).strip()
            municipio = str(row.get('NM_MUNICIPIO_CORRETO', '') or row.get('NM_MUNICIPIO', '')).strip()
            uf = str(row.get('NM_UF_CORRETO', '') or row.get('NM_UF', '')).strip()
            
            # Estratégia 1: CEP correto + cidade
            if cep_correto and municipio:
                logger.info(f"Tentando: CEP {cep_correto} + {municipio}")
                coords = self.geocoder.search_by_cep(cep_correto, municipio, "BR")
                if coords:
                    logger.info(f"✓ Encontrado por CEP + cidade")
                    return coords
            
            # Estratégia 2: Endereço completo (logradouro + bairro + cidade + UF)
            if logradouro and municipio:
                logger.info(f"Tentando: {logradouro}, {bairro}, {municipio}/{uf}")
                coords = self.geocoder.search_by_address(logradouro, "", bairro, municipio, uf)
                if coords:
                    logger.info(f"✓ Encontrado por endereço completo")
                    return coords
            
            # Estratégia 3: Logradouro + cidade + UF (sem bairro)
            if logradouro and municipio and uf:
                logger.info(f"Tentando: {logradouro}, {municipio}/{uf}")
                coords = self.geocoder.search_by_address(logradouro, "", "", municipio, uf)
                if coords:
                    logger.info(f"✓ Encontrado por logradouro + cidade")
                    return coords
            
            # Estratégia 4: Apenas bairro + cidade + UF
            if bairro and municipio and uf:
                logger.info(f"Tentando: {bairro}, {municipio}/{uf}")
                coords = self.geocoder.search_by_address("", "", bairro, municipio, uf)
                if coords:
                    logger.info(f"✓ Encontrado por bairro + cidade")
                    return coords
            
            # Estratégia 5: Apenas cidade + UF (coordenadas do centro da cidade)
            if municipio and uf:
                logger.info(f"Tentando: {municipio}/{uf}")
                coords = self.geocoder.search_by_address("", "", "", municipio, uf)
                if coords:
                    logger.info(f"✓ Encontrado centro da cidade")
                    return coords
            
            logger.warning(f"Nenhuma coordenada encontrada para: {municipio}")
            return None
        
        except Exception as e:
            logger.warning(f"Erro ao buscar coordenadas com fallback: {str(e)}")
            return None
