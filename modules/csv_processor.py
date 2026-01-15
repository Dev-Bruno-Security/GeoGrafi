"""
Processador de CSV em chunks para dados de alta volume
"""
import pandas as pd
from typing import Iterator, Callable, Optional, Dict, List
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import io

from .cep_validator import CEPValidator
from .geocoder import Geocoder
from .cache_manager import CacheManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVProcessor:
    """Processa arquivos CSV em chunks com enriquecimento de dados geográficos"""
    
    def __init__(
        self,
        chunk_size: int = 1000,
        max_workers: int = 3,
        use_cache: bool = True,
        cache_db: str = "cache.db"
    ):
        """
        Inicializa o processador
        
        Args:
            chunk_size: Número de linhas por chunk
            max_workers: Número de workers para processamento paralelo
            use_cache: Usar cache local
            cache_db: Caminho do banco de cache
        """
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.cep_validator = CEPValidator(rate_limit_delay=0.15)
        self.geocoder = Geocoder(rate_limit_delay=1.5)
        self.cache_manager = CacheManager(cache_db) if use_cache else None
        
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'fixed_ceps': 0,
            'found_coordinates': 0,
            'errors': []
        }
    
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
        try:
            # Primeiro, obtém o número total de linhas
            with open(file_path, 'r', encoding='utf-8') as f:
                self.stats['total_rows'] = sum(1 for _ in f) - 1  # -1 para header
            
            # Lê chunks
            for chunk in pd.read_csv(
                file_path,
                chunksize=self.chunk_size,
                encoding='utf-8'
            ):
                yield chunk
        
        except UnicodeDecodeError:
            # Tenta com encoding diferente
            logger.warning("Erro de encoding, tentando latin1...")
            with open(file_path, 'r', encoding='latin1') as f:
                self.stats['total_rows'] = sum(1 for _ in f) - 1
            
            for chunk in pd.read_csv(
                file_path,
                chunksize=self.chunk_size,
                encoding='latin1'
            ):
                yield chunk
    
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Processa um chunk do CSV"""
        chunk = chunk.copy()
        
        # Aplicar mapeamento de colunas se existir
        if self.col_mapping:
            for new_col, old_col in self.col_mapping.items():
                if old_col in chunk.columns and new_col not in chunk.columns:
                    chunk = chunk.rename(columns={old_col: new_col})
        
        # Adiciona coluna para CEP corrigido se não existir
        if 'CD_CEP_CORRETO' not in chunk.columns:
            chunk['CD_CEP_CORRETO'] = None
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
                        # CEP válido
                        chunk.at[idx, 'CD_CEP_CORRETO'] = cep
                        
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
                            
                            # Busca coordenadas pelo CEP corrigido
                            cep_data = self.cep_validator.search_cep(cep_corrigido)
                            if cep_data:
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
            
            except Exception as e:
                logger.error(f"Erro ao processar linha {idx}: {str(e)}")
                self.stats['errors'].append({
                    'row': idx,
                    'error': str(e)
                })
        
        return chunk
    
    def _search_cep_by_address(self, row: pd.Series) -> Optional[str]:
        """Busca CEP correto usando endereço"""
        street = str(row.get('NM_LOGRADOURO', '')).strip()
        neighborhood = str(row.get('NM_BAIRRO', '')).strip()
        city = str(row.get('NM_MUNICIPIO', '')).strip()
        state = str(row.get('NM_UF', '')).strip()
        
        if not street or not city:
            return None
        
        # Monta query para buscar
        query = f"{street}, {city}, {state}, Brazil"
        
        try:
            coords = self.geocoder.search_by_address(street, "", neighborhood, city, state)
            if coords:
                # Se encontrou coordenadas, tenta localizar o CEP mais próximo
                # Por enquanto, retorna None (pode ser melhorado com busca reversa)
                logger.info(f"Encontradas coordenadas para {street}: {coords}")
                return None
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
