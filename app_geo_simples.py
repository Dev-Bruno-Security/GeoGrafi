"""
Interface Streamlit para Processamento de Dados Geográficos
Versão simplificada - sem erros de importação
"""

import streamlit as st
import pandas as pd
import os
import tempfile
from pathlib import Path
import time
import requests
import logging
from typing import Optional, Dict, Tuple
import gc  # Garbage collection para arquivos grandes

# Configuração de logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# ===================== CLASSES DE PROCESSAMENTO =====================

class CEPValidator:
    """Valida e busca informações de CEP"""
    
    BASE_URL = "https://viacep.com.br/ws"
    TIMEOUT = 10
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1
    
    def __init__(self, rate_limit_delay: float = 0.15):
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
        """Busca informações de um CEP"""
        if not cep:
            return None
        
        cep_clean = ''.join(filter(str.isdigit, str(cep)))
        
        if len(cep_clean) != 8:
            return None
        
        if cep_clean in self.cache:
            return self.cache[cep_clean]
        
        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                self._apply_rate_limit()
                url = f"{self.BASE_URL}/{cep_clean}/json/"
                response = requests.get(url, timeout=self.TIMEOUT)
                
                if response.status_code == 200:
                    data = response.json()
                    if not data.get('erro'):
                        self.cache[cep_clean] = data
                        return data
                    else:
                        self.cache[cep_clean] = None
                        return None
            except requests.exceptions.RequestException as e:
                if attempt < self.RETRY_ATTEMPTS - 1:
                    time.sleep(self.RETRY_DELAY)
        
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


class Geocoder:
    """Busca coordenadas usando Nominatim"""
    
    BASE_URL = "https://nominatim.openstreetmap.org/search"
    TIMEOUT = 10
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2
    
    def __init__(self, rate_limit_delay: float = 1.5):
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        self.cache = {}
        self.headers = {'User-Agent': 'GeoGrafi/1.0'}
    
    def _apply_rate_limit(self):
        """Aplica rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def search_by_address(self, street: str, neighborhood: str = "", 
                         city: str = "", state: str = "BR") -> Optional[Tuple[float, float]]:
        """Busca coordenadas usando endereço"""
        parts = [street]
        if neighborhood:
            parts.append(neighborhood)
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        
        query = ", ".join(parts)
        cache_key = f"address:{query}"
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        if not query or len(query.strip()) < 3:
            return None
        
        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                self._apply_rate_limit()
                params = {'q': query, 'format': 'json', 'limit': 1}
                response = requests.get(self.BASE_URL, params=params, 
                                       headers=self.headers, timeout=self.TIMEOUT)
                
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        result = (float(data[0]['lat']), float(data[0]['lon']))
                        self.cache[cache_key] = result
                        return result
                    return None
            except requests.exceptions.RequestException:
                if attempt < self.RETRY_ATTEMPTS - 1:
                    time.sleep(self.RETRY_DELAY)
        
        return None


class CSVProcessor:
    """Processa arquivo CSV com enriquecimento de dados"""
    
    def __init__(self, chunk_size: int = 1000, col_mapping: dict = None, sep: str = None):
        self.chunk_size = chunk_size
        self.cep_validator = CEPValidator()
        self.geocoder = Geocoder()
        self.col_mapping = col_mapping or {}
        self.sep = sep  # Delimitador (detectado automaticamente)
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'fixed_ceps': 0,
            'found_coordinates': 0,
            'errors': []
        }
    
    def _detect_encoding(self, file_path: str) -> str:
        """Detecta encoding do arquivo"""
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1000)
                return encoding
            except (UnicodeDecodeError, Exception):
                continue
        
        return 'latin-1'  # Fallback padrão
    
    def _detect_separator(self, file_path: str, encoding: str) -> str:
        """Detecta o separador/delimitador do arquivo"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline()
            
            # Conta ocorrências de possíveis separadores
            separators = {',': first_line.count(','),
                         ';': first_line.count(';'),
                         '\t': first_line.count('\t'),
                         '|': first_line.count('|')}
            
            # Retorna o separador com mais ocorrências
            if max(separators.values()) > 0:
                return max(separators, key=separators.get)
            return ','  # Fallback padrão
        except Exception:
            return ','
    
    def process_file(self, file_path: str, progress_callback=None) -> Dict:
        """Processa arquivo CSV (otimizado para arquivos grandes)"""
        chunks = []
        
        # Detecta encoding
        encoding = self._detect_encoding(file_path)
        
        # Detecta separador se não fornecido
        if not self.sep:
            self.sep = self._detect_separator(file_path, encoding)
        
        try:
            # Conta linhas
            with open(file_path, 'r', encoding=encoding) as f:
                total = sum(1 for _ in f) - 1
                self.stats['total_rows'] = total
        except Exception as e:
            return {'dataframe': pd.DataFrame(), 'stats': self.stats}
        
        # Processa chunks
        try:
            reader = pd.read_csv(file_path, chunksize=self.chunk_size, encoding=encoding, sep=self.sep)
        except Exception as e:
            return {'dataframe': pd.DataFrame(), 'stats': self.stats}
        
        for chunk in reader:
            # Renomeia colunas se necessário
            if self.col_mapping:
                for new_col, old_col in self.col_mapping.items():
                    if old_col in chunk.columns and new_col not in chunk.columns:
                        chunk = chunk.rename(columns={old_col: new_col})
            
            processed = self._process_chunk(chunk)
            chunks.append(processed)
            
            self.stats['processed_rows'] += len(chunk)
            if progress_callback:
                progress = (self.stats['processed_rows'] / self.stats['total_rows']) * 100
                progress_callback(progress)
            
            # Limpeza de memória para arquivos grandes
            del chunk
            gc.collect()
        
        final_df = pd.concat(chunks, ignore_index=True)
        
        return {
            'dataframe': final_df,
            'stats': self.stats
        }
    
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Processa um chunk"""
        chunk = chunk.copy()
        
        # Adiciona colunas para dados corrigidos
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
        
        for idx, row in chunk.iterrows():
            try:
                cep = str(row.get('CD_CEP', '')).strip()
                found_coords = False
                cep_valido = False
                
                # Critério 1: Validar CEP existente
                if cep and self.cep_validator.validate_cep_format(cep):
                    cep_data = self.cep_validator.search_cep(cep)
                    
                    if cep_data:
                        # CEP válido encontrado - replica dados originais para colunas corretas
                        chunk.at[idx, 'CD_CEP_CORRETO'] = cep
                        chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] = str(row.get('NM_LOGRADOURO', '')).strip()
                        chunk.at[idx, 'NM_BAIRRO_CORRETO'] = str(row.get('NM_BAIRRO', '')).strip()
                        chunk.at[idx, 'NM_MUNICIPIO_CORRETO'] = str(row.get('NM_MUNICIPIO', '')).strip()
                        chunk.at[idx, 'NM_UF_CORRETO'] = str(row.get('NM_UF', '')).strip()
                        cep_valido = True
                        
                        # Busca coordenadas pelo CEP
                        if pd.isna(row.get('DS_LATITUDE')) or pd.isna(row.get('DS_LONGITUDE')):
                            coords = self._get_coordinates_from_cep(cep_data)
                            if coords:
                                chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                                chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                                self.stats['found_coordinates'] += 1
                                found_coords = True
                
                # Critério 2: CEP inválido ou não encontrado - busca CEP pelo endereço
                if not cep_valido:
                    cep_correto = self._find_cep_by_address(row)
                    if cep_correto:
                        chunk.at[idx, 'CD_CEP_CORRETO'] = cep_correto
                        self.stats['fixed_ceps'] += 1
                        
                        # Busca dados completos do endereço usando o CEP corrigido
                        cep_data_correto = self.cep_validator.search_cep(cep_correto)
                        if cep_data_correto:
                            # Salva endereço correto
                            chunk.at[idx, 'NM_LOGRADOURO_CORRETO'] = cep_data_correto.get('logradouro', '')
                            chunk.at[idx, 'NM_BAIRRO_CORRETO'] = cep_data_correto.get('bairro', '')
                            chunk.at[idx, 'NM_MUNICIPIO_CORRETO'] = cep_data_correto.get('localidade', '')
                            chunk.at[idx, 'NM_UF_CORRETO'] = cep_data_correto.get('uf', '')
                            
                            # Busca coordenadas usando o CEP corrigido
                            if not found_coords:
                                coords = self._get_coordinates_from_cep(cep_data_correto)
                                if coords:
                                    chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                                    chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                                    self.stats['found_coordinates'] += 1
                                    found_coords = True
                
                # Critério 3: Se ainda não encontrou coordenadas, busca por endereço
                if not found_coords and (pd.isna(chunk.at[idx, 'DS_LATITUDE']) or pd.isna(chunk.at[idx, 'DS_LONGITUDE'])):
                    # Tenta buscar por endereço completo primeiro
                    coords = self._get_coordinates_by_full_address(row)
                    
                    if coords:
                        chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                        chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                        self.stats['found_coordinates'] += 1
                        found_coords = True
                    else:
                        # Tenta buscar apenas por município (fallback)
                        coords = self._get_coordinates_by_city(row)
                        if coords:
                            chunk.at[idx, 'DS_LATITUDE'] = coords[0]
                            chunk.at[idx, 'DS_LONGITUDE'] = coords[1]
                            self.stats['found_coordinates'] += 1
                            found_coords = True
            
            except Exception as e:
                self.stats['errors'].append({'row': idx, 'error': str(e)})
        
        return chunk
    
    def _get_coordinates_from_cep(self, cep_data: Dict) -> Optional[tuple]:
        """Extrai coordenadas do CEP"""
        try:
            street = cep_data.get('logradouro', '')
            neighborhood = cep_data.get('bairro', '')
            city = cep_data.get('localidade', '')
            state = cep_data.get('uf', '')
            
            return self.geocoder.search_by_address(street, neighborhood, city, state)
        except:
            return None
    
    def _find_cep_by_address(self, row: pd.Series) -> Optional[str]:
        """
        Busca CEP usando endereço (logradouro, bairro, município)
        Usa busca no ViaCEP por componentes do endereço
        """
        try:
            state = str(row.get('NM_UF', '')).strip().upper()
            city = str(row.get('NM_MUNICIPIO', '')).strip()
            street = str(row.get('NM_LOGRADOURO', '')).strip()
            
            if not state or not city:
                return None
            
            # Rate limiting
            time.sleep(0.2)
            
            # Busca no ViaCEP usando formato: UF/Cidade/Logradouro
            # Exemplo: PE/Caruaru/Rio Formoso
            if street:
                # Limpa o nome da rua (remove números, etc)
                street_clean = ' '.join([word for word in street.split() if not word.isdigit()])
                
                if len(street_clean) >= 3:
                    try:
                        url = f"https://viacep.com.br/ws/{state}/{city}/{street_clean}/json/"
                        response = requests.get(url, timeout=10)
                        
                        if response.status_code == 200:
                            results = response.json()
                            
                            if isinstance(results, list) and len(results) > 0:
                                # Retorna o primeiro CEP encontrado
                                cep_found = results[0].get('cep', '').replace('-', '')
                                if cep_found:
                                    return cep_found
                    except:
                        pass
            
            # Se não encontrou por logradouro, tenta buscar apenas por cidade
            # Isso retorna um CEP genérico da cidade
            try:
                time.sleep(0.2)
                url = f"https://viacep.com.br/ws/{state}/{city}/centro/json/"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    results = response.json()
                    
                    if isinstance(results, list) and len(results) > 0:
                        cep_found = results[0].get('cep', '').replace('-', '')
                        if cep_found:
                            return cep_found
            except:
                pass
            
            return None
        
        except Exception as e:
            return None
    
    def _get_coordinates_by_address(self, row: pd.Series) -> Optional[tuple]:
        """Busca coordenadas por endereço"""
        try:
            street = str(row.get('NM_LOGRADOURO', '')).strip()
            neighborhood = str(row.get('NM_BAIRRO', '')).strip()
            city = str(row.get('NM_MUNICIPIO', '')).strip()
            state = str(row.get('NM_UF', '')).strip()
            
            if not street or not city:
                return None
            
            return self.geocoder.search_by_address(street, neighborhood, city, state)
        except:
            return None
    
    def _get_coordinates_by_full_address(self, row: pd.Series) -> Optional[tuple]:
        """Busca coordenadas usando endereço completo (logradouro + bairro + município)"""
        try:
            street = str(row.get('NM_LOGRADOURO', '')).strip()
            neighborhood = str(row.get('NM_BAIRRO', '')).strip()
            city = str(row.get('NM_MUNICIPIO', '')).strip()
            state = str(row.get('NM_UF', '')).strip()
            
            # Tenta com endereço completo primeiro
            if street and city:
                coords = self.geocoder.search_by_address(street, neighborhood, city, state)
                if coords:
                    return coords
            
            # Tenta apenas com logradouro e município
            if street and city:
                coords = self.geocoder.search_by_address(street, "", city, state)
                if coords:
                    return coords
            
            # Tenta apenas com bairro e município
            if neighborhood and city:
                coords = self.geocoder.search_by_address(neighborhood, "", city, state)
                if coords:
                    return coords
            
            return None
        
        except Exception as e:
            return None
    
    def _get_coordinates_by_city(self, row: pd.Series) -> Optional[tuple]:
        """Busca coordenadas usando apenas município como fallback"""
        try:
            city = str(row.get('NM_MUNICIPIO', '')).strip()
            state = str(row.get('NM_UF', '')).strip()
            
            if not city:
                return None
            
            # Busca apenas pelo município
            return self.geocoder.search_by_address(city, "", "", state)
        
        except Exception as e:
            return None


# ===================== INTERFACE STREAMLIT =====================

st.set_page_config(
    page_title="GeoGrafi",
    page_icon="📍",
    layout="wide"
)

st.markdown("# 📍 GeoGrafi")
st.markdown("### Enriqueça seus dados CSV com CEPs e coordenadas")

# Sidebar
with st.sidebar:
    st.markdown("## ⚙️ Configurações")
    st.markdown("### Para arquivos grandes (>500MB)")
    chunk_size = st.slider("Tamanho do chunk", 500, 10000, 5000, 500, help="Maior = mais memória, mais rápido")
    use_cache = st.checkbox("Usar cache", True)

# Abas
tab1, tab2, tab3 = st.tabs(["📤 Processar", "📋 Informações", "❓ Ajuda"])

with tab1:
    st.markdown("## Envie seu arquivo CSV")
    
    uploaded_file = st.file_uploader("Selecione um CSV", type=['csv'])
    
    if uploaded_file is not None:
        st.markdown("### 👁️ Preview")
        
        try:
            # Tenta múltiplas codificações e separadores
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            separators = [',', ';', '\t', '|']
            df_preview = None
            detected_sep = ','
            
            for encoding in encodings:
                try:
                    # Lê primeira linha para detectar separador
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name
                    
                    with open(tmp_path, 'r', encoding=encoding) as f:
                        first_line = f.readline()
                    
                    # Conta ocorrências de separadores
                    sep_counts = {sep: first_line.count(sep) for sep in separators}
                    detected_sep = max(sep_counts, key=sep_counts.get)
                    
                    if sep_counts[detected_sep] == 0:
                        detected_sep = ','
                    
                    # Tenta ler com separador detectado
                    df_preview = pd.read_csv(tmp_path, nrows=5, encoding=encoding, sep=detected_sep)
                    os.remove(tmp_path)
                    break
                except (UnicodeDecodeError, Exception):
                    try:
                        os.remove(tmp_path)
                    except:
                        pass
                    uploaded_file.seek(0)
                    continue
            
            if df_preview is None:
                st.error("❌ Não foi possível ler o arquivo")
                st.stop()
            
            st.markdown(f"**Separador detectado:** `{repr(detected_sep)}`")
            st.dataframe(df_preview, use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Tamanho", f"{uploaded_file.size / (1024*1024):.2f} MB")
            with col2:
                st.metric("Colunas", len(df_preview.columns))
            with col3:
                st.metric("Linhas (preview)", len(df_preview))
            
            # Verifica colunas
            required_cols = ['CD_CEP', 'NM_LOGRADOURO', 'NM_BAIRRO', 'NM_MUNICIPIO', 'NM_UF']
            alt_cols = {
                'CD_CEP': ['NR_CEP', 'CEP', 'CD_CEP'],
                'NM_LOGRADOURO': ['DS_ENDERECO', 'ENDERECO', 'LOGRADOURO', 'NM_LOGRADOURO'],
                'NM_BAIRRO': ['DS_BAIRRO', 'BAIRRO', 'NM_BAIRRO'],
                'NM_MUNICIPIO': ['NM_CIDADE', 'CIDADE', 'MUNICIPIO', 'NM_MUNICIPIO', 'DS_MUNICIPIO'],
                'NM_UF': ['UF', 'ESTADO', 'NM_UF', 'DS_UF']
            }
            
            # Mapeia colunas disponíveis
            col_mapping = {}
            for required, alternatives in alt_cols.items():
                found = None
                for alt in alternatives:
                    if alt in df_preview.columns:
                        found = alt
                        break
                
                if found:
                    col_mapping[required] = found
            
            missing_cols = [col for col in required_cols if col not in col_mapping]
            
            if missing_cols:
                st.error(f"⚠️ Colunas faltando: {', '.join(missing_cols)}")
                st.info(f"📌 Colunas encontradas: {', '.join(df_preview.columns.tolist())}")
            else:
                st.success(f"✅ Colunas OK!")
                if col_mapping:
                    st.info(f"📋 Mapeamento automático:\n{col_mapping}")
                
                if st.button("🚀 Processar", type="primary", use_container_width=True):
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name
                    
                    try:
                        processor = CSVProcessor(chunk_size=chunk_size, col_mapping=col_mapping, sep=detected_sep)
                        
                        progress_bar = st.progress(0)
                        status = st.empty()
                        
                        def update(current):
                            progress_bar.progress(min(current / 100, 0.99))
                            status.text(f"Progresso: {current:.1f}%")
                        
                        status.text("⏳ Iniciando...")
                        start = time.time()
                        
                        result = processor.process_file(tmp_path, update)
                        elapsed = time.time() - start
                        
                        progress_bar.progress(1.0)
                        df_result = result['dataframe']
                        stats = result['stats']
                        
                        st.success("✅ Processamento Concluído!")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total", stats['processed_rows'])
                        with col2:
                            st.metric("CEPs corrigidos", stats['fixed_ceps'])
                        with col3:
                            st.metric("Coordenadas", stats['found_coordinates'])
                        with col4:
                            st.metric("Tempo (min)", f"{elapsed/60:.2f}")
                        
                        st.dataframe(df_result.head(10), use_container_width=True)
                        
                        csv_result = df_result.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            "📥 Baixar CSV",
                            csv_result,
                            f"dados_{int(time.time())}.csv",
                            "text/csv",
                            use_container_width=True
                        )
                    
                    finally:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
        
        except Exception as e:
            st.error(f"❌ Erro: {str(e)}")

with tab2:
    st.markdown("""
    ## 📋 Como funciona
    
    1. **Validação de CEP** - Verifica se o CEP é válido
    2. **Busca de Coordenadas** - Obtém latitude e longitude
    3. **Exportação** - Baixa arquivo processado
    
    ## Colunas Esperadas
    - CD_CEP, NM_LOGRADOURO, NM_BAIRRO, NM_MUNICIPIO, NM_UF
    """)

with tab3:
    st.markdown("""
    ## ❓ FAQ
    
    **Quanto tempo leva para processar milhões de registros?**
    
    Depende do volume e da API, normalmente 5-10 horas com cache.
    """)
