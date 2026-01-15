"""
Interface Streamlit para Processamento de Dados Geográficos
Corrige CEPs e adiciona coordenadas em arquivos CSV grandes
"""

import streamlit as st
import pandas as pd
import os
import tempfile
from pathlib import Path
import time
from modules.csv_processor import CSVProcessor
from modules.cache_manager import CacheManager

# Configuração da página
st.set_page_config(
    page_title="GeoGrafi - Processador de CEP e Coordenadas",
    page_icon="📍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #1e2028;
        color: #e8ecf4;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #2f3340;
    }
    .stMetric label {
        color: #e8ecf4 !important;
    }
    .stMetric p {
        color: #8ab4ff !important;
        font-size: 24px !important;
        font-weight: bold !important;
    }
    .header-title {
        color: #8ab4ff;
        text-align: center;
    }
    .success-box {
        background-color: #123226;
        color: #d3f2e4;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #2ecc71;
        border-right: 1px solid #1f7a50;
    }
    .error-box {
        background-color: #2f1b22;
        color: #f6c1c8;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #e74c3c;
        border-right: 1px solid #80333d;
    }
    .info-box {
        background-color: #132736;
        color: #c7e9ff;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #17a2b8;
        border-right: 1px solid #0d4f63;
    }
</style>
""", unsafe_allow_html=True)

# Título
st.markdown("# 📍 GeoGrafi - Processador Geográfico", unsafe_allow_html=True)
st.markdown("### Enriqueça seus dados CSV com CEPs e coordenadas", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("## ⚙️ Configurações")
    
    chunk_size = st.slider(
        "Tamanho do chunk (linhas)",
        min_value=100,
        max_value=5000,
        value=1000,
        step=100,
        help="Número de linhas processadas por vez (impacta memória)"
    )
    
    max_workers = st.slider(
        "Número de workers",
        min_value=1,
        max_value=10,
        value=3,
        help="Threads paralelas para processamento"
    )
    
    use_cache = st.checkbox(
        "Usar cache local",
        value=True,
        help="Cacheia resultados de CEP e geocoding para acelerar"
    )
    
    st.markdown("---")
    
    # Cache stats
    cache_manager = CacheManager()
    stats = cache_manager.get_stats()
    
    st.markdown("### 📊 Estatísticas do Cache")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CEPs em cache", stats['cep_cache_entries'])
    with col2:
        st.metric("Coords em cache", stats['geocode_cache_entries'])
    with col3:
        st.metric("Processamentos", stats['completed_jobs'])
    
    if st.button("🗑️ Limpar cache antigo", help="Remove entradas com mais de 30 dias"):
        cache_manager.clear_old_cache(days=30)
        st.success("Cache limpado!")

# Abas
tab1, tab2, tab3 = st.tabs(["📤 Processar", "📋 Informações", "❓ Ajuda"])

with tab1:
    st.markdown("## Envie seu arquivo CSV")
    
    # Upload
    uploaded_file = st.file_uploader(
        "Selecione um arquivo CSV",
        type=['csv'],
        help="Arquivo deve ter as colunas: CD_CEP, NM_LOGRADOURO, NM_BAIRRO, NM_MUNICIPIO, NM_UF"
    )
    
    if uploaded_file is not None:
        # Lê preview
        st.markdown("### 👁️ Preview do arquivo")
        
        try:
            # Detecta encoding do arquivo
            import chardet
            uploaded_file.seek(0)
            raw_data = uploaded_file.read(100000)
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0.0)
            
            # Se confiança baixa, usa latin-1
            if not encoding or confidence < 0.5:
                encoding = 'latin-1'
            
            # Detecta delimitador
            import csv
            uploaded_file.seek(0)
            sample = uploaded_file.read(8192).decode(encoding, errors='replace')
            try:
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
            except:
                delimiter = ','
            
            st.info(f"📝 Encoding: {encoding} (conf: {confidence:.2%}) | Delimitador: '{delimiter}'")
            
            # Reseta ponteiro do arquivo
            uploaded_file.seek(0)
            
            # Lê preview com encoding e delimitador corretos
            df_preview = pd.read_csv(
                uploaded_file, 
                nrows=5, 
                encoding=encoding,
                encoding_errors='replace',
                on_bad_lines='warn',
                delimiter=delimiter,
                quotechar='"',
                skipinitialspace=True
            )
            st.dataframe(df_preview, width="stretch")
            
            # Informações do arquivo
            file_size_mb = uploaded_file.size / (1024 * 1024)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Tamanho", f"{file_size_mb:.2f} MB")
            with col2:
                st.metric("Colunas", len(df_preview.columns))
            with col3:
                st.metric("Linhas (preview)", len(df_preview))
            
            # Verifica colunas obrigatórias com mapeamento automático
            required_cols = ['CD_CEP', 'NM_LOGRADOURO', 'NM_BAIRRO', 'NM_MUNICIPIO', 'NM_UF']
            alt_cols = {
                'CD_CEP': ['NR_CEP', 'CEP', 'CD_CEP'],
                'NM_LOGRADOURO': ['DS_ENDERECO', 'ENDERECO', 'LOGRADOURO', 'NM_LOGRADOURO'],
                'NM_BAIRRO': ['DS_BAIRRO', 'BAIRRO', 'NM_BAIRRO'],
                'NM_MUNICIPIO': ['NM_CIDADE', 'CIDADE', 'MUNICIPIO', 'NM_MUNICIPIO', 'DS_MUNICIPIO'],
                'NM_UF': ['UF', 'ESTADO', 'NM_UF', 'DS_UF']
            }
            
            col_mapping = {}
            for required in required_cols:
                if required in df_preview.columns:
                    col_mapping[required] = required
                    continue
                for alt in alt_cols.get(required, []):
                    if alt in df_preview.columns:
                        col_mapping[required] = alt
                        break
            
            missing_cols = [col for col in required_cols if col not in col_mapping]
            
            if missing_cols:
                st.markdown(f"""
                <div class="error-box">
                    <strong>⚠️ Colunas faltando:</strong> {', '.join(missing_cols)}
                </div>
                """, unsafe_allow_html=True)
            else:
                used_alternatives = {req: src for req, src in col_mapping.items() if req != src}
                if used_alternatives:
                    mapping_text = '<br>'.join([f"{req} ⟵ {src}" for req, src in used_alternatives.items()])
                    st.markdown(f"""
                    <div class="info-box">
                        <strong>ℹ️ Mapeamento automático aplicado:</strong><br>{mapping_text}
                    </div>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                <div class="success-box">
                    <strong>✅ Todas as colunas obrigatórias encontradas!</strong>
                </div>
                """, unsafe_allow_html=True)
                
                # Botão de processamento
                if st.button("🚀 Iniciar Processamento", type="primary", width="stretch"):
                    
                    # Cria arquivo temporário
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name
                    
                    try:
                        # Inicializa processador
                        processor = CSVProcessor(
                            chunk_size=chunk_size,
                            max_workers=max_workers,
                            use_cache=use_cache,
                            col_mapping=col_mapping
                        )
                        
                        # Barra de progresso
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        stats_container = st.empty()
                        
                        def update_progress(current):
                            progress_bar.progress(min(current / 100, 0.99))
                            status_text.text(f"Progresso: {current:.1f}%")
                        
                        # Processa arquivo
                        status_text.text("⏳ Iniciando processamento...")
                        start_time = time.time()
                        
                        result = processor.process_file(
                            tmp_path,
                            progress_callback=update_progress
                        )
                        
                        elapsed_time = time.time() - start_time
                        progress_bar.progress(1.0)
                        
                        # Estatísticas finais
                        df_result = result['dataframe']
                        stats = result['stats']
                        
                        st.markdown("### ✅ Processamento Concluído!")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total processado", stats['processed_rows'])
                        with col2:
                            st.metric("CEPs corrigidos", stats['fixed_ceps'])
                        with col3:
                            st.metric("Coordenadas encontradas", stats['found_coordinates'])
                        with col4:
                            st.metric("Tempo (min)", f"{elapsed_time / 60:.2f}")
                        
                        if stats['errors']:
                            st.markdown(f"""
                            <div class="error-box">
                                <strong>⚠️ Erros durante processamento:</strong> {len(stats['errors'])}
                            </div>
                            """, unsafe_allow_html=True)
                        
                        # Preview dos resultados
                        st.markdown("### 📊 Preview dos Resultados")
                        st.dataframe(df_result.head(10), width="stretch")
                        
                        # Download
                        csv_result = df_result.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Baixar CSV processado",
                            data=csv_result,
                            file_name=f"dados_processados_{int(time.time())}.csv",
                            mime="text/csv",
                            width="stretch"
                        )
                        
                    finally:
                        # Remove arquivo temporário
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
        
        except Exception as e:
            st.error(f"❌ Erro ao processar arquivo: {str(e)}")

with tab2:
    st.markdown("## 📋 Informações sobre o Processamento")
    
    st.markdown("""
    ### 🔄 Fluxo de Processamento
    
    1. **Validação de CEP** (Critério 1)
       - Verifica se o CEP é válido usando ViaCEP
       - Se válido, usa o endereço fornecido pela API
       - Se inválido, passa para próximo critério
    
    2. **Busca de CEP por Endereço** (Critério 2)
       - Usa Nominatim (OpenStreetMap) para buscar o CEP
       - Localiza o endereço exato baseado em rua, bairro e cidade
       - Salva o CEP corrigido em coluna nova
    
    3. **Busca de Coordenadas**
       - Usa o CEP validado/corrigido para buscar latitude/longitude
       - Se não encontrar, tenta buscar direto pelo endereço
       - Salva coordenadas nas colunas DS_LATITUDE e DS_LONGITUDE
    
    ### 📊 Colunas Esperadas
    
    | Coluna | Descrição | Tipo |
    |--------|-----------|------|
    | CD_MUNICIPIO | Código do município | Texto |
    | CD_CEP | CEP original | Texto |
    | NM_MUNICIPIO | Nome do município | Texto |
    | NM_LOGRADOURO | Nome da rua/logradouro | Texto |
    | NM_BAIRRO | Nome do bairro | Texto |
    | NM_UF | Estado | Texto |
    | DS_LONGITUDE | Longitude (preenchida) | Número |
    | DS_LATITUDE | Latitude (preenchida) | Número |
    
    ### 📈 Colunas Criadas
    
    | Coluna | Descrição |
    |--------|-----------|
    | CD_CEP_CORRETO | CEP corrigido/validado |
    | NM_LOGRADOURO_CORRETO | Logradouro correto do CEP validado |
    | NM_BAIRRO_CORRETO | Bairro correto do CEP validado |
    | NM_MUNICIPIO_CORRETO | Município correto do CEP validado |
    | NM_UF_CORRETO | UF correta do CEP validado |
    | DS_LATITUDE | Latitude do endereço |
    | DS_LONGITUDE | Longitude do endereço |
    
    ### ⚡ Otimizações
    
    - **Cache Local**: Resultados são armazenados em banco SQLite local
    - **Rate Limiting**: Respeita limites de API para não ser bloqueado
    - **Processamento em Chunks**: Processa arquivo em lotes para economia de memória
    - **Deduplicação**: CEPs e endereços repetidos são consultados apenas uma vez
    
    ### 🌐 APIs Utilizadas
    
    - **ViaCEP** (https://viacep.com.br) - Para validação de CEP
    - **Nominatim** (https://nominatim.org) - Para geocoding de endereços
    
    """)

with tab3:
    st.markdown("## ❓ Perguntas Frequentes")
    
    with st.expander("Como funciona a validação de CEP?"):
        st.markdown("""
        1. O sistema faz requisição para ViaCEP com o CEP informado
        2. Se a API retorna dados válidos, o CEP é considerado correto
        3. Se retorna erro, o CEP é marcado como inválido
        4. No caso de inválido, o sistema tenta descobrir o CEP correto usando o endereço
        """)
    
    with st.expander("Posso processar milhões de registros?"):
        st.markdown("""
        **Sim!** A aplicação foi desenvolvida especificamente para isso:
        
        - Processamento em chunks (não carrega tudo na memória)
        - Cache local para evitar requisições repetidas
        - Rate limiting para não sobrecarregar as APIs
        - Processamento paralelo com workers
        
        Estimativa: 1 milhão de registros = ~5-10 horas (com cache)
        """)
    
    with st.expander("O que é a coluna CD_CEP_CORRETO?"):
        st.markdown("""
        Esta coluna é preenchida apenas quando o CEP original é inválido 
        e o sistema consegue descobrir o CEP correto usando o endereço.
        
        Se o CEP original é válido, esta coluna permanece vazia.
        """)
    
    with st.expander("Por que alguns endereços não retornam coordenadas?"):
        st.markdown("""
        Possíveis razões:
        
        1. Endereço incompleto ou com erros de digitação
        2. Bairro ou logradouro não encontrado na base de dados
        3. Localidade com nome alternativo (apelido vs nome oficial)
        4. Limite de requisições da API atingido
        
        Nestes casos, a coluna fica vazia e pode ser analisada posteriormente.
        """)
    
    with st.expander("O que é o cache e como limpar?"):
        st.markdown("""
        O cache armazena resultados de CEPs e endereços já consultados,
        evitando requisições repetidas desnecessárias.
        
        **Benefícios:**
        - Processamento muito mais rápido
        - Menos carga nas APIs
        - Economia de banda
        
        Você pode limpar o cache na barra lateral (dados com mais de 30 dias).
        """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.9em;">
    <p>GeoGrafi v1.0 | Processador de Dados Geográficos</p>
    <p>Desenvolvido por B.J</p>
</div>
""", unsafe_allow_html=True)
