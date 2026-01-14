"""
Interface Visual para Leitura de Arquivos CSV Grandes
Usa Streamlit para criar uma interface web interativa
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from csv_reader import CSVReader, CSVAnalyzer
from pathlib import Path
import os


# Configuração da página
st.set_page_config(
    page_title="Leitor de CSV - IBGE",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #f0f2f6 0%, #ffffff 100%);
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .stat-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #28a745;
    }
</style>
""", unsafe_allow_html=True)


def inicializar_sessao():
    """Inicializa variáveis de sessão"""
    if 'reader' not in st.session_state:
        st.session_state.reader = None
    if 'df_sample' not in st.session_state:
        st.session_state.df_sample = None
    if 'file_info' not in st.session_state:
        st.session_state.file_info = None
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 0


def ensure_sample_loaded(sample_size: int = 1000) -> bool:
    """Garante que uma amostra esteja carregada em sessão."""
    try:
        if st.session_state.reader is None:
            return False
        if st.session_state.df_sample is None or not isinstance(st.session_state.df_sample, pd.DataFrame) or st.session_state.df_sample.empty:
            with st.spinner(f'Carregando amostra de {sample_size} linhas...'):
                st.session_state.df_sample = st.session_state.reader.read_sample(sample_size)
        return isinstance(st.session_state.df_sample, pd.DataFrame)
    except Exception as e:
        st.warning(f"Não foi possível carregar amostra automaticamente: {e}")
        return False


def carregar_arquivo(file_path):
    """Carrega o arquivo CSV"""
    try:
        with st.spinner('Carregando arquivo...'):
            reader = CSVReader(file_path)
            st.session_state.reader = reader
            st.session_state.file_info = reader.get_file_info()
            st.session_state.df_sample = reader.read_sample(1000)
        return True
    except Exception as e:
        st.error(f"Erro ao carregar arquivo: {e}")
        return False


def pagina_inicial():
    """Página inicial com seleção de arquivo"""
    st.markdown('<div class="main-header">📊 Leitor de Arquivos CSV Grandes</div>', unsafe_allow_html=True)
    
    st.markdown("""
    ### Bem-vindo ao Leitor de CSV
    
    Esta aplicação permite visualizar e analisar arquivos CSV muito grandes (até 1,5GB+) de forma eficiente.
    
    **Recursos disponíveis:**
    - 📁 Visualização de dados em tabela paginada
    - 📊 Estatísticas e análises
    - 🔍 Filtros e buscas
    - 📈 Gráficos interativos
    - 💾 Exportação de dados filtrados
    """)
    
    # Seleção de arquivo
    st.markdown("---")
    st.subheader("Selecionar Arquivo CSV")
    
    # Diretório padrão
    default_dir = r"C:\Users\bruno.joao\Desktop\Programação\Base_IBGE\CSV"
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Lista arquivos CSV no diretório
        if os.path.exists(default_dir):
            csv_files = [f for f in os.listdir(default_dir) if f.endswith('.csv')]
            if csv_files:
                selected_file = st.selectbox("Escolha um arquivo:", csv_files)
                file_path = os.path.join(default_dir, selected_file)
            else:
                st.warning("Nenhum arquivo CSV encontrado no diretório padrão.")
                file_path = st.text_input("Digite o caminho completo do arquivo CSV:")
        else:
            file_path = st.text_input("Digite o caminho completo do arquivo CSV:")
    
    with col2:
        st.write("")
        st.write("")
        if st.button("📂 Carregar Arquivo", type="primary", use_container_width=True):
            if file_path and os.path.exists(file_path):
                if carregar_arquivo(file_path):
                    st.success("✅ Arquivo carregado com sucesso!")
                    st.rerun()
            else:
                st.error("Arquivo não encontrado!")


def pagina_visualizar():
    """Página de visualização dos dados"""
    st.markdown('<div class="main-header">📋 Visualização de Dados</div>', unsafe_allow_html=True)
    
    if st.session_state.reader is None:
        st.warning("Por favor, carregue um arquivo primeiro.")
        return
    
    # Garantir amostra disponível
    if not ensure_sample_loaded(1000):
        st.info("Use o painel lateral para carregar mais linhas ou selecione outro arquivo.")
        return
    
    # Informações do arquivo
    with st.expander("ℹ️ Informações do Arquivo", expanded=False):
        info = st.session_state.file_info
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Tamanho", info['size_mb'])
        with col2:
            st.metric("Encoding", info['encoding'])
        with col3:
            st.metric("Delimitador", f"'{info['delimiter']}'")
        with col4:
            try:
                col_count = len(st.session_state.df_sample.columns) if st.session_state.df_sample is not None else 0
            except Exception:
                col_count = 0
            st.metric("Colunas", col_count)
    
    # Configurações de visualização
    st.sidebar.subheader("⚙️ Configurações")
    
    rows_per_page = st.sidebar.slider("Linhas por página:", 10, 500, 50)
    
    # Seleção de colunas
    all_columns = []
    try:
        if st.session_state.df_sample is not None:
            all_columns = st.session_state.df_sample.columns.tolist()
        else:
            all_columns = st.session_state.reader.get_column_names()
    except Exception:
        all_columns = []
    selected_columns = st.sidebar.multiselect(
        "Colunas para exibir:",
        all_columns,
        default=all_columns[:10] if len(all_columns) > 10 else all_columns
    )
    
    if not selected_columns:
        selected_columns = all_columns
    
    # Opções de carregamento
    load_option = st.sidebar.radio(
        "Modo de carregamento:",
        ["Amostra (1000 linhas)", "Carregar mais linhas", "Arquivo completo (cuidado!)"]
    )
    
    # Carregar dados baseado na opção
    if load_option == "Amostra (1000 linhas)":
        df_display = st.session_state.df_sample if st.session_state.df_sample is not None else pd.DataFrame()
    elif load_option == "Carregar mais linhas":
        n_rows = st.sidebar.number_input("Número de linhas:", 1000, 100000, 10000)
        if st.sidebar.button("Carregar"):
            with st.spinner(f'Carregando {n_rows:,} linhas...'):
                df_display = st.session_state.reader.read_sample(n_rows)
                st.success(f"✅ {len(df_display):,} linhas carregadas!")
        else:
            df_display = st.session_state.df_sample if st.session_state.df_sample is not None else pd.DataFrame()
    else:
        st.sidebar.warning("⚠️ Carregar arquivo completo pode consumir muita memória!")
        if st.sidebar.button("Carregar TUDO", type="primary"):
            with st.spinner('Carregando arquivo completo...'):
                try:
                    chunks = []
                    for chunk in st.session_state.reader.read_in_chunks(10000):
                        chunks.append(chunk)
                    df_display = pd.concat(chunks, ignore_index=True)
                    st.success(f"✅ {len(df_display):,} linhas carregadas!")
                except Exception as e:
                    st.error(f"Erro: {e}")
                    df_display = st.session_state.df_sample
        else:
            df_display = st.session_state.df_sample if st.session_state.df_sample is not None else pd.DataFrame()
    
    # Filtros
    st.subheader("🔍 Filtros")
    
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        search_column = st.selectbox("Coluna para filtrar:", ["Todas"] + selected_columns)
    
    with filter_col2:
        search_term = st.text_input("Termo de busca:")
    
    with filter_col3:
        st.write("")
        st.write("")
        apply_filter = st.button("🔎 Aplicar Filtro")
    
    # Aplicar filtro
    if apply_filter and search_term:
        if search_column == "Todas":
            mask = df_display.astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
        else:
            mask = df_display[search_column].astype(str).str.contains(
                search_term, case=False, na=False
            )
        df_display = df_display[mask]
        st.info(f"📊 {len(df_display):,} linhas encontradas")
    
    # Exibir dados
    st.subheader("📊 Dados")
    
    # Paginação
    total_rows = len(df_display)
    total_pages = (total_rows - 1) // rows_per_page + 1
    
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    
    with col1:
        if st.button("⏮️ Primeira"):
            st.session_state.current_page = 0
    
    with col2:
        if st.button("◀️ Anterior"):
            if st.session_state.current_page > 0:
                st.session_state.current_page -= 1
    
    with col3:
        st.markdown(f"<div style='text-align: center; padding: 0.5rem;'>"
                   f"Página {st.session_state.current_page + 1} de {total_pages} "
                   f"({total_rows:,} linhas totais)</div>", 
                   unsafe_allow_html=True)
    
    with col4:
        if st.button("▶️ Próxima"):
            if st.session_state.current_page < total_pages - 1:
                st.session_state.current_page += 1
    
    with col5:
        if st.button("⏭️ Última"):
            st.session_state.current_page = total_pages - 1
    
    # Calcular índices da página
    start_idx = st.session_state.current_page * rows_per_page
    end_idx = min(start_idx + rows_per_page, total_rows)
    
    # Exibir tabela
    # Evitar erro quando não há dados
    if df_display is None or df_display.empty:
        st.warning("Nenhum dado para exibir. Carregue mais linhas ou selecione outro arquivo.")
        return
    df_page = df_display.iloc[start_idx:end_idx][selected_columns]
    st.dataframe(df_page, use_container_width=True, height=600)
    
    # Opções de exportação
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.write(f"**Dados exibidos:** Linhas {start_idx + 1} a {end_idx} de {total_rows:,}")
    
    with col2:
        # Exportar apenas as colunas selecionadas
        csv = df_display[selected_columns].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="💾 Exportar Dados Filtrados",
            data=csv,
            file_name="dados_ibge_filtrados.csv",
            mime="text/csv",
            use_container_width=True
        )


def pagina_estatisticas():
    """Página de estatísticas e análises"""
    st.markdown('<div class="main-header">📈 Estatísticas e Análises</div>', unsafe_allow_html=True)
    
    if st.session_state.reader is None:
        st.warning("Por favor, carregue um arquivo primeiro.")
        return
    
    # Garantir amostra disponível
    if not ensure_sample_loaded(1000):
        st.info("Use o painel lateral para carregar mais linhas ou selecione outro arquivo.")
        return
    
    df = st.session_state.df_sample
    
    # Resumo geral
    st.subheader("📊 Resumo Geral")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de Colunas", len(df.columns))
    
    with col2:
        st.metric("Linhas na Amostra", len(df))
    
    with col3:
        numeric_cols = df.select_dtypes(include=['number']).columns
        st.metric("Colunas Numéricas", len(numeric_cols))
    
    with col4:
        text_cols = df.select_dtypes(include=['object']).columns
        st.metric("Colunas de Texto", len(text_cols))
    
    # Informações das colunas
    st.markdown("---")
    st.subheader("📋 Informações das Colunas")
    
    col_info = []
    for col in df.columns:
        col_info.append({
            'Coluna': col,
            'Tipo': str(df[col].dtype),
            'Não Nulos': df[col].notna().sum(),
            'Nulos': df[col].isna().sum(),
            '% Nulos': f"{(df[col].isna().sum() / len(df) * 100):.2f}%",
            'Únicos': df[col].nunique()
        })
    
    df_info = pd.DataFrame(col_info)
    st.dataframe(df_info, use_container_width=True)
    
    # Estatísticas descritivas
    if len(numeric_cols) > 0:
        st.markdown("---")
        st.subheader("📊 Estatísticas Descritivas (Colunas Numéricas)")
        
        selected_numeric = st.multiselect(
            "Selecione colunas para análise:",
            numeric_cols.tolist(),
            default=numeric_cols.tolist()[:5] if len(numeric_cols) > 5 else numeric_cols.tolist()
        )
        
        if selected_numeric:
            st.dataframe(df[selected_numeric].describe(), use_container_width=True)
            
            # Gráficos
            st.markdown("---")
            st.subheader("📈 Visualizações")
            
            chart_type = st.selectbox(
                "Tipo de gráfico:",
                ["Histograma", "Box Plot", "Gráfico de Linhas", "Correlação"]
            )
            
            if chart_type == "Histograma":
                col_to_plot = st.selectbox("Selecione uma coluna:", selected_numeric)
                fig = px.histogram(df, x=col_to_plot, title=f"Distribuição de {col_to_plot}")
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Box Plot":
                col_to_plot = st.selectbox("Selecione uma coluna:", selected_numeric)
                fig = px.box(df, y=col_to_plot, title=f"Box Plot de {col_to_plot}")
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Gráfico de Linhas":
                col_to_plot = st.selectbox("Selecione uma coluna:", selected_numeric)
                fig = px.line(df, y=col_to_plot, title=f"Tendência de {col_to_plot}")
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Correlação":
                if len(selected_numeric) > 1:
                    corr_matrix = df[selected_numeric].corr()
                    fig = px.imshow(
                        corr_matrix,
                        text_auto=True,
                        title="Matriz de Correlação",
                        color_continuous_scale="RdBu_r"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Selecione pelo menos 2 colunas para correlação.")
    
    # Valores mais frequentes
    st.markdown("---")
    st.subheader("🔝 Valores Mais Frequentes")
    
    col_for_freq = st.selectbox("Selecione uma coluna:", df.columns.tolist())
    top_n = st.slider("Top N valores:", 5, 50, 10)
    
    value_counts = df[col_for_freq].value_counts().head(top_n)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.dataframe(value_counts.reset_index(), use_container_width=True)
    
    with col2:
        fig = px.bar(
            x=value_counts.values,
            y=value_counts.index.astype(str),
            orientation='h',
            title=f"Top {top_n} valores mais frequentes"
        )
        st.plotly_chart(fig, use_container_width=True)


def main():
    """Função principal"""
    inicializar_sessao()
    
    # Sidebar
    st.sidebar.title("📊 Menu Principal")
    
    if st.session_state.reader is None:
        pagina = st.sidebar.radio("Navegação:", ["🏠 Início"])
    else:
        # Mostrar informações do arquivo carregado
        st.sidebar.success("✅ Arquivo Carregado")
        if st.session_state.file_info:
            st.sidebar.info(
                f"**Arquivo:** {Path(st.session_state.file_info['path']).name}\n\n"
                f"**Tamanho:** {st.session_state.file_info['size_mb']}"
            )
        
        if st.sidebar.button("🔄 Trocar Arquivo"):
            st.session_state.reader = None
            st.session_state.df_sample = None
            st.session_state.file_info = None
            st.rerun()
        
        st.sidebar.markdown("---")
        
        pagina = st.sidebar.radio(
            "Navegação:",
            ["🏠 Início", "📋 Visualizar Dados", "📈 Estatísticas"]
        )
    
    # Renderizar página selecionada
    if pagina == "🏠 Início":
        pagina_inicial()
    elif pagina == "📋 Visualizar Dados":
        pagina_visualizar()
    elif pagina == "📈 Estatísticas":
        pagina_estatisticas()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.8rem;'>
        <p>Leitor de CSV - IBGE</p>
        <p>v1.0 - 2026</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
