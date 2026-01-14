"""
Exemplos práticos de uso do leitor de CSV
"""

from csv_reader import CSVReader, CSVAnalyzer
import pandas as pd


def exemplo1_informacoes_basicas():
    """Exemplo 1: Obter informações básicas do arquivo"""
    print("=" * 70)
    print("EXEMPLO 1: Informações Básicas do Arquivo")
    print("=" * 70)
    
    # Substitua pelo caminho do seu arquivo
    file_path = input("Digite o caminho do arquivo CSV: ").strip().strip('"')
    
    reader = CSVReader(file_path)
    
    # Informações do arquivo
    info = reader.get_file_info()
    print("\nInformações do arquivo:")
    for key, value in info.items():
        print(f"  {key}: {value}")
    
    # Primeiras linhas
    print("\nPrimeiras 5 linhas:")
    sample = reader.read_sample(5)
    print(sample)
    
    # Nomes das colunas
    print("\nColunas disponíveis:")
    for i, col in enumerate(reader.get_column_names(), 1):
        print(f"  {i}. {col}")


def exemplo2_processar_em_chunks():
    """Exemplo 2: Processar arquivo grande em chunks"""
    print("\n" + "=" * 70)
    print("EXEMPLO 2: Processamento em Chunks")
    print("=" * 70)
    
    file_path = input("Digite o caminho do arquivo CSV: ").strip().strip('"')
    reader = CSVReader(file_path)
    
    # Processar em chunks de 10000 linhas
    total_rows = 0
    chunk_count = 0
    
    print("\nProcessando arquivo...")
    for chunk in reader.read_in_chunks(chunk_size=10000):
        chunk_count += 1
        total_rows += len(chunk)
        
        # Exemplo de processamento: contar valores únicos em uma coluna
        if chunk_count == 1:
            print(f"\nColunas disponíveis: {', '.join(chunk.columns.tolist())}")
        
        # Mostrar progresso a cada 5 chunks
        if chunk_count % 5 == 0:
            print(f"Processados {total_rows:,} linhas em {chunk_count} chunks...")
    
    print(f"\nProcessamento concluído!")
    print(f"Total de linhas: {total_rows:,}")
    print(f"Total de chunks: {chunk_count}")


def exemplo3_analise_estatistica():
    """Exemplo 3: Análise estatística dos dados"""
    print("\n" + "=" * 70)
    print("EXEMPLO 3: Análise Estatística")
    print("=" * 70)
    
    file_path = input("Digite o caminho do arquivo CSV: ").strip().strip('"')
    reader = CSVReader(file_path)
    
    # Análise de amostra
    print("\nAnalisando amostra dos dados...")
    analysis = reader.analyze_data(sample_size=50000)
    
    print(f"\nTotal de colunas: {analysis['total_columns']}")
    print(f"Linhas analisadas: {analysis['sample_rows']:,}")
    print(f"Uso de memória: {analysis['memory_usage_mb']:.2f} MB")
    
    print("\nTipos de dados:")
    for col, dtype in analysis['column_types'].items():
        print(f"  {col}: {dtype}")
    
    print("\nValores faltantes:")
    has_missing = False
    for col, missing in analysis['missing_values'].items():
        if missing > 0:
            has_missing = True
            percentage = (missing / analysis['sample_rows']) * 100
            print(f"  {col}: {missing:,} ({percentage:.2f}%)")
    
    if not has_missing:
        print("  Nenhum valor faltante encontrado!")
    
    # Estatísticas de colunas numéricas
    print("\nCalculando estatísticas das colunas numéricas...")
    stats = CSVAnalyzer.get_statistics(reader, chunk_size=10000)
    
    if stats:
        print("\nEstatísticas:")
        for col, values in stats.items():
            print(f"\n  {col}:")
            print(f"    Mínimo: {values['min']:,.2f}")
            print(f"    Máximo: {values['max']:,.2f}")
            print(f"    Média: {values['mean']:,.2f}")
            print(f"    Total de valores: {values['count']:,}")
    else:
        print("  Nenhuma coluna numérica encontrada.")


def exemplo4_filtrar_dados():
    """Exemplo 4: Filtrar e exportar dados"""
    print("\n" + "=" * 70)
    print("EXEMPLO 4: Filtrar e Exportar Dados")
    print("=" * 70)
    
    file_path = input("Digite o caminho do arquivo CSV: ").strip().strip('"')
    reader = CSVReader(file_path)
    
    # Mostrar colunas disponíveis
    sample = reader.read_sample(5)
    print("\nColunas disponíveis:")
    for i, col in enumerate(sample.columns, 1):
        print(f"  {i}. {col}")
    
    print("\nExemplo de filtro: manter apenas linhas onde uma coluna atende condição")
    print("(Este é um exemplo genérico - adapte para suas necessidades)")
    
    # Exemplo: filtrar linhas onde a primeira coluna não é nula
    first_col = sample.columns[0]
    
    def filtro(df):
        # Filtrar apenas linhas onde a primeira coluna não é nula
        return df[first_col].notna()
    
    output_path = "dados_filtrados.csv"
    print(f"\nFiltrando dados e salvando em: {output_path}")
    
    total_filtered = CSVAnalyzer.filter_data(
        reader,
        condition=filtro,
        output_path=output_path,
        chunk_size=10000
    )
    
    print(f"\nFiltro aplicado com sucesso!")
    print(f"Total de linhas no arquivo filtrado: {total_filtered:,}")


def exemplo5_converter_formato():
    """Exemplo 5: Converter e processar dados"""
    print("\n" + "=" * 70)
    print("EXEMPLO 5: Converter e Processar Dados")
    print("=" * 70)
    
    file_path = input("Digite o caminho do arquivo CSV: ").strip().strip('"')
    reader = CSVReader(file_path)
    
    # Mostrar colunas
    sample = reader.read_sample(5)
    print("\nColunas disponíveis:")
    for i, col in enumerate(sample.columns, 1):
        print(f"  {i}. {col}")
    
    # Função de processamento personalizada
    def processar_chunk(chunk):
        """
        Exemplo de processamento:
        - Remove espaços em branco extras de colunas de texto
        - Converte colunas para tipos apropriados
        """
        # Remover espaços de colunas de texto
        for col in chunk.select_dtypes(include=['object']).columns:
            chunk[col] = chunk[col].str.strip() if chunk[col].dtype == 'object' else chunk[col]
        
        # Aqui você pode adicionar mais processamentos:
        # - Remover colunas desnecessárias
        # - Criar novas colunas calculadas
        # - Filtrar valores
        # etc.
        
        return chunk
    
    output_path = "dados_processados.csv"
    print(f"\nProcessando e salvando em: {output_path}")
    
    reader.process_and_save(
        output_path=output_path,
        chunk_size=10000,
        process_func=processar_chunk
    )
    
    print(f"\nArquivo processado com sucesso!")
    print(f"Total de linhas processadas: {reader.total_rows:,}")


def exemplo6_contar_linhas():
    """Exemplo 6: Contar total de linhas"""
    print("\n" + "=" * 70)
    print("EXEMPLO 6: Contar Total de Linhas")
    print("=" * 70)
    
    file_path = input("Digite o caminho do arquivo CSV: ").strip().strip('"')
    reader = CSVReader(file_path)
    
    print("\nContando linhas do arquivo...")
    total = reader.count_rows()
    
    print(f"\nTotal de linhas no arquivo: {total:,}")
    
    # Estimar tamanho por linha
    file_size_bytes = reader.file_path.stat().st_size
    bytes_per_row = file_size_bytes / (total + 1)  # +1 para incluir cabeçalho
    
    print(f"Tamanho médio por linha: {bytes_per_row:.2f} bytes")


def menu_exemplos():
    """Menu principal de exemplos"""
    print("=" * 70)
    print("EXEMPLOS DE USO - LEITOR DE CSV")
    print("=" * 70)
    
    while True:
        print("\nEscolha um exemplo para executar:")
        print("1. Informações básicas do arquivo")
        print("2. Processar arquivo em chunks")
        print("3. Análise estatística dos dados")
        print("4. Filtrar e exportar dados")
        print("5. Converter e processar dados")
        print("6. Contar total de linhas")
        print("0. Sair")
        
        choice = input("\nOpção: ").strip()
        
        try:
            if choice == '1':
                exemplo1_informacoes_basicas()
            elif choice == '2':
                exemplo2_processar_em_chunks()
            elif choice == '3':
                exemplo3_analise_estatistica()
            elif choice == '4':
                exemplo4_filtrar_dados()
            elif choice == '5':
                exemplo5_converter_formato()
            elif choice == '6':
                exemplo6_contar_linhas()
            elif choice == '0':
                print("\nEncerrando exemplos...")
                break
            else:
                print("\nOpção inválida!")
        except Exception as e:
            print(f"\nErro ao executar exemplo: {e}")
            import traceback
            traceback.print_exc()
        
        input("\nPressione Enter para continuar...")


if __name__ == "__main__":
    menu_exemplos()
