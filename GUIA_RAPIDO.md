# Guia Rápido - Leitor de CSV Grande

## Instalação Rápida

```bash
# 1. Instalar dependências
pip install pandas chardet

# 2. Executar o programa
python csv_reader.py
```

## Uso Mais Simples

```python
from csv_reader import CSVReader

# Criar leitor
reader = CSVReader("seu_arquivo.csv")

# Ver amostra
print(reader.read_sample(10))

# Processar tudo
for chunk in reader.read_in_chunks(10000):
    print(f"Processando {len(chunk)} linhas")
    # Seu código aqui
```

## Comandos Úteis

### Ver informações do arquivo
```python
reader = CSVReader("arquivo.csv")
info = reader.get_file_info()
print(info)
```

### Contar linhas
```python
total = reader.count_rows()
print(f"Total: {total:,} linhas")
```

### Exportar filtrado
```python
from csv_reader import CSVAnalyzer

def meu_filtro(df):
    return df['coluna'] > 100  # sua condição

CSVAnalyzer.filter_data(reader, meu_filtro, "saida.csv")
```

### Calcular estatísticas
```python
stats = CSVAnalyzer.get_statistics(reader)
print(stats)
```

## Problemas Comuns

### "Encoding error"
```python
reader = CSVReader("arquivo.csv")
reader.encoding = "latin-1"  # ou "iso-8859-1"
```

### "Delimiter error"
```python
reader = CSVReader("arquivo.csv")
reader.delimiter = ";"  # ou "\t"
```

### Memória insuficiente
```python
# Use chunks menores
for chunk in reader.read_in_chunks(5000):  # ao invés de 10000
    # processar
```

## Ajuste de Performance

| Tamanho do Arquivo | Chunk Size Recomendado |
|-------------------|------------------------|
| < 500 MB          | 50000                 |
| 500 MB - 1 GB     | 20000                 |
| 1 GB - 2 GB       | 10000                 |
| > 2 GB            | 5000                  |

## Exemplos Prontos

Execute o arquivo de exemplos:
```bash
python exemplo_uso.py
```

Escolha um dos 6 exemplos práticos disponíveis!
