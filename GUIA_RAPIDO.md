# Guia Rápido - GeoGrafi

## 1) Instalação

```bash
python -m pip install -r requirements.txt
```

## 2) Executar a interface principal

```bash
streamlit run app_geo.py
```

## 3) Fluxo em 1 minuto

1. Abra a aba Processar
2. Envie o CSV
3. Confirme preview, encoding e delimitador detectados
4. Ajuste chunk size e workers se necessário
5. Clique em Iniciar Processamento
6. Baixe o CSV enriquecido

## Colunas canônicas esperadas

- CD_CEP
- NM_LOGRADOURO
- NM_BAIRRO
- NM_MUNICIPIO
- NM_UF

Observação: o app detecta colunas alternativas automaticamente (como CEP, NR_CEP, ENDERECO, BAIRRO, CIDADE, UF etc.).

## Saída gerada

- CD_CEP_CORRETO
- NM_LOGRADOURO_CORRETO
- NM_BAIRRO_CORRETO
- NM_MUNICIPIO_CORRETO
- NM_UF_CORRETO
- DS_LATITUDE
- DS_LONGITUDE

## Configuração recomendada

- Arquivo pequeno (< 500 MB): chunk_size 2000 a 5000, workers 3 a 6
- Arquivo médio (500 MB a 1.5 GB): chunk_size 1000 a 2000, workers 2 a 4
- Arquivo grande (> 1.5 GB): chunk_size 500 a 1000, workers 1 a 3

## Diagnóstico rápido

### Testar conectividade das APIs

```bash
python test_api_connection.py
```

### Limpar cache antigo

Use o botão Limpar cache antigo na barra lateral da interface.

## Modo legado (opcional)

Se você quiser apenas leitura/análise genérica de CSV, ainda pode usar:

```bash
python csv_reader.py
```

Mas para o produto atual, use app_geo.py.
