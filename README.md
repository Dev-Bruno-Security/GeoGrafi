# GeoGrafi

Aplicação Python para enriquecimento geográfico de arquivos CSV grandes.

O fluxo principal valida e corrige CEPs, busca coordenadas e devolve um CSV enriquecido com processamento em chunks, cache local e interface Streamlit.

## Principais recursos

- Processamento em chunks para arquivos grandes
- Detecção automática de encoding e delimitador
- Validação de CEP com ViaCEP
- Geocodificação com Nominatim (OpenStreetMap)
- Cache local em SQLite para reduzir chamadas repetidas
- Mapeamento automático de colunas comuns
- Interface web com Streamlit para upload, processamento e download

## Requisitos

- Python 3.10+
- Dependências em requirements.txt

## Instalação

```bash
python -m pip install -r requirements.txt
```

## Como executar

### App principal (recomendado)

```bash
streamlit run app_geo.py
```

A interface abre no navegador e permite:

1. Enviar um arquivo CSV
2. Validar colunas obrigatórias (com mapeamento automático)
3. Ajustar chunk size e workers
4. Processar e baixar o CSV resultante

### Smoke test de conectividade com APIs

```bash
python test_api_connection.py
```

## Colunas esperadas no fluxo geográfico

Obrigatórias no modelo canônico:

- CD_CEP
- NM_LOGRADOURO
- NM_BAIRRO
- NM_MUNICIPIO
- NM_UF

Também são aceitos nomes alternativos (mapeamento automático), por exemplo:

- CD_CEP: NR_CEP, CEP
- NM_LOGRADOURO: DS_ENDERECO, ENDERECO, LOGRADOURO
- NM_BAIRRO: DS_BAIRRO, BAIRRO
- NM_MUNICIPIO: NM_CIDADE, CIDADE, MUNICIPIO, DS_MUNICIPIO
- NM_UF: UF, ESTADO, DS_UF

## Colunas adicionadas/atualizadas no resultado

- CD_CEP_CORRETO
- NM_LOGRADOURO_CORRETO
- NM_BAIRRO_CORRETO
- NM_MUNICIPIO_CORRETO
- NM_UF_CORRETO
- DS_LATITUDE
- DS_LONGITUDE

## Estrutura do projeto

- app_geo.py: interface Streamlit principal
- modules/csv_processor.py: processamento em chunks e enriquecimento geográfico
- modules/cep_validator.py: integração com ViaCEP
- modules/geocoder.py: integração com Nominatim
- modules/cache_manager.py: cache SQLite
- test_api_connection.py: diagnóstico de conectividade

## Modo legado (CSV genérico)

O repositório também mantém utilitários antigos para leitura genérica de CSV:

- csv_reader.py
- interface_visual.py
- app_geo_simples.py

Esses arquivos continuam disponíveis, mas o fluxo recomendado para o produto atual é app_geo.py.

## Dicas de performance

- Até 500 MB: chunk_size entre 2000 e 5000
- 500 MB a 1.5 GB: chunk_size entre 1000 e 2000
- Acima de 1.5 GB: chunk_size entre 500 e 1000
- Mantenha cache ativado para reprocessamentos

## Observações sobre APIs

- ViaCEP e Nominatim têm políticas de uso e limite de requisição
- O projeto aplica rate limiting e retries para reduzir falhas transitórias

## Licença

Defina aqui a licença do projeto, se aplicável.
