# Diario de Bordo

## Problem description
A amostra de dados em anexo (info_transportes.csv), possui dados de um aplicativo de transporte privado, cujas colunas são:

- DATA_INICIO (formato: "mm-dd-yyyy HH")
- DATA_FIM (formato: "mm-dd-yyyy HH")
- CATEGORIA
- LOCAL_INICIO
- LOCAL_FIM 
- PROPOSITO
- DISTANCIA

Uma equipe está elaborando um modelo para compreender como os clientes estão utilizando o aplicativo. Para isso, você precisa fornecer uma nova tabela “info_corridas_do_dia", com dados agrupados pela data de início do transporte utilizando a formatação “yyyy-MM-dd”, contendo as seguintes colunas:

| Coluna               | Descrição                                                                 |
|----------------------|---------------------------------------------------------------------------|
| DT_REF               | Data de referência                                                        |
| QT_CORR              | Quantidade total de corridas                                              |
| QT_CORR_NEG          | Quantidade de corridas com a categoria “Negócio”                          |
| QT_CORR_PESS         | Quantidade de corridas com a categoria “Pessoal”                          |
| VL_MAX_DIST          | Maior distância percorrida por uma corrida                                |
| VL_MIN_DIST          | Menor distância percorrida por uma corrida                                |
| VL_AVG_DIST          | Média das distâncias percorridas                                          |
| QT_CORR_REUNI        | Quantidade de corridas com o propósito declarado como “Reunião”           |
| QT_CORR_NAO_REUNI    | Quantidade de corridas com propósito declarado diferente de “Reunião”     |

---


Abaixo temos um exemplo de uma possível linha da tabela:

| DT_REF     | QT_CORR | QT_CORR_NEG | QT_CORR_PESS | VL_MAX_DIST | VL_MIN_DIST | VL_AVG_DIST | QT_CORR_REUNI | QT_CORR_NAO_REUNI |
|------------|----------|--------------|---------------|--------------|--------------|---------------|----------------|--------------------|
| 2022-01-01 | 20       | 12           | 8             | 2.2          | 0.7          | 1.1           | 6              | 10                 |

---
## 📁 Project Structure
```
.
├── docker-compose.yml
├── data/
│   ├── raw/
|   |   └── info_transportes.csv
|   ├── bronze/
|   |   └── bronze_info_corridas_do_dia.parquet/
|   ├── silver/
|   |   └── silver_info_corridas_do_dia.parquet/
|   ├── gold/
|   |   └── gold_info_corridas_do_dia.parquet/
├── etl/
|   ├── bronze/
|   |   ├──__init__.py
|   |   └── process_corridas_bronze.py
|   ├── silver/
|   |   ├──__init__.py
|   |   └── process_corridas_silver.py
|   ├── gold/
|   |   ├──__init__.py
|   |   └── process_corridas_gold.py
|   ├── __init__.py
|   └── run.py
├── tests/
|   ├── Dockerfile.tests
|   ├── test_corridas_processor_bronze.py
|   ├── test_corridas_processor_silver.py
|   ├── test_corridas_processor_gold.py
├── requirements.txt
├── Dockerfile
├── .gitignore
└── README.md
```

## 🚀 Getting Started
1. Clone the repository:
``` bash
git clone https://github.com/LuanFranco98/code-elevate.git
cd code-elevate
```
2. Start the project:
``` bash
docker compose up --build
```

This will start:
- ETL processer
- Tests

## 📊 Data Flow
1. Bronze Transformation (process_corridas_bronze.py):
    - Filers for partiton date formats following pattern and not null
    - Filters for the desired start and end dates
    - Saves as parquet, partitioned

1. Silver Transformation (process_corridas_silver.py):
    - Same filters as bronze
    - Data formatting to yyyy-MM-dd on columns *DATA_INICIO* and *DATA_FIM*
    - Null tratment on *PROPOSITO* column
    - LongType cast on *DISTANCIA* column

1. Gold Transformation (process_corridas_gold.py):
    - Same filters as bronze
    - Agregations

1. Tests (tests/):
    - Asserts that methods are working correctly



## 🛠 Environment Variables
Defined inside docker-compose.yml.


## 👤 Author
**Luan Silveira Franco**<br/>
*Data Engineer* 
