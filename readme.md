## Assignment - Data Engineer.

Diário de Bordo
A amostra de dados em anexo (info_transportes.csv), possui dados de um aplicativo de transporte privado, cujas colunas são:

DATA_INICIO (formato: "mm-dd-yyyy HH")

DATA_FIM (formato: "mm-dd-yyyy HH")

CATEGORIA ∘ LOCAL_INICIO ∘ LOCAL_FIM ∘ PROPOSITO

DISTANCIA

Uma equipe está elaborando um modelo para compreender como os clientes estão utilizando o aplicativo. Para isso, você precisa fornecer uma nova tabela chamada "info_corridas_do_dia", com dados agrupados pela data de início da corrida utilizando a formatação "yyyy-MM-dd", contendo as seguintes colunas:

Nome da coluna	Descrição
DT_REF	Data de referência.
QT_CORR	Quantidade de corridas.
QT_CORR_NEG	Quantidade de corridas com a categoria “Negócio”.
QT_CORR_PESS	Quantidade de corridas com a categoria “Pessoal”.
VL_MAX_DIST	Maior distância percorrida por uma corrida.
VL_MIN_DIST	Menor distância percorrida por uma corrida.
VL_AVG_DIST	Média das distâncias percorridas.
QT_CORR_REUNI	Quantidade de corridas com o propósito de “Reunião”.
QT_CORR_NAO_REUNI	Quantidade de corridas com o propósito declarado e diferente de “Reunião”.

Exemplo de uma linha da tabela:

2022-01-01 | 20 | 12 | 8 | 2.2 | 0.7 | 1.1 | 6 | 10

Imagem 3: Monitoramento de Sensores IoT
Monitoramento de Sensores IoT

Descrição do Problema
Você precisa criar um sistema de monitoramento de sensores IoT que envia dados de sensores em tempo real para um tópico Kafka (producer) e consome esses dados para processamento e armazenamento (consumer).

Criar o Producer

Desenvolver um script em Python (ou outra linguagem de sua escolha) que gera dados falsos de sensores IoT e envia esses dados para um tópico Kafka.

Utilizar uma biblioteca como faker para gerar dados falsos.

Criar o Consumer

Desenvolver um script que consome os dados do tópico Kafka e processa esses dados.

Armazenar os dados consumidos em um banco de dados.
\

## Pré-requisitos
- Python 3.10+
- Java 11 ou superior instalado no sistema
  - Verifique com: `java -version`
  - Configure o JAVA_HOME (exemplo: `/usr/lib/jvm/java-11-openjdk-amd64`)

## durante o desenvolvimento:


- rodar os testes:
entrar na folder do pt1: PYTHONPATH=. pytest tests/