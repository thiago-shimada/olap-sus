# Análise Integrada de Dados de Saúde Pública Brasileira

O código desse repositório se refere a segunda atividade da matéria de SCC0245 - Processamento Analítico de Dados (2025).
Desenvolvido pelos alunos:

* 12542352 - Fernando Gonçalves Campos
* 12542626 - Samuel Figueiredo Veronez
* 12691032 - Thiago Shimada


O respositório contem os fluxos de processameto e o dashboard para visualização de dados de Nascimento e Mortalidade obtidos pelo sus, em:
* SINASC = https://opendatasus.saude.gov.br/dataset/sistema-de-informacao-sobre-nascidos-vivos-sinasc
* SIM: https://opendatasus.saude.gov.br/dataset/sim

---
## Como executar:

1. Clone o projeto e navegue até a pasta raiz
2. Execute o comando docker compose up -d
3. Acesse o MinIO no navegador: http://localhost:9001
    * Faça login com usuário e senha: minioadmin / minioadmin
    * Crie um bucket chamado landing
    * Dentro do bucket, crie a seguinte estrutura de pastas:
source_sus/sim/dt=2025-11-27. (Obs: além de sim, utilize sinasc, e altere a data conforme necessidade)
    Adicione um arquivo de óbitos dentro desta pasta
4. No navegador, acesse o Airflow: http://localhost:8080
    * Faça login com usuário e senha: airflow / airflow
    * No menu lateral esquerdo, vá em Administração -> Conexões
    * Clique em Adicionar (botão no canto superior direito)
    * Configure a conexão com os seguintes parâmetros:
        * ID da conexão: spark_conn
        * Tipo da conexão: Spark
        * Host: spark://spark-master:7077
    * No metu lateral esquerdo, vá em DAGs e procure por sus_minio_ingest_dag
    * Ative a DAG pressionando o slider