# Coffee Shop DW
# 
Projeto de construção de um Data Warehouse no Snowflake orquestrando as tarefas com Apache Airflow.
# 
# 📁 Estrutura do Projeto
 
```
coffee-shop-dw/
├── config/             # Arquivos de configuração (atualmente vazio)
├── dags/               # DAGs do Apache Airflow
│   └── dag_cafe_etl.py # DAG principal para carga no DW
├── logs/               # Diretório de logs gerados pelo Airflow
├── plugins/            # Plugins customizados para o Airflow (atualmente vazio)
├── docker-compose.yaml # Orquestração dos serviços com Docker
└── create_dw.sql       # Script SQL para criação das tabelas no Snowflake
```
 
🚀 Tecnologias e Ferramentas
 
- **Apache Airflow** (orquestração de workflows)
- **Docker** (ambiente isolado e portátil)
- **Snowflake** (Data Warehouse)
- **Python** (desenvolvimento da DAG)
 
#📦 Requisitos para execução local
 
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
 
▶️ Como executar o projeto

1. Clone este repositório:

    ```
    git clone https://github.com/seu-usuario/coffee-shop-dw.git
    cd coffee-shop-dw
    ```

 2. Suba o ambiente com o Docker Compose:
 
    ```
    docker-compose up
    ```
 
 3. Acesse o Airflow via navegador:
 
    ```
    http://localhost:8080
    ```
    Usuário padrão: `airflow`  
    Senha: `airflow`
 
 4. Execute o script `create_dw.sql` diretamente no console do Snowflake para criar as tabelas do DW.
 
 5. Execute a DAG `dag_cafe_etl` manualmente ou agende conforme desejado.
 
 📝 Observações
 
 - As pastas `config/`, `logs/` e `plugins/` estão incluídas propositalmente na estrutura, pois são importantes para o funcionamento do Airflow mesmo que estejam vazias no início.
 - Arquivos `.gitkeep` são utilizados para preservar essas pastas no controle de versão.
 
 📚 Sobre o projeto
 
 Este projeto foi desenvolvido como parte da disciplina de Engenharia de Dados na pós-graduação
