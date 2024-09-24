# ML_Items - Pipeline de Extração de Dados do Mercado Livre

O projeto consiste em construir um pipeline de dados utilizando Airflow para realizar a extração de dados da página do Mercado Livre. O pipeline busca informações de um item escolhido (neste exemplo, bicicletas) e armazena os dados extraídos no PostgreSQL.

## Pré-requisitos

Certifique-se de ter as seguintes ferramentas instaladas em sua máquina:
- **Python** (versão 3.9)
- **Docker** e **Docker Compose**

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/humrique/ML_itens.git
   cd ML_itens/

2. Ambiente Virtual:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

3. Suba os containers Docker:
    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    docker compose up airflow-init
    docker compose up

4. Acesse o Airflow:
    ```bash
    http://localhost:8080/
    # Usuário: airflow
    # Senha: airflow
    
5. Configurando uma conneciton com o Postgress no Airflow
   - No Airflow, crie uma nova conexão com o PostgreSQL. Como os containers estão na mesma rede Docker, o Host pode ser configurado com o nome do serviço Docker (postgres).
   - TODO: Automatizar essa conexão na primeira vez que o Docker subir.
     ```bash
     #Connection Id: ml_itens_connection
     #Connection Type: Postgres
     #Host: postgres
     #Database: ml_itens
     #Login: airflow
     #Password: airflow
     
   ![image](https://github.com/user-attachments/assets/51b4ccc8-36c3-428e-aa8d-359b46e554bf)
6. Execute a DAG
   ![image](https://github.com/user-attachments/assets/d47a0538-1ecb-4d56-b5a2-6bdf940526da)


## Visualização dos Dados
   - Acesse PgAdmin -> http://localhost:5050      
   - Acesse o Query Tool
        ![image](https://github.com/user-attachments/assets/5ffb56cb-9a44-4da7-b938-d41bcdb1954e)
   - Execute essa consulta SQL para vizualisar os dados
        ```bash
        SELECT * FROM ITENS

   ![image](https://github.com/user-attachments/assets/62fe8173-3d14-4bfd-8525-5d3a10838d0d)

##  TODO Projeto
  - Tratar os dados antes de inserir no banco
  - Criar uma data lake local utilizando arquitetura de dados em camadas ( Bronze, Silver e Gold)
  - Subir esse data lake para a AWS utilizando S3 
