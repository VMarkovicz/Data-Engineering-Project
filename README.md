# Data Engineering Project

## Requirements

List of the required tools to run the project.
- Docker
  - Apache Spark
  - Apache Airflow
  - PostgreSQL
- Python Virtual Environment
  - With libraries listed in requirements.txt
  - Python >= 3.13
- Databases
  - World Bank 
    - Already included in the repo.
    - Data (src\raw_datasets\WB_DATA_usa_2014_onwards.csv)
    - Metadata (src\raw_datasets\WB_METADATA_usa_2014_onwards.csv)
  - BitFinex
    - Already included in the repo.
    - Data (src\raw_datasets\QDL_BITFINEX_bd501c887fbc1cc545f11778912a9118.csv)
  - Zillow Databases
    - Data, Indicators and Regions
    - Can be downloaded from: [Zillow datasets link](https://drive.google.com/drive/folders/10DMfQ3-oiKxioS1XS5hJTgSQujb8sEGV?usp=drive_link)
    - Need to be in this path:
      - src\raw_datasets\
        - ZILLOW_DATA_962c837a6ccefddddf190101e0bafdaf.csv
        - ZILLOW_INDICATORS_e93833a53d6c88463446a364cda611cc.csv
        - ZILLOW_REGIONS_1a51d107db038a83ac171d604cb48d5b.csv
  - Stock and Crypto Data
    - Dayly updated data from Alpaca API
    - Need a API Key from [Alpaca](https://alpaca.markets/)
      - Stored in .env file as:
        - ALPACA_API_KEY
        - ALPACA_API_SECRET_KEY
      - Copied as well to Airflow variables:
        - AIRFLOW_VAR_ALPACA_API_KEY
        - AIRFLOW_VAR_ALPACA_API_SECRET_KEY
    - Stored in (src\raw_datasets\cryptostcks\\*)
  OPENAI API Key
    - For RAG service
    - Get your API key from [OpenAI](https://platform.openai.com/account/api-keys)
      - Stored in .env file as OPENAI_AI_KEY


## Python Requirements

  #### Windows (PowerShell)
  ```powershell
  # Create virtual environment
  python -m venv .venv

  # Activate virtual environment
  .\.venv\Scripts\Activate.ps1

  # Install requirements
  pip install -r requirements.txt
  ```

  #### Linux / Mac
  ```bash
  # Create virtual environment
  python3 -m venv .venv

  # Activate virtual environment
  source .venv/bin/activate

  # Install requirements
  pip install -r requirements.txt
  ```


## Docker Setup

#### Build and Start Docker Containers

  **All Platforms (Windows/Linux/Mac):**
  ```sh
  # Build and start all containers
  docker compose up

  # Or build first, then start
  docker compose build
  docker compose up
  ```

  #### Stop Docker Containers
  ```sh
  # Stop all containers
  docker compose down

  # Stop and remove volumes (clean slate)
  docker compose down -v
  ```

  #### Run Ingestion and Processing Pipelines
  - Airflow Webserver: http://localhost:8081
  - Airflow Username: admin
  - Airflow Password: admin
  - Airflow DAGs:
    - full_data_ingestion_pipeline: Activate this DAG to run the full data ingestion and processing pipeline.
      - This includes the following sub-pipelines:
          - Bronze and Silver data ingestion for:
            - Wold Bank data
            - Bitfinex + previous stocks from Alpaca + stock_daily_pipeline
            - Zillow data
          - Gold data processing for unification of all datasets 
          - Validation and loading into a star schema in DuckDB.
          - Creation of vector embeddings and loading into PostgreSQL for RAG service.
          
    - OBS: It will automatically trigger daily updates for stock and crypto data (stock_daily_pipeline).


## Access API 
  - The API service is available at: http://localhost:3333/api
  - It's docs available at: http://localhost:3333/api/docs


## Access Metabase Interface
  - The Metabase service is available at: http://localhost:3333/apps/metabase
  - Requires user setup on first access.
  - Requires to specify DuckDB file path
    - /datalake/analytics/warehouse_star.duckdb
  - Set DuckDB connection as read-only.
    - If metabase loads backup data correctly during setup, you don't need to follow the previous steps
    - Just access the login page and set these credentials
      - email: jonathan224santos@gmail.com
      - password: 111722js

## RAG Service
  - The RAG service is available at: src\rag\rag_execution.py
  - Make sure to set your OpenAI API key in the .env file before running.
  - Run the RAG service:

    **All Platforms (Windows/Linux/Mac):**
    ```sh
    python src/rag/rag_execution.py
    ```
  
  - Example questions to ask:
    - Type 'test' to run example queries.