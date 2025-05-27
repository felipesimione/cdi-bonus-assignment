# CDI Bonus Assignment

This project processes wallet change data (CDC) and calculates daily CDI bonus payouts for users, leveraging Apache Spark for data transformation and PostgreSQL for storage.

**Now includes a Streamlit dashboard for observability and log exploration.**

## Table of Contents

- [Project Overview](#project-overview)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [Observability Dashboard](#observability-dashboard)
- [Production Recommendations](#production-recommendations)
- [Design Choices](#design-choices)
- [Project Folder Structure](#project-folder-structure)
- [Database Schema Diagram](#database-schema-diagram)
- [Functional Requirements Met](#functional-requirements-met)
- [Non-Functional Requirements Met](#non-functional-requirements-met)
- [Trade-offs and Limitations](#trade-offs-and-limitations)
- [Important Notes and Recommendations](#important-notes-and-recommendations)
- [Function Documentation](#function-documentation)
- [Business Rules](#business-rules)

---

## Project Overview

This service ingests raw CDC wallet data, processes historical balances, applies daily CDI rates, and calculates daily bonus payouts for each user. The results are stored in a PostgreSQL database. The pipeline is orchestrated using Apache Spark for scalability and performance.

## Installation

1. **Clone the repository:**
    ```sh
    git clone git@github.com:felipesimione/cdi-bonus-assignment.git
    cd cdi-bonus-assignment
    ```

2. **Install dependencies:**
    - Ensure you have Docker and Docker Compose installed on your system.
    - Python dependencies are managed via `requirements.txt` and installed inside the container automatically.

3. **Start the services:**
    ```sh
    docker-compose build --no-cache
    docker-compose up -d
    ```

    If you need to stop and remove all containers and volumes, run:
    ```sh
    docker-compose down --volumes
    ```

    This will:
    - Build the Spark and application containers.
    - Start PostgreSQL and Spark services.
    - Run the main pipeline.
    - Run the tests.
    - Create the `app.log` file.

## Usage

- The raw CDC files in the `data/raw_cdc_files` directory (e.g., `wallet_cdc_raw.csv`) will be created automatically when you run docker-compose.
- The main entry point is `src/main.py`, which orchestrates data ingestion, transformation, and loading.
- Logs are written to `src/logs/app.log`.

## Observability Dashboard

- The Streamlit dashboard for observability and log exploration is started automatically when you run `docker-compose up -d`.
- No manual command is needed; the dashboard will be available as soon as the containers are running.
- You can access the dashboard by:
    - Visiting [http://localhost:8501](http://localhost:8501) in your browser, **or**
    - Clicking the Streamlit container in Docker Desktop and opening the published port.
- The dashboard provides:
    - **CDI Bonus Observability:** Business and data quality metrics for bonus payouts.
    - **Log Explorer:** Interactive log viewer for batch processing logs.

> ⚠️ **Attention:**  
**Note:** *Please, after open the dashboard, click in the refresh button to ensure the correct information.*

## Testing

- Unit tests are located in the `tests` directory.
- The docker-compose process runs the tests and you can check the output in the terminal. If you would like to run tests locally after running docker-compose, use:
    ```sh
    docker-compose up tests
    ```

## Production Recommendations

- **Transactional Database:**  
  Stored the calculated daily bonus payouts in the `daily_bonus_payouts` table in a PostgreSQL database. For production, use a managed PostgreSQL instance with high availability, automated backups, and point-in-time recovery to ensure data durability and reliability.

- **Data Access:**  
  - Applications and services should access bonus payout data via secure SQL queries or through an API layer.
  - Example SQL query to retrieve daily bonuses for a user:
    ```sql
    SELECT * FROM daily_bonus_payouts WHERE user_id = '<USER_ID>' ORDER BY payout_date DESC;
    ```
  - For integration with other systems (e.g., payment processors, dashboards), consider exposing the data through a REST API or using ETL tools to synchronize with downstream services.

- **Data Ingestion:**  
  - For real-time or near-real-time ingestion, implement streaming pipelines (e.g., Apache Kafka + Spark Structured Streaming) to update the database as new CDC events arrive.
  - Ensure idempotency and proper deduplication to avoid double processing of events.

- **Schema Management:**  
  - Use database migration tools such as Alembic or Flyway to manage schema changes and versioning in a controlled and auditable way.

- **Monitoring and Observability:**  
  - Integrate with monitoring tools (e.g., Prometheus, Grafana) to track database health, ETL pipeline status, and data quality metrics.
  - Set up alerting for failures, delays, or data anomalies.
  - For quality check use properly tools like Soda and Amundsen (open source).
  - For data catolog use properly tools like Alation and Glue Catalog.

- **Security:**  
  - Store credentials and sensitive information securely using environment variables or a secrets manager.
  - Restrict database access to authorized services and users only, following the principle of least privilege.
  - Enable SSL/TLS for all database connections.

- **Data Consumption:**  
  - Downstream systems (such as payment engines, reporting tools, or analytics platforms) should read from the `daily_bonus_payouts` table, filtering by date, user, or status as needed.
  - The `daily_interest_rates` table is also available for auditing or recalculation purposes.

- **Backup and Disaster Recovery:**  
  - Schedule regular backups of the production database and test restore procedures periodically.
  - Document recovery steps and ensure they are part of your operational runbooks.

- **Performance:**  
  - Monitor query performance and add indexes as needed (already present in the schema) to support efficient access patterns.
  - Partition large tables if necessary to maintain performance at scale.
<br><br>

> ⚠️ **Attention:**  
**Note:** *The daily CDI rates are stored in the `daily_interest_rates` table and can be accessed similarly for auditing or recalculation purposes.*

<br>

## Design Choices

- **Spark for ETL:** Chosen for scalability and efficient processing of large CDC datasets.
- **PostgreSQL:** Reliable, ACID-compliant storage for transactional and analytical queries.
- **Modular Structure:** Code is organized into logical modules for data setup, transformation, and utility functions. *more below*
- **Schema Design:** Tables are normalized for data integrity, with indexes for efficient querying (see `create_tables.sql`). *more below*
- **Logging:** Centralized logging for traceability and debugging.
- **Streamlit Dashboard:** Provides observability and log exploration for transparency and troubleshooting.

### Project Folder Structure

```text
cdi-bonus-assignment/
│
├── data/
│   └── raw_cdc_files/           # Raw CDC wallet data files
│
├── sql/
│   └── create_tables.sql        # SQL script for table creation
│
├── src/
│   ├── db/                      # Database connection and utility functions
│   ├── logs/                    # Log files
│   ├── main.py                  # Main orchestration script
|   ├── setup_data.py            # Loads initial data required for the pipeline to run.
│   └── transformation/          # ETL transformation scripts
│       ├── calculate_cdi_bonus.py
│       ├── generate_daily_rates.py
│       └── wallet_history.py
│
├── streamlit_app/               # Streamlit dashboard for observability
│
├── tests/                       # Unit and integration tests
│
├── .env                         # Environment variables
├── docker-compose.yml           # Docker Compose configuration
├── Dockerfile                   # Dockerfile configuration
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation
```
<br>

> ⚠️ **Attention:**  
> **Note:**  
> - Each module is separated by responsibility (data ingestion, transformation, database,  dashboard, etc.).
> - The `src/transformation/` folder contains the main ETL logic.
> - The `tests/` folder is for all test code.
> - The `streamlit_app/` folder contains the observability dashboard.

<br>

### Database Schema Diagram

```text
+-------------------+
|      users        |
+-------------------+
| user_id (PK)      |
+-------------------+

        |
        | 1
        |-----------------------------+
        |                             |
        v                             v

+-------------------------+      +-------------------------+
|    wallet_history       |      |  daily_bonus_payouts    |
+-------------------------+      +-------------------------+
| history_id (PK)         |      | payout_id (PK)          |
| user_id (FK) ---------- |----> | user_id (FK)            |
| timestamp               |      | payout_date             |
| balance                 |      | calculated_amount       |
+-------------------------+      +-------------------------+
| UNIQUE(user_id,         |      | UNIQUE(payout_date,     |
|        timestamp)       |      |        user_id)         |
+-------------------------+      +-------------------------+

+-----------------------------+
|   daily_interest_rates      |
+-----------------------------+
| rate_date (PK)              |
| daily_rate                  |
+-----------------------------+
```

**Legend:**
- PK = Primary Key
- FK = Foreign Key

**Database Schema**

The system uses four main tables:
- `users`: Basic user information
- `wallet_history`: Historical balance records
- `daily_interest_rates`: Daily CDI rates
- `daily_bonus_payouts`: Calculated bonus amounts

For detailed table structure and column descriptions, refer to `create_tables.sql`.

**Relationships:**
- `wallet_history.user_id` and `daily_bonus_payouts.user_id` reference `users.user_id`
- `daily_bonus_payouts.payout_date` references a date, which should exist in `daily_interest_rates.rate_date` for rate lookup



### Functional Requirements Met

- Ingests and processes CDC wallet data.
- Calculates daily CDI bonus payouts per user.
- Stores results in a relational database.
- Provides observability and log exploration via Streamlit.

### Non-Functional Requirements Met

- **Scalability:** Spark enables horizontal scaling.
- **Reliability:** Database constraints and indexes ensure data integrity.
- **Maintainability:** Modular codebase and clear separation of concerns.
- **Observability:** Streamlit dashboard and log explorer for monitoring and debugging.

## Trade-offs and Limitations

- **Time Constraints:** Some features (e.g., real-time streaming, advanced error handling, and full test coverage) were simplified or omitted.
- **Batch Processing:** The pipeline currently runs in batch mode; for production, a streaming approach may be preferable.
- **Configuration:** Some parameters are hardcoded or set via environment variables; a more robust configuration management could be implemented.
- **Testing:** Only core logic is unit tested; integration and end-to-end tests are limited.
- **Columnar Database:** Although columnar databases (such as Amazon Redshift, BigQuery, or ClickHouse) are optimized for analytical queries and large-scale aggregations, this project uses a normalized relational (row-based) schema in PostgreSQL. This choice prioritizes transactional integrity, data consistency, and integration with other operational systems, rather than analytical performance. While this approach is ideal for transactional workloads and data sharing with other services, it may not deliver the same query performance for complex analytics as a columnar data warehouse. For advanced analytical needs, data could be periodically exported or replicated to a dedicated columnar database.

<br>

## Important Notes and Recommendations

### Database Initialization and Docker Compose Behavior

- **Automatic Table Creation:**  
  When you start the services for the first time using `docker-compose up -d`, Docker Compose will automatically execute the SQL script to create all necessary tables in the PostgreSQL database.
- **One-Time Execution:**  
  This initialization script runs only on the first startup with a fresh (empty) database volume. If you stop and restart the containers without removing the volume (i.e., using `docker-compose down` followed by `docker-compose up`), the script will **not** run again. This is the desired behavior, as you do not want to recreate tables and lose data every time the service starts.
- **Resetting the Database:**  
  If you need to reset the database and recreate all tables from scratch, you must remove the volume with:
  ```sh
  docker-compose down -v
  docker-compose up -d
  ```
  This will delete all data and re-run the table creation script.

### Setup Instructions
*With more detail*

1. **Clone the repository:**
    ```sh
    git clone git@github.com:felipesimione/cdi-bonus-assignment.git
    cd cdi-bonus-assignment
    ```
2. **Configure environment variables:**  
   The environment file will be available on github, but ideally it should be hidden.
    ```sh
    .env
    ```
3. **Start the services:**  
   Use Docker Compose to build and start all services:
    ```sh
    docker-compose up -d
    ```
   This will automatically set up the database and run the pipeline.

4. **Resetting the database:**  
   If you need to reset the database, use:
    ```sh
    docker-compose down -v
    docker-compose up -d
    ```

---

### Data Precision: Why We Use Decimal Instead of Float

- **Financial Accuracy:**  
  Floating-point types (`float`, `double`) are not recommended for financial calculations due to precision errors, which can lead to inconsistent results.  
  In this project, all monetary values are handled as `decimal` (or `numeric`) types, both in the database and in calculations, to ensure accuracy.
- **Performance Tradeoff:**  
  Calculations with decimals are slower than with floats, but accuracy is critical for financial applications. This tradeoff is necessary to guarantee correct results.

---

### Project Structure and Orchestration

- **Orchestration in Main:**  
  The main orchestration logic currently resides in `main.py`. While this works, best practice is to separate orchestration into its own module or use a dedicated workflow tool such as Apache Airflow for better scalability and maintainability.
- **Project Structure:**  
  The current folder structure can be further improved to better reflect ETL (Extract, Transform, Load) best practices. Consider separating ingestion, transformation, and loading logic into distinct modules or packages for easier reuse and testing.
- **Reusable Functions:**  
  Refactoring the code to have reusable functions for ingestion, transformation, and loading (with parameterized variables) will make the pipeline more modular and maintainable.

---

### Testing Improvements

- **Test Coverage:**  
  While core logic is unit tested, test coverage can be improved by adding more tests, especially for edge cases and integration scenarios.
- **Spark Session Optimization:**  
  Creating a single reusable SparkSession for all tests (instead of one per test) can significantly reduce test execution time and resource usage, as Spark is expensive to initialize.

---

### Observability and Notifications

- **Observability:**  
  The project includes a Streamlit dashboard for monitoring and log exploration. For production, consider integrating with enterprise monitoring tools (e.g., Prometheus, Grafana).
- **Error Notifications:**  
  For enhanced operational awareness, you can implement a process to send error or breakage notifications to a company chat tool (such as Slack) for real-time alerts.

---

### Additional Recommendations

- **Configuration Management:**  
  Move hardcoded parameters to configuration files or environment variables for greater flexibility and security.
- **Security:**  
  + Store sensitive credentials securely (e.g., environment variables, secrets manager) and restrict database access.
  + The environment file will be available on github, but ideally it should be hidden.
- **API Layer:**  
  For broader integration, consider exposing bonus payout data via a REST API.
- **Schema Management:**  
  Use migration tools (like Alembic or Flyway) for managing database schema changes in production environments.

- **Data Validation with Pydantic (Future Recommendation):**
  For future improvements, consider adopting [Pydantic](https://docs.pydantic.dev/) for data validation and settings management. Pydantic provides robust, type-safe data models that help catch errors early, ensure data consistency, and improve code readability.

  **Why use Pydantic?**
  - **Type Safety:** Enforces type hints at runtime, reducing bugs from unexpected data types.
  - **Validation:** Automatically validates and parses input data, raising clear errors for invalid data.
  - **Settings Management:** Easily manage configuration via environment variables or `.env` files using `pydantic.BaseSettings`.
  - **Integration:** Works well with modern Python data pipelines and APIs.

  **How it applies to this project:**
  - **ETL Data Models:** Define schemas for CDC records, wallet history, and bonus payouts to validate data before processing with Spark or inserting into the database.
  - **Configuration:** Replace manual environment variable parsing with Pydantic settings classes for cleaner, safer config management.
  - **Testing:** Use Pydantic models in tests to generate valid and invalid data cases, improving test coverage and reliability.

  **Example (for CDC record validation):**
  ```python
  from pydantic import BaseModel, validator
  from datetime import datetime

  class CDCRecord(BaseModel):
      user_id: str
      timestamp: datetime
      amount_change: float

      @validator('amount_change')
      def amount_must_be_nonzero(cls, v):
          if v == 0:
              raise ValueError('amount_change must not be zero')
          return v
  ```

  **Recommendation:**  
  While the current codebase uses pandas and Spark DataFrames for flexibility, introducing Pydantic models as a validation layer before ingestion or transformation will make the pipeline more robust and maintainable as the project grows.

---

For further details on the database schema, business rules, and function documentation, see the relevant sections above and the `create_tables.sql` file.
<br><br><br>


# Function Documentation

This project calculates daily CDI (Certificado de Depósito Interbancário) bonus payments for eligible users based on their wallet balances. Below is a description of the main functions in the transformation layer.
<br><br>

## `wallet_history.py`

### `calculate_wallet_history(df_raw_cdc: DataFrame) -> DataFrame`
- **Purpose:**  
  Processes raw CDC (Change Data Capture) data to build a historical record of wallet balances for each user.
- **How it works:**  
  - Converts timestamp strings to proper datetime format, handling nulls.
  - Adds a unique row ID for tie-breaking.
  - Uses a window function to partition by user and order by timestamp, then calculates the running sum of `amount_change` to get the balance at each point.
- **Returns:**  
  A DataFrame with columns: `user_id`, `timestamp`, and `balance`.
<br>

## `generate_daily_rates.py`

### `fetch_cdi_daily_rates(start_date, end_date)`
- **Purpose:**  
  Fetches CDI rates from the Brazilian Central Bank (BCB) for a given period and converts annual rates to daily rates.
- **How it works:**  
  - Uses the BCB API (series 4389) to get annual CDI rates.
  - Converts annual rates to daily rates using the formula:  
    `(1 + annual_rate/100)^(1/365) - 1`
  - Handles conversion errors and logs warnings for missing data.
- **Returns:**  
  A dictionary mapping each date to its daily CDI rate.

### `insert_daily_rates_into_db()`
- **Purpose:**  
  Inserts the fetched daily CDI rates into the `daily_interest_rates` table in the database.
- **How it works:**  
  - Determines the date range based on wallet history.
  - Fetches CDI rates for the period.
  - Inserts or updates each day's rate in the database, skipping days with missing data.
  - Commits the transaction and logs the process.
<br>

## `calculate_cdi_bonus.py`

### `calculate_cdi_bonus_for_day(spark, calculation_date, jdbc_url, jdbc_properties)`
- **Purpose:**  
  Calculates and records the CDI bonus for all eligible users for a specific day.
- **How it works:**  
  - Looks up the CDI daily rate for the calculation date, searching back up to 90 days if needed.
  - Reads wallet balances as of the start of the calculation day.
  - Determines eligibility:  
    - User must have a balance > 100  
    - No wallet movements in the last 24 hours
  - Calculates the bonus as `balance * daily_rate`.
  - Deletes any existing bonus records for the day and inserts the new results into `daily_bonus_payouts`.

### `calculate_cdi_bonus_for_period(spark, start_date_override=None, end_date_override=None)`
- **Purpose:**  
  Orchestrates the CDI bonus calculation for a range of dates.
- **How it works:**  
  - Determines the calculation period either from arguments or from wallet history.
  - For each day in the period, calls `calculate_cdi_bonus_for_day`.
  - Handles errors and logs the process.
<br>

## Business Rules

1. **Eligibility Criteria**:
   - Minimum balance: > 100
   - Holding period: 24 hours without movements
   - Valid CDI rate available (within 90 days)

2. **Rate Application**:
   - Uses daily CDI rates
   - Falls back to most recent rate within 90 days if current date's rate is unavailable
   - Bonus = balance * daily_rate

3. **Data Processing**:
   - Processes data day by day
   - Maintains audit trail through detailed logging
   - Handles data conflicts and updates