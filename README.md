# CDI Bonus Assignment

This project processes wallet change data (CDC) and calculates daily CDI bonus payouts for users, leveraging Apache Spark for data transformation and PostgreSQL for storage.

## Table of Contents

- [Project Overview](#project-overview)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [Production Recommendations](#production-recommendations)
- [Design Choices](#design-choices)
- [Trade-offs and Limitations](#trade-offs-and-limitations)

---

## Project Overview

This service ingests raw CDC wallet data, processes historical balances, applies daily CDI rates, and calculates daily bonus payouts for each user. The results are stored in a PostgreSQL database. The pipeline is orchestrated using Apache Spark for scalability and performance.

## Installation

1. **Clone the repository:**
    ```sh
    git clone felipesimione/cdi-bonus-assignment
    gh repo clone felipesimione/cdi-bonus-assignment # via GitHub CLI
    cd cdi-bonus-assignment
    ```

2. **Install dependencies:**
    - Ensure you have Docker and Docker Compose installed on your system.
    - Python dependencies are managed via `requirements.txt` and installed inside the container automatically.

3. **Start the services:**
    ```sh
    docker-compose up --build
    ```

    This will:
    - Build the Spark and application containers.
    - Start PostgreSQL and Spark services.
    - Run the main pipeline.
    - Run the tests.
    - Create the [app.log](http://_vscodecontentref_/2) file.

## Usage

- The raw CDC files in the [raw_cdc_files](http://_vscodecontentref_/0) directory (e.g., `wallet_cdc_raw.csv`) will be created automatically when you run dcoker-compose.
- The main entry point is [main.py](http://_vscodecontentref_/1), which orchestrates data ingestion, transformation, and loading.
- Logs are written to [app.log](http://_vscodecontentref_/2).

## Testing

- Unit tests are located in the [tests](http://_vscodecontentref_/3) directory.
- The docker-compose run the tests and you can check in the terminal, but if you would like to run tests locally you can run the command below after run docker-compose:
    ```sh
    ## requirements.txt will be run with docker-compose
    docker-compose up tests
    ```

## Production Recommendations

- **Transactional Database:** Use a managed PostgreSQL instance with high availability and automated backups.
- **Data Ingestion:** Use batch or streaming ingestion pipelines (e.g., Apache Kafka + Spark Structured Streaming) for real-time CDC data.
- **Schema Management:** Apply migrations using tools like Alembic or Flyway.
- **Monitoring:** Integrate with monitoring tools (e.g., Prometheus, Grafana) for observability.
- **Security:** Store credentials securely (e.g., environment variables, secrets manager).

## Design Choices

- **Spark for ETL:** Chosen for scalability and efficient processing of large CDC datasets.
- **PostgreSQL:** Reliable, ACID-compliant storage for transactional and analytical queries.
- **Modular Structure:** Code is organized into logical modules for data setup, transformation, and utility functions.
- **Schema Design:** Tables are normalized for data integrity, with indexes for efficient querying (see [create_tables.sql](http://_vscodecontentref_/4)).
- **Logging:** Centralized logging for traceability and debugging.
- **Logging:** 

### Functional Requirements Met

- Ingests and processes CDC wallet data.
- Calculates daily CDI bonus payouts per user.
- Stores results in a relational database.

### Non-Functional Requirements Met

- **Scalability:** Spark enables horizontal scaling.
- **Reliability:** Database constraints and indexes ensure data integrity.
- **Maintainability:** Modular codebase and clear separation of concerns.

## Trade-offs and Limitations

- **Time Constraints:** Some features (e.g., real-time streaming, advanced error handling, and full test coverage) were simplified or omitted.
- **Batch Processing:** The pipeline currently runs in batch mode; for production, a streaming approach may be preferable.
- **Configuration:** Some parameters are hardcoded or set via environment variables; a more robust configuration management could be implemented.
- **Testing:** Only core logic is unit tested; integration and end-to-end tests are limited.
