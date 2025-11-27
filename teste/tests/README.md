# Testing Setup for Spark Ingestion Script

This README provides instructions on how to set up and run tests for the Spark ingestion script located in the `dags/spark_script.py` file.

## Prerequisites

Before running the tests, ensure you have the following installed:

- Docker
- Docker Compose

## Directory Structure

The tests are organized as follows:

```
tests/
├── data/
│   └── sih/
│       └── dt=2023-01-01/
│           └── sample.csv  # Sample CSV file for testing
├── docker-compose.test.yml   # Docker Compose configuration for PostgreSQL
├── Dockerfile                 # Dockerfile for building the test environment
└── requirements.txt           # Python dependencies for the tests
```

## Running the Tests

1. **Build the Docker Image**: Navigate to the `tests` directory and build the Docker image using the following command:

   ```
   docker build -t spark-ingestion-test .
   ```

2. **Start the PostgreSQL Service**: Use Docker Compose to start the PostgreSQL service defined in `docker-compose.test.yml`:

   ```
   docker-compose -f docker-compose.test.yml up -d
   ```

3. **Run the Tests**: Execute the tests by running the following command:

   ```
   docker run --rm spark-ingestion-test
   ```

4. **Stop the PostgreSQL Service**: After the tests have completed, stop the PostgreSQL service:

   ```
   docker-compose -f docker-compose.test.yml down
   ```

## Notes

- Ensure that the `sample.csv` file in `tests/data/sih/dt=2023-01-01/` is structured according to the expectations of the `spark_script.py` for accurate testing.
- Modify the `requirements.txt` file as needed to include any additional dependencies required for your tests.