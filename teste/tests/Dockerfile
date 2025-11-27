FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script and data for testing
COPY ../dags/spark_script.py .
COPY data/ ./data/

# Set the entry point for the container
ENTRYPOINT ["python", "spark_script.py"]