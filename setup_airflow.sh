#!/bin/bash

# Set Airflow Home Directory
export AIRFLOW_HOME=~/airflow

# Initialize Airflow Database
echo "Initializing Airflow DB..."
airflow db init

# Create an Admin User (Change username & password if needed)
echo "Creating Airflow Admin User..."
airflow users create \
    --username admin \
    --password admin \
    --role Admin \
    --email admin@example.com \
    --firstname Admin \
    --lastname User

# Start Airflow Webserver
echo "Starting Airflow Webserver on port 8080..."
airflow webserver --port 8080 &

# Start Airflow Scheduler
echo "Starting Airflow Scheduler..."
airflow scheduler &

echo "Airflow Setup Complete! Access it at: http://localhost:8080/"
