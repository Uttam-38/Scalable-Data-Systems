#!/bin/bash
# Relational Database Ingestion and Query Pipeline

DB_NAME="postgres"
DB_USER="postgres"

echo "=== Starting relational database pipeline ==="

# Step 1: Drop existing tables (if any)
psql -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS authors, subreddits, submissions, comments CASCADE;"
psql -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS query1, query2, query3, query4, query5 CASCADE;"

# Step 2: Create base tables
psql -U "$DB_USER" -d "$DB_NAME" -f sql/create_tables.sql

# Step 3: Apply relationships and constraints
psql -U "$DB_USER" -d "$DB_NAME" -f sql/create_relations.sql

# Step 4: Load data (CSV files not included in repository)
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY authors FROM './authors.csv' CSV HEADER;"
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY subreddits FROM './subreddits.csv' CSV HEADER;"
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY submissions FROM './submissions.csv' CSV HEADER;"
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY comments FROM './comments.csv' CSV HEADER;"

# Step 5: Execute analytical queries
psql -U "$DB_USER" -d "$DB_NAME" -f sql/queries.sql

echo "=== Relational database pipeline completed successfully ==="
