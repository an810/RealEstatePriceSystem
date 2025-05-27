#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# Run the initialization script
echo "Initializing database..."
docker exec -i real_estate_db psql -U postgres -d real_estate < init.sql

echo "Database initialization completed!" 