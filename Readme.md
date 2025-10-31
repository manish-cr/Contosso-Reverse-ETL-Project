## Project Overview
This project loads Contosso datasets from Microsoft into PostgreSQL. While the original data was designed for SQL Server, this implementation focuses on FactSales and its related dimension tables due to the large number of database constraints. The full table schema reference is available [here](http://www.powerpivot-info.com/postgresql/ContosoDW.pdf).

## Project Structure

### tables.py
- Defines table structures as pandas DataFrames
- Contains schema definitions for all tables

### Utils.py
Provides three main functions:
- `create_tables()` - Creates database tables
- `add_constraints()` - Adds primary and foreign key constraints  
- `insert_data()` - Handles data insertion

### script.py
Main execution file that runs the reverse ETL process in this order:
1. Table creation
2. Constraint addition
3. Data insertion

## Performance Note
Parallel processing was implemented for data insertion to optimize performance during SQL table imports.

## Real-World Applications
This approach can be extended to live data feed scenarios where:
- Kafka or Flink handle data streaming
- Airflow orchestrates the data pipeline
- PostgreSQL serves as the analytical database for further processing

The project demonstrates a complete reverse ETL workflow that can be adapted for production data pipelines.