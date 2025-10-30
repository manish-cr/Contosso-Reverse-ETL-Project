#!/usr/bin/env python3
"""
Contosso Database Load Script
This script creates tables, sets up relationships, and loads data from CSV files
into a PostgreSQL database.
"""

import psycopg2
from pathlib import Path
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import multiprocessing
import json

# Import custom modules
from tables import Tables
from utils import create_tables, add_constraints, insert_data, process_results, print_summary


def main():
    """Main function to orchestrate the database load process"""
    
    # Load database configuration from config.json
    config_path = Path(__file__).parent / "config.json"
    
    try:
        with open(config_path, 'r') as config_file:
            DB_CONFIG = json.load(config_file)
        print(f"✓ Loaded configuration from {config_path}")
    except FileNotFoundError:
        print(f"✗ Configuration file not found: {config_path}")
        print("Please create a config.json file with database credentials")
        return
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in configuration file: {e}")
        return
    
    print("="*60)
    print("CONTOSSO DATABASE LOAD SCRIPT")
    print("="*60)
    
    # 1. Connect to PostgreSQL
    print("\n[1/5] Connecting to PostgreSQL database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✓ Successfully connected to database")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        return
    
    # 2. Load CSV files
    print("\n[2/5] Loading CSV files from archive folder...")
    folder_path = Path(os.getcwd() + "/archive")
    
    if not folder_path.exists():
        print(f"✗ Archive folder not found: {folder_path}")
        return
    
    t = Tables()
    t.create_dynamic_tables(folder_path)
    table_list = t.get_table_list()
    print(f"✓ Loaded {len(table_list)} CSV files")
    
    # 3. Create tables
    create_tables(table_list, t, cur, conn)
    
    # 4. Set up primary and foreign keys
    add_constraints(cur, conn)
    
    # 5. Insert data using multiprocessing
    print("\n[5/5] Inserting data into tables (using multiprocessing)...")
    
    # Create SQLAlchemy engine for bulk inserts
    password = quote_plus(DB_CONFIG['password'])
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    # Create list of arguments for each process
    args_list = [(item, t, engine) for item in table_list]
    
    # Use multiprocessing to insert data in parallel
    with multiprocessing.Pool(processes=2) as pool:
        results = pool.map(insert_data, args_list)
    
    # Process and print results
    successful_tables, failed_tables = process_results(results)
    print_summary(successful_tables, failed_tables)
    
    # Close database connection
    cur.close()
    conn.close()
    print("\n✓ Database connection closed")
    print("="*60)


if __name__ == "__main__":
    main()
