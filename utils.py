"""
Utils Module
Contains utility functions for database operations
"""

import pandas as pd


def create_tables(table_list, tables_obj, cursor, connection):
    """
    Create database tables based on CSV dataframes
    
    Args:
        table_list (list): List of table names to create
        tables_obj (Tables): Tables object containing dataframes
        cursor: Database cursor
        connection: Database connection
    
    Returns:
        int: Number of tables successfully created
    """
    print("\n[3/5] Creating database tables...")
    tables_created = 0
    
    for item in table_list:
        table_name = item
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        
        # Get the dataframe for this table
        df = getattr(tables_obj, item)
        
        # Build SQL based on dataframe columns and types
        for col, dtype in df.dtypes.items():
            sql_type = "TEXT"  # default
            
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "NUMERIC"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP"
            
            create_table_sql += f"{col} {sql_type}, "
        
        create_table_sql = create_table_sql.rstrip(', ') + ");"
        
        try:
            cursor.execute(create_table_sql)
            connection.commit()
            tables_created += 1
        except Exception as e:
            print(f"✗ Error creating table {table_name}: {e}")
            connection.rollback()
    
    print(f"✓ Created {tables_created} tables")
    return tables_created


def add_constraints(cursor, connection):
    """
    Add primary and foreign key constraints to tables
    
    Args:
        cursor: Database cursor
        connection: Database connection
    
    Returns:
        tuple: (number of primary keys added, number of foreign keys added)
    """
    print("\n[4/5] Setting up primary and foreign key constraints...")
    
    pkey_list_commands = [
        "ALTER TABLE FactSales ADD CONSTRAINT PK_FactSales PRIMARY KEY (SalesKey);",
        "ALTER TABLE DimPromotion ADD CONSTRAINT PK_DimPromotion PRIMARY KEY (PromotionKey);",
        "ALTER TABLE DimProduct ADD CONSTRAINT PK_DimProduct PRIMARY KEY (ProductKey);",
        "ALTER TABLE DimDate ADD CONSTRAINT PK_DimDate PRIMARY KEY (DateKey);",
        "ALTER TABLE DimStore ADD CONSTRAINT PK_DimStore PRIMARY KEY (StoreKey);",
        "ALTER TABLE DimCurrency ADD CONSTRAINT PK_DimCurrency PRIMARY KEY (CurrencyKey);",
        "ALTER TABLE DimChannel ADD CONSTRAINT PK_DimChannel PRIMARY KEY (ChannelKey);"
    ]
    
    fkey_list_commands = [
        "ALTER TABLE FactSales ADD FOREIGN KEY (PromotionKey) REFERENCES DimPromotion (PromotionKey);",
        "ALTER TABLE FactSales ADD FOREIGN KEY (ProductKey) REFERENCES DimProduct (ProductKey);",
        "ALTER TABLE FactSales ADD FOREIGN KEY (DateKey) REFERENCES DimDate (DateKey);",
        "ALTER TABLE FactSales ADD FOREIGN KEY (StoreKey) REFERENCES DimStore (StoreKey);",
        "ALTER TABLE FactSales ADD FOREIGN KEY (CurrencyKey) REFERENCES DimCurrency (CurrencyKey);",
        "ALTER TABLE FactSales ADD FOREIGN KEY (ChannelKey) REFERENCES DimChannel (ChannelKey);"
    ]
    
    pkeys_added = 0
    for query in pkey_list_commands:
        try:
            cursor.execute(query)
            connection.commit()
            pkeys_added += 1
        except Exception as e:
            # Constraint might already exist, continue
            connection.rollback()
    
    print(f"✓ Added {pkeys_added} primary key constraints")
    
    fkeys_added = 0
    for query in fkey_list_commands:
        try:
            cursor.execute(query)
            connection.commit()
            fkeys_added += 1
        except Exception as e:
            # Constraint might already exist, continue
            connection.rollback()
    
    print(f"✓ Added {fkeys_added} foreign key constraints")
    
    return pkeys_added, fkeys_added


def insert_data(args):
    """
    Insert data into a table using SQLAlchemy engine
    
    Args:
        args (tuple): Tuple containing (table_name, tables_obj, engine)
    
    Returns:
        tuple: (table_name, rows_inserted, error)
    """
    item, tables_obj, engine = args
    table_name = item
    print(f"Inserting data into {table_name}...")
    
    try:
        # Get the dataframe for this table
        df = getattr(tables_obj, item)
        
        # Clean column names
        df_clean = df.rename(columns=lambda x: x.replace(':', '_').replace(' ', '_').replace('-', '_'))
        
        # Use pandas to_sql method
        rows_inserted = df_clean.to_sql(
            table_name, 
            engine, 
            if_exists='append',  # Append to existing table
            index=False,         # Don't include DataFrame index
            method='multi',      # Insert multiple rows at once
            chunksize=1000       # Insert in batches of 1000
        )
        
        print(f"✓ Successfully inserted {rows_inserted} rows into {table_name}")
        return (table_name, rows_inserted, None)
        
    except Exception as e:
        print(f"✗ Error inserting data into {table_name}: {e}")
        return (table_name, 0, str(e))


def process_results(results):
    """
    Process multiprocessing results and categorize successes/failures
    
    Args:
        results (list): List of tuples from insert_data function
    
    Returns:
        tuple: (successful_tables list, failed_tables list)
    """
    successful_tables = []
    failed_tables = []
    
    for table_name, rows_inserted, error in results:
        if error is None:
            successful_tables.append((table_name, rows_inserted))
        else:
            failed_tables.append((table_name, error))
    
    return successful_tables, failed_tables


def print_summary(successful_tables, failed_tables):
    """
    Print insertion summary
    
    Args:
        successful_tables (list): List of successfully loaded tables
        failed_tables (list): List of failed table loads
    """
    print("\n" + "="*60)
    print("INSERTION SUMMARY")
    print("="*60)
    print(f"Successfully loaded: {len(successful_tables)} tables")
    for table_name, row_count in successful_tables:
        print(f"  ✓ {table_name}: {row_count} rows")
    
    if failed_tables:
        print(f"\nFailed to load: {len(failed_tables)} tables")
        for table_name, error in failed_tables:
            print(f"  ✗ {table_name}: {error}")
