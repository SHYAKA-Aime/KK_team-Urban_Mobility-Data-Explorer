#!/usr/bin/env python3
"""
Database Initialization Script
Reads schema.sql and creates all necessary tables and views
"""

import subprocess
import mysql.connector
from config import DB_CONFIG
import sys

def read_sql_file(filename):
    """Read SQL file and return contents"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        sys.exit(1)

# def execute_sql_statements(cursor, sql_content):
#     """Execute multiple SQL statements from content"""
#     # Split by semicolon and filter empty statements
#     statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    
#     for i, statement in enumerate(statements, 1):
#         try:
#             # Skip comments and empty lines
#             if statement.startswith('--') or not statement:
#                 continue
            
#             print(f"Executing statement {i}/{len(statements)}...")
#             cursor.execute(statement)
#             print(f"  Success")
            
#         except mysql.connector.Error as err:
#             print(f"  Warning: {err}")
#             # Continue with other statements even if one fails



def main():
    """Main initialization function"""
    print("NYC TAXI DATABASE INITIALIZATION")
    print()
    
    # Connect to MySQL
    try:
        print("Connecting to MySQL server...")
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        print("Connected successfully")
        print()
        
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        print("\nPlease ensure:")
        print("  1. MySQL server is running")
        print("  2. Credentials in config.py are correct")
        sys.exit(1)
    
    # Create database if it doesn't exist
    try:
        print(f"Creating database '{DB_CONFIG['database']}'...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        print("Database created/verified")
        print()
        
    except mysql.connector.Error as err:
        print(f"Error creating database: {err}")
        sys.exit(1)
    
    # Use the database
    cursor.execute(f"USE {DB_CONFIG['database']}")
    
    # Read and execute schema.sql
    print("Loading schema from schema.sql...")
    sql_content = read_sql_file('schema.sql')
    print(f"Schema file loaded ({len(sql_content)} characters)")
    print()
    
    # Run schema.sql using MySQL CLI directly
    print("Loading schema from schema.sql...")
    try:
        subprocess.run(
            [
                "mysql",
                f"-u{DB_CONFIG['user']}",
                f"-p{DB_CONFIG['password']}",
                f"-h{DB_CONFIG['host']}",
                DB_CONFIG["database"],
                "-e",
                "source schema.sql;"
            ],
            check=True
        )
        print("\nSchema executed successfully via MySQL client\n")
    except subprocess.CalledProcessError as err:
        print(f"Error executing schema.sql: {err}")
        print("Check that the MySQL client is installed and in your PATH.")
        sys.exit(1)
    
    # Commit changess
    conn.commit()
    print()
    print("DATABASE INITIALIZATION COMPLETE")
    print()
    
    # Show created tables
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    print("Created tables and views:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cursor.fetchone()[0]
        print(f"  - {table[0]:<30} ({count} records)")
    
    print()
    print("Next steps:")
    print("  1. Run: python backend/data_processor.py")
    print("  2. Then: python backend/server.py")
    print()
    
    # Close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        sys.exit(1)