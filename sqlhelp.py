import os
import sys
import argparse
import difflib
from db_connection import get_db_connection

def get_all_tables(cursor):
    """Fetches all table names from the public schema."""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    return [row[0] for row in cursor.fetchall()]

def find_best_match(query, tables):
    """Finds the best matching table name using fuzzy search."""
    # Exact match
    if query in tables:
        return query
    
    # Fuzzy match
    matches = difflib.get_close_matches(query, tables, n=1, cutoff=0.4)
    if matches:
        return matches[0]
    
    # Substring match (if fuzzy fails or is too weak)
    for table in tables:
        if query in table:
            return table
            
    return None

def print_table_structure(cursor, table_name):
    """Prints the structure of the specified table."""
    print(f"\nStructure for table: {table_name}")
    print("-" * 80)
    print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10} {'Default':<20}")
    print("-" * 80)
    
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    
    columns = cursor.fetchall()
    
    if not columns:
        print("No columns found (or table does not exist).")
        return

    for col in columns:
        col_name = col[0]
        data_type = col[1]
        nullable = col[2]
        default = str(col[3]) if col[3] is not None else "None"
        
        # Truncate long strings
        if len(default) > 20:
             default = default[:17] + "..."
             
        print(f"{col_name:<30} {data_type:<20} {nullable:<10} {default:<20}")
    print("-" * 80)

def main():
    parser = argparse.ArgumentParser(description="Query PostgreSQL table structure.")
    # We accept the first argument as the table name, treating leading dashes as part of the name if needed
    # But argparse handles flags like -nfe as optional arguments usually.
    # To support `python sqlhelp.py -nfe`, we can just look at sys.argv manually or Configure argparse.
    
    # Simple approach: Join all arguments to form the query, stripping any leading dashes
    if len(sys.argv) < 2:
        print("Usage: python3 sqlhelp.py <table_name>")
        sys.exit(1)
        
    raw_query = sys.argv[1]
    
    # Strip leading dashes like -nfe or --nfe
    query = raw_query.lstrip('-')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        tables = get_all_tables(cursor)
        match = find_best_match(query, tables)
        
        if match:
            if match != query:
                print(f"Did you mean '{match}'? Showing results for '{match}'...")
            print_table_structure(cursor, match)
        else:
            print(f"No table found matching '{query}'.")
            print("Available tables:", ", ".join(tables))
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
