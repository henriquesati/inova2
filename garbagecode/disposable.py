import sys
from collections import defaultdict, deque
from db_connection import get_db_connection

def get_foreign_keys(cursor):
    """
    Retrieves a list of (table_name, foreign_table_name) tuples.
    This represents that 'table_name' depends on 'foreign_table_name'.
    """
    cursor.execute("""
        SELECT
            tc.table_name, 
            ccu.table_name AS foreign_table_name
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public';
    """)
    return cursor.fetchall()

def get_all_tables(cursor):
    """Fetches all table names from the public schema."""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
    """)
    return [row[0] for row in cursor.fetchall()]

def topological_sort(tables, dependencies):
    """
    Returns a list of tables in creation order (dependencies first).
    """
    # Build graph
    adj = defaultdict(set)
    in_degree = {t: 0 for t in tables}
    
    for dependent, dependency in dependencies:
        # If a table relates to itself, ignore for sorting purposes to avoid cycles
        if dependent == dependency:
            continue
        if dependency in tables and dependent in tables:
            if dependent not in adj[dependency]:
                adj[dependency].add(dependent)
                in_degree[dependent] += 1

    # Kahn's Algorithm
    queue = deque([t for t in tables if in_degree[t] == 0])
    sorted_tables = []
    
    while queue:
        u = queue.popleft()
        sorted_tables.append(u)
        
        for v in adj[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                queue.append(v)
                
    # Check for cycles (if len(sorted_tables) < len(tables))
    # Any remaining tables are part of a cycle or depend on a cycle
    remaining = [t for t in tables if t not in sorted_tables]
    if remaining:
        print(f"WARNING: Circular dependencies or isolated cycle detected involving: {remaining}")
        # Append remaining just so they appear in the list, though order is undefined
        sorted_tables.extend(remaining)
        
    return sorted_tables

def print_table_sample(cursor, table_name):
    print(f"\n{'='*60}")
    print(f"TABLE: {table_name}")
    print(f"{'='*60}")
    
    # Get columns
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    rows = cursor.fetchall()
    
    if not rows:
        print("(No data found)")
        return

    # Get column headers
    colnames = [desc[0] for desc in cursor.description]
    print(f"Columns: {', '.join(colnames)}")
    print("-" * 60)
    
    for row in rows:
        # Convert row to a simple string representation
        print(row)
    print("\n")

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("Analyzing database schema...")
        tables = get_all_tables(cursor)
        fks = get_foreign_keys(cursor)
        
        print(f"Found {len(tables)} tables and {len(fks)} foreign key relationships.")
        
        # Sort tables
        creation_order = topological_sort(tables, fks)
        
        print("\n" + "#" * 40)
        print(" SUGGESTED CREATION ORDER (Flow)")
        print("#" * 40)
        for i, table in enumerate(creation_order, 1):
            print(f"{i}. {table}")
            
        print("\n" + "#" * 40)
        print(" DATA SAMPLES (in creation order)")
        print("#" * 40)
        
        for table in creation_order:
            print_table_sample(cursor, table)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
