
import sys
import os
from db_connection import get_db_connection

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    tables = ['liquidacao_nota_fiscal', 'pagamento', 'nfe_pagamento']
    
    print("Checking constraints for tables:", tables)
    
    for table in tables:
        print(f"\n--- {table} ---")
        cursor.execute("""
            SELECT tc.constraint_name, tc.constraint_type
            FROM information_schema.table_constraints tc
            WHERE tc.table_name = %s
        """, (table,))
        
        rows = cursor.fetchall()
        if not rows:
            print("No constraints found at all.")
        else:
            for r in rows:
                print(f"  {r[0]} ({r[1]})")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
