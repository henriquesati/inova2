import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from db_connection import get_db_connection

def dump_samples():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("--- PAGAMENTO (First 3) ---")
    cursor.execute("SELECT id_pagamento, id_empenho, valor FROM pagamento LIMIT 3")
    for r in cursor.fetchall():
        print(f"ID: {r[0]} | EMP: {r[1]} | VAL: {r[2]}")
        
    print("\n--- NFE_PAGAMENTO (First 3) ---")
    cursor.execute("SELECT id, chave_nfe, valor_pagamento FROM nfe_pagamento LIMIT 3")
    for r in cursor.fetchall():
        print(f"ID: {r[0]} | KEY: {r[1]} | VAL: {r[2]}")
        
    conn.close()

if __name__ == "__main__":
    dump_samples()
