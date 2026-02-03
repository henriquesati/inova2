import sys
import os
from db_connection import get_db_connection

def run_raw_analysis():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print("--- RAW SQL ANALYSIS ---")
        
        # 1. Check Total Counts
        cursor.execute("SELECT COUNT(*) FROM contrato")
        total_contracts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM empenho")
        total_empenhos = cursor.fetchone()[0]
        
        print(f"Total Contracts: {total_contracts}")
        print(f"Total Empenhos: {total_empenhos}")
        
        # 2. Check for Multi-Empenho Contracts
        query = """
        SELECT id_contrato, COUNT(*) as qtd
        FROM empenho
        GROUP BY id_contrato
        HAVING COUNT(*) > 1
        ORDER BY qtd DESC;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n--- CONTRACTS WITH > 1 EMPENHO ---")
        if not results:
            print("✅ NULL SET (No contracts found with more than 1 empenho)")
        else:
            print(f"⚠️ FOUND {len(results)} CONTRACTS:")
            for row in results:
                print(f"  - Contrato {row[0]}: {row[1]} empenhos")
                
        # 3. Check for Orphaned Empenhos (Optional but good sanity check)
        cursor.execute("SELECT COUNT(*) FROM empenho WHERE id_contrato IS NULL")
        orphans = cursor.fetchone()[0]
        if orphans > 0:
             print(f"\n⚠️ FOUND {orphans} ORPHANED EMPENHOS (id_contrato IS NULL)")

    except Exception as e:
        print(f"SQL Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run_raw_analysis()
