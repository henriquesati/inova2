import sys
import os
from typing import List, Dict

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from db_connection import get_db_connection
from result import Result
from models.contrato import Contrato
from models.empenho import Empenho
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from clientside.transaction.empenho_transaction import EmpenhoTransaction

def analyze_names():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Fetch Fornecedores
        print("Fetching Fornecedores...")
        cursor.execute("SELECT * FROM fornecedor")
        fornecedores = {}
        cols = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            res = Fornecedor.from_row(d)
            if res.is_ok:
                fornecedores[res.value.id_fornecedor] = res.value

        # 2. Fetch Contratos
        print("Fetching Contratos...")
        cursor.execute("SELECT * FROM contrato")
        contratos = []
        cols = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            res = Contrato.create(d)
            if res.is_ok:
                contratos.append(res.value)

        # 3. Fetch Empenhos
        print("Fetching Empenhos...")
        cursor.execute("SELECT * FROM empenho")
        empenhos_map: Dict[int, List[Empenho]] = {} 
        cols = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            if "cpfcnpjcredor" in d and "cpf_cnpj_credor" not in d:
                 d["cpf_cnpj_credor"] = d["cpfcnpjcredor"]
            res = Empenho.from_row(d)
            if res.is_ok:
                e = res.value
                if e.id_contrato not in empenhos_map:
                    empenhos_map[e.id_contrato] = []
                empenhos_map[e.id_contrato].append(e)

        # 4. Analyze Differences
        print("\n--- ANALYZING NAMES ---")
        
        matches = 0
        mismatches = 0
        
        for contrato in contratos:
            fornecedor = fornecedores.get(contrato.id_fornecedor)
            if not fornecedor:
                continue
                
            empenhos = empenhos_map.get(contrato.id_contrato, [])
            for emp in empenhos:
                # Compare Names
                if emp.credor == fornecedor.nome:
                    matches += 1
                else:
                    mismatches += 1
                    print(f"MISMATCH found in Contrato {contrato.id_contrato}:")
                    print(f"  Fornecedor Nome: '{fornecedor.nome}'")
                    print(f"  Empenho Credor : '{emp.credor}'")
                    print("-" * 30)

        print("\n--- SUMMARY ---")
        print(f"Total Comparisons: {matches + mismatches}")
        print(f"Exact Matches: {matches}")
        print(f"Mismatches: {mismatches}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    analyze_names()
