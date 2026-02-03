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

def fetch_all_data_fast():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Fetch Entities
        print("Fetching Entidades...")
        cursor.execute("SELECT * FROM entidade")
        entidades = {}
        cols = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            res = Entidade.from_row(d)
            if res.is_ok:
                entidades[res.value.id_entidade] = res.value

        # 2. Fetch Fornecedores
        print("Fetching Fornecedores...")
        cursor.execute("SELECT * FROM fornecedor")
        fornecedores = {}
        cols = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            res = Fornecedor.from_row(d)
            if res.is_ok:
                fornecedores[res.value.id_fornecedor] = res.value

        # 3. Fetch Contratos
        print("Fetching Contratos...")
        cursor.execute("SELECT * FROM contrato")
        contratos = []
        cols = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            res = Contrato.create(d)
            if res.is_ok:
                contratos.append(res.value)

        # 4. Fetch Empenhos
        print("Fetching Empenhos...")
        cursor.execute("SELECT * FROM empenho")
        empenhos_map: Dict[int, List[Empenho]] = {} # id_contrato -> List[Empenho]
        cols = [d[0] for d in cursor.description]
        
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
             # Fix mapping here too just in case
            if "cpfcnpjcredor" in d and "cpf_cnpj_credor" not in d:
                 d["cpf_cnpj_credor"] = d["cpfcnpjcredor"]

            res = Empenho.from_row(d)
            if res.is_ok:
                e = res.value
                if e.id_contrato not in empenhos_map:
                    empenhos_map[e.id_contrato] = []
                empenhos_map[e.id_contrato].append(e)

        return Result.ok({
            "contracts": contratos,
            "entidades": entidades,
            "fornecedores": fornecedores,
            "empenhos_map": empenhos_map
        })

    except Exception as e:
        return Result.err(f"DB Error: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def write_structure(f, obj, indent=0):
    spacing = " " * indent
    if obj is None:
        f.write(f"{spacing}None\n")
        return

    if isinstance(obj, list):
        if not obj:
            f.write(f"{spacing}[]\n")
        else:
            f.write(f"{spacing}[\n")
            for item in obj:
                write_structure(f, item, indent + 2)
            f.write(f"{spacing}]\n")
        return

    if hasattr(obj, "__dataclass_fields__") or hasattr(obj, "__dict__"):
        name = obj.__class__.__name__
        attrs = {}
        if hasattr(obj, "__dict__"):
            attrs = obj.__dict__
        elif hasattr(obj, "__dataclass_fields__"):
             from dataclasses import fields
             attrs = {field.name: getattr(obj, field.name) for field in fields(obj)}
             
        for key, value in attrs.items():
            if isinstance(value, list):
                 f.write(f"{spacing}  {key}: ({len(value)} items)\n")
                 write_structure(f, value, indent + 4)
            elif hasattr(value, "__dict__") or hasattr(value, "__dataclass_fields__"):
                f.write(f"{spacing}  {key}:\n")
                write_structure(f, value, indent + 4)
            else:
                f.write(f"{spacing}  {key}: {value}\n")
        f.write(f"{spacing}}}\n")
    else:
        f.write(f"{spacing}{obj}\n")

def main():
    print("🚀 Starting OPTIMIZED dump and analysis...")
    
    data_res = fetch_all_data_fast()
    if data_res.is_err:
        print(f"FAILED: {data_res.error}")
        sys.exit(1)
        
    data = data_res.value
    contracts = data["contracts"]
    entidades = data["entidades"]
    fornecedores = data["fornecedores"]
    empenhos_map = data["empenhos_map"]
    
    print(f"Data Loaded: {len(contracts)} contracts, {len(empenhos_map)} contracts with empenhos.")

    # Clear Report
    report_file = os.path.join(current_dir, "report.txt")
    with open(report_file, "w") as f:
        f.write(f"DATABASE DUMP & ANALYSIS\n")
        f.write(f"Total Contracts: {len(contracts)}\n")
        f.write("="*50 + "\n\n")

    multi_empenho_contracts = []

    # Process in Memory
    with open(report_file, "a") as f:
        for i, contract in enumerate(contracts, 1):
            
            # Rehydrate Manually for Speed
            entidade = entidades.get(contract.id_entidade)
            fornecedor = fornecedores.get(contract.id_fornecedor)
            empenhos = empenhos_map.get(contract.id_contrato, [])
            
            # Create Transaction Object
            tx = EmpenhoTransaction(
                contrato=contract,
                entidade=entidade,
                fornecedor=fornecedor,
                empenhos=empenhos
            )
            
            # Analysis
            if len(empenhos) > 1:
                multi_empenho_contracts.append({
                    "id_contrato": contract.id_contrato,
                    "count": len(empenhos)
                })

            # Dump
            f.write(f"Item #{i} (Contrato ID: {contract.id_contrato})\n")
            f.write(f"  Empenhos Count: {len(empenhos)}\n")
            f.write("  Structure Dump:\n")
            write_structure(f, tx, indent=4)
            f.write("\n" + "-" * 30 + "\n")

    print("\n--- ANALYSIS RESULT ---")
    if not multi_empenho_contracts:
        print("✅ NO contract has more than one empenho.")
    else:
        print(f"⚠️ Found {len(multi_empenho_contracts)} contracts with multiple empenhos:")
        for item in multi_empenho_contracts:
            print(f"  - Contrato {item['id_contrato']}: {item['count']} empenhos")

    print(f"\nDump saved to {report_file}")

if __name__ == "__main__":
    main()
