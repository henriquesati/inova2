
import sys
import os
from typing import List

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from db_connection import get_db_connection
from result import Result
from models.contrato import Contrato
from clientside.transaction.empenho_transaction import ContratoEmpenhado
from clientside.domains.empenho import executar_empenho_rules

def E_fetch_contracts(limit: int = 5) -> Result[List[Contrato]]:
    """
    [E]XTRACT: Fetches contracts from the database.
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM contrato LIMIT %s"
        cursor.execute(query, (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        contracts = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            res = Contrato.create(row_dict)
            if res.is_ok:
                contracts.append(res.value)
            else:
                print(f"⚠️ [E] Warning: Failed to parse contract row {row_dict.get('id_contrato')}: {res.error}")
        
        return Result.ok(contracts)
        
    except Exception as e:
        return Result.err(f"Database error fetching contracts: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def T_build_empenho_transaction(contract: Contrato) -> Result[ContratoEmpenhado]:
    """
    [T]RANSFORM: Builds the EmpenhoTransaction (ContratoEmpenhado) from a Contract.
    Uses the build_from_contract factory which hydrates internal entities.
    """
    # Wrap in Result because build_from_contract expects Result[Contrato]
    return ContratoEmpenhado.build_from_contract(Result.ok(contract))

def print_structure(obj, indent=0):
    """
    Recursively prints the structure of an object.
    """
    spacing = " " * indent
    
    # Handle None
    if obj is None:
        print(f"{spacing}None")
        return

    # Handle Dataclasses and Objects with __dict__
    if hasattr(obj, "__dataclass_fields__") or hasattr(obj, "__dict__"):
        name = obj.__class__.__name__
        
        # Get attributes (unifying dataclass and standard class access)
        attrs = {}
        if hasattr(obj, "__dataclass_fields__"):
             from dataclasses import asdict
             # We don't use asdict(obj) directly because it recurses too eagerly into dicts
             # We want to maintain object identity controls if possible, but loose recursion is fine.
             # Actually, let's just use __dict__ if available, which dataclasses usually have.
             pass
        
        if hasattr(obj, "__dict__"):
            attrs = obj.__dict__
        elif hasattr(obj, "__dataclass_fields__"):
             # Fallback if slots?
             from dataclasses import fields
             attrs = {f.name: getattr(obj, f.name) for f in fields(obj)}
             
        for key, value in attrs.items():
            if hasattr(value, "__dict__") or hasattr(value, "__dataclass_fields__"):
                print(f"{spacing}  {key}:")
                print_structure(value, indent + 4)
            else:
                print(f"{spacing}  {key}: {value}")
        print(f"{spacing}}}")
    else:
        # Primitives
        print(f"{spacing}{obj}")

def L_validate_and_log(idx: int, transaction: ContratoEmpenhado):
    """
    [L]OAD: Executes domain rules and logs the transaction state.
    In a real ETL, this might load the valid transaction into a warehouse or processed table.
    Here, 'Loading' is verifying and logging to valid/invalid outputs.
    """
    rules_res = executar_empenho_rules(transaction)
    
    ent_name = transaction.entidade.nome if transaction.entidade else "Unknown"
    forn_name = transaction.fornecedor.nome if transaction.fornecedor else "Unknown"
    
    print(f"\n   🔍 Structure Dump #{idx}:")
    print_structure(transaction, indent=3)

    if rules_res.is_ok:
        print(f"   ✅ [L] Validated #{idx}: Entidade='{ent_name}' | Fornecedor='{forn_name}'")
    else:
        print(f"   🚫 [L] Invalid #{idx}: {rules_res.error}")

def run_pipeline():
    print("🚀 Starting Functional ETL Pipeline (E -> T -> L)...\n")

    # 1. EXTRACT
    print("--- [E]XTRACT Phase ---")
    contracts_res = E_fetch_contracts(limit=10)
    
    if contracts_res.is_err:
        print(f"❌ Extraction Failed: {contracts_res.error}")
        return

    contracts = contracts_res.value
    print(f"📦 Extracted {len(contracts)} contracts.\n")

    # 2. TRANSFORM & LOAD
    print("--- [T]RANSFORM & [L]OAD Phase ---")
    
    for i, contract in enumerate(contracts, 1):
        print(f"\n🔄 Processing Item #{i} (Contract ID: {contract.id_contrato})...")

        # Transform
        empenho_res = T_build_empenho_transaction(contract)

        if empenho_res.is_err:
            print(f"   ⚠️ [T] Transformation Failed: {empenho_res.error}")
            continue
        
        # Load / Validate
        L_validate_and_log(i, empenho_res.value)

    print("\n🏁 Pipeline Completed.")

if __name__ == "__main__":
    run_pipeline()
