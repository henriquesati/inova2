import sys
import os
from typing import List

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from db_connection import get_db_connection
from result import Result
from models.contrato import Contrato
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.domains.liquidação import Valida

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
                print(f"⚠️ [E] Warning: Failed to parse contract row: {res.error}")
        
        return Result.ok(contracts)
        
    except Exception as e:
        return Result.err(f"Database error fetching contracts: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def print_structure(obj, indent=0):
    """
    Recursively prints the structure of an object.
    """
    spacing = " " * indent
    if obj is None:
        print(f"{spacing}None")
        return

    if isinstance(obj, list):
        if not obj:
            print(f"{spacing}[]")
        else:
            print(f"{spacing}[")
            for item in obj:
                print_structure(item, indent + 2)
            print(f"{spacing}]")
        return
        
    if hasattr(obj, "__dataclass_fields__") or hasattr(obj, "__dict__"):
        attrs = {}
        if hasattr(obj, "__dict__"):
            attrs = obj.__dict__
        elif hasattr(obj, "__dataclass_fields__"):
             from dataclasses import fields
             attrs = {f.name: getattr(obj, f.name) for f in fields(obj)}
             
        for key, value in attrs.items():
            if isinstance(value, list):
                 print(f"{spacing}  {key}: ({len(value)} items)")
                 print_structure(value, indent + 4)
            elif hasattr(value, "__dict__") or hasattr(value, "__dataclass_fields__"):
                print(f"{spacing}  {key}:")
                print_structure(value, indent + 4)
            else:
                print(f"{spacing}  {key}: {value}")
        print(f"{spacing}}}")
    else:
        print(f"{spacing}{obj}")

def run_pipeline():
    print("🚀 Starting Liquidacao ETL Pipeline...\n")

    # 1. EXTRACT
    print("--- [E]XTRACT Phase ---")
    contracts_res = E_fetch_contracts(limit=3)
    
    if contracts_res.is_err:
        print(f"❌ Extraction Failed: {contracts_res.error}")
        return

    contracts = contracts_res.value
    print(f"📦 Extracted {len(contracts)} contracts.\n")

    # 2. TRANSFORM & LOAD
    print("--- [T]RANSFORM & [L]OAD Phase ---")
    
    for i, contract in enumerate(contracts, 1):
        print(f"\n🔄 Processing Item #{i} (Contract ID: {contract.id_contrato})...")

        # Step 1: Build EmpenhoTransaction
        # Wrap contract in Result as expected by build_from_contract
        empenho_tx_res = EmpenhoTransaction.build_from_contract(Result.ok(contract))
        
        if empenho_tx_res.is_err:
            print(f"   ⚠️ [T] Empenho Transaction Failed: {empenho_tx_res.error}")
            continue
            
        # Step 2: Build LiquidacaoTransaction
        # Pass the Result[EmpenhoTransaction] directly as expected
        liquidacao_tx_res = LiquidacaoTransaction.build_from_empenho_transaction(empenho_tx_res)
        
        if liquidacao_tx_res.is_err:
             print(f"   ⚠️ [T] Liquidacao Transaction Failed: {liquidacao_tx_res.error}")
             continue
             
        tx = liquidacao_tx_res.value
        # Normalization step (Simulating user pipeline where checks would happen before this)
        # Count total items in nested dict
        total_liq = sum(len(inner) for inner in tx.itens_liquidados.values()) if isinstance(tx.itens_liquidados, dict) and tx.itens_liquidados and isinstance(next(iter(tx.itens_liquidados.values())), dict) else len(tx.itens_liquidados)
        total_emp = len(tx.empenho_transaction.empenhos)
        
        print(f"\n   🔍 Structure Dump #{i} (Empenhos: {total_emp}, Liquidados: {total_liq}):")
        print_structure(tx, indent=3)
        
        # Validation Phase
        val_res = Valida(tx)
        if val_res.is_ok:
             print(f"   ✅ [L] Validated #{i}")
        else:
             print(f"   🚫 [L] Invalid #{i}: {val_res.error}")

    print("\n🏁 Pipeline Completed.")

if __name__ == "__main__":
    run_pipeline()
