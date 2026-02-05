
import sys
import os
import argparse
from typing import List

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from db_connection import get_db_connection
from result import Result
from models.contrato import Contrato
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.domains.empenho import executar_empenho_rules
from utils.etl_common import batch_load_contratos, batch_load_related_data

def print_structure(obj, indent=0):
    """
    Recursively prints the structure of an object.
    """
    spacing = " " * indent
    
    # Handle None
    if obj is None:
        print(f"{spacing}None")
        return

    # Handle List
    if isinstance(obj, list):
        if not obj:
            print(f"{spacing}[]")
        else:
            print(f"{spacing}[")
            for item in obj:
                print_structure(item, indent + 2)
            print(f"{spacing}]")
        return

    # Handle Dataclasses and Objects with __dict__
    if hasattr(obj, "__dataclass_fields__") or hasattr(obj, "__dict__"):
        # Get attributes (unifying dataclass and standard class access)
        attrs = {}
        if hasattr(obj, "__dict__"):
            attrs = obj.__dict__
        elif hasattr(obj, "__dataclass_fields__"):
             from dataclasses import fields
             attrs = {f.name: getattr(obj, f.name) for f in fields(obj)}
             
        # Print attributes
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
        # Primitives
        print(f"{spacing}{obj}")

def L_validate_and_log(idx: int, transaction: EmpenhoTransaction):
    """
    [L]OAD: Executes domain rules and logs the transaction state.
    """
    rules_res = executar_empenho_rules(transaction)
    
    ent_name = transaction.entidade.nome if transaction.entidade else "Unknown"
    forn_name = transaction.fornecedor.nome if transaction.fornecedor else "Unknown"
    
    # Empenho summary
    total_empenhos = len(transaction.empenhos)
    
    print(f"\n   🔍 Structure Dump #{idx} (Empenhos: {total_empenhos}):")
    print_structure(transaction, indent=3)

    if rules_res.is_ok:
        print(f"   ✅ [L] Validated #{idx}: Entidade='{ent_name}' | Fornecedor='{forn_name}' | Empenhos Count={total_empenhos}")
    else:
        print(f"   🚫 [L] Invalid #{idx}: {rules_res.error}")

def run_pipeline(batch_size: int = 100):
    print(f"🚀 Starting Functional ETL Pipeline (E -> T -> L) - Batch Size: {batch_size}...\n")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Contar total
    cursor.execute("SELECT COUNT(*) FROM contrato")
    total_contratos = cursor.fetchone()[0]
    print(f"Total contratos: {total_contratos}")

    offset = 0
    total_processed = 0
    batch_num = 0

    while True:
        batch_num += 1
        # Extract
        print(f"\n--- [E]XTRACT Batch {batch_num} ---")
        contratos = batch_load_contratos(cursor, offset, batch_size)
        if not contratos:
            break
        
        print(f"📦 Extracted {len(contratos)} contracts.")

        # Load Related Data
        (entidades, fornecedores, empenhos, _, _, _) = batch_load_related_data(cursor, contratos)

        # Transform & Load
        print(f"--- [T]RANSFORM & [L]OAD Batch {batch_num} ---")
        
        # Batch Transform
        tx_results = EmpenhoTransaction.build_from_batch(
            contratos, entidades, fornecedores, empenhos
        )

        for i, (contrato, emp_result) in enumerate(zip(contratos, tx_results), 1):
             global_idx = offset + i
             print(f"\n🔄 Processing Item #{global_idx} (Contract ID: {contrato.id_contrato})...")

             if emp_result.is_err:
                 print(f"   ⚠️ [T] Transformation Failed: {emp_result.error}")
                 continue
             
             L_validate_and_log(global_idx, emp_result.value)

        total_processed += len(contratos)
        offset += batch_size
        
        # Optional: Ask user or just process all if it's a full run. 
        # The prompt implies processing ALL so we continue.

    cursor.close()
    conn.close()
    print("\n🏁 Pipeline Completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", "-b", type=int, default=100)
    args = parser.parse_args()
    run_pipeline(args.batch)
