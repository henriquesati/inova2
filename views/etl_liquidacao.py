
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
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.domains.liquidação import Valida
from utils.etl_common import batch_load_contratos, batch_load_related_data
from views.etl_empenhos import print_structure

def run_pipeline(batch_size: int = 100):
    print(f"🚀 Starting Liquidacao ETL Pipeline - Batch Size: {batch_size}...\n")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM contrato")
    total_contratos = cursor.fetchone()[0]
    print(f"Total contratos: {total_contratos}")

    offset = 0
    batch_num = 0

    while True:
        batch_num += 1
        print(f"\n--- [E]XTRACT Batch {batch_num} ---")
        contratos = batch_load_contratos(cursor, offset, batch_size)
        if not contratos:
            break
        
        print(f"📦 Extracted {len(contratos)} contracts.")

        # Load Related Data
        (entidades, fornecedores, empenhos, liquidacoes, nfes, _) = batch_load_related_data(cursor, contratos)

        print(f"--- [T]RANSFORM & [L]OAD Batch {batch_num} ---")
        
        # Batch Transform Empenho
        emp_results = EmpenhoTransaction.build_from_batch(
            contratos, entidades, fornecedores, empenhos
        )

        for i, (contrato, emp_res) in enumerate(zip(contratos, emp_results), 1):
             global_idx = offset + i
             print(f"\n🔄 Processing Item #{global_idx} (Contract ID: {contrato.id_contrato})...")

             if emp_res.is_err:
                 print(f"   ⚠️ [T] Empenho Transaction Failed: {emp_res.error}")
                 continue
             
             # Transform Liquidacao
             liq_tx_res = LiquidacaoTransaction.build_from_batch(emp_res.value, liquidacoes, nfes)
             
             if liq_tx_res.is_err:
                 print(f"   ⚠️ [T] Liquidacao Transaction Failed: {liq_tx_res.error}")
                 continue
            
             tx = liq_tx_res.value
             
             # Stats for structure dump
             total_liq = 0
             if tx.itens_liquidados:
                 if isinstance(tx.itens_liquidados, dict):
                    if tx.itens_liquidados and isinstance(next(iter(tx.itens_liquidados.values())), list):
                         total_liq = sum(len(inner) for inner in tx.itens_liquidados.values())
                    else:
                         total_liq = len(tx.itens_liquidados)
                 else:
                     total_liq = len(tx.itens_liquidados)

             total_emp = len(tx.empenho_transaction.empenhos)
             
             print(f"\n   🔍 Structure Dump #{global_idx} (Empenhos: {total_emp}, Liquidados: {total_liq}):")
             print_structure(tx, indent=3)
             
             # Validation
             val_res = Valida(tx)
             if val_res.is_ok:
                  print(f"   ✅ [L] Validated #{global_idx}")
             else:
                  print(f"   🚫 [L] Invalid #{global_idx}: {val_res.error}")
        
        offset += batch_size

    cursor.close()
    conn.close()
    print("\n🏁 Pipeline Completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", "-b", type=int, default=100)
    args = parser.parse_args()
    run_pipeline(args.batch)
