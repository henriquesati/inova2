
import sys
import os
import argparse
from typing import List

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from db_connection import get_db_connection
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.transaction.transaction_pagamento import PaymentTransaction
from clientside.domains.pagamento import Valida
from utils.etl_common import batch_load_contratos, batch_load_related_data
from views.etl_empenhos import print_structure

def run_pipeline(batch_size: int = 100):
    print(f"🚀 Starting Pagamento ETL Pipeline - Batch Size: {batch_size}...\n")

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
        (entidades, fornecedores, empenhos, liquidacoes, nfes, pagamentos) = batch_load_related_data(cursor, contratos)

        print(f"--- [T]RANSFORM & [V]IEW Batch {batch_num} ---")
        
        # Batch Transform Empenho
        emp_results = EmpenhoTransaction.build_from_batch(
            contratos, entidades, fornecedores, empenhos
        )

        for i, (contrato, emp_res) in enumerate(zip(contratos, emp_results), 1):
            global_idx = offset + i
            print(f"\n🔄 Processing Item #{global_idx} (Contract ID: {contrato.id_contrato})...")

            if emp_res.is_err:
                print(f"   ⚠️ Empenho Tx Failed: {emp_res.error}")
                continue
                
            # Transform Liquidacao
            liq_tx_res = LiquidacaoTransaction.build_from_batch(emp_res.value, liquidacoes, nfes)
            if liq_tx_res.is_err:
                 print(f"   ⚠️ Liquidacao Tx Failed: {liq_tx_res.error}")
                 continue
            
            # Transform Payment
            payment_tx_res = PaymentTransaction.build_from_batch(liq_tx_res.value, pagamentos)
            if payment_tx_res.is_err:
                print(f"   ⚠️ Payment Tx Failed: {payment_tx_res.error}")
                continue

            tx = payment_tx_res.value

            # Validate Domain Rules
            validation_res = Valida(tx)
            if validation_res.is_err:
                 print(f"   🛑 DOMAIN ERROR (ANOMALY DETECTED): {validation_res.error}")
            else:
                 print("   ✅ Business Rules Passed.")
            
            # Count payments
            total_pags = sum(
                len(items) for items in tx.pagamentos_por_empenho.values()
            )
            
            print(f"\n   🔍 Structure Dump #{global_idx} (Pagamentos Found: {total_pags}):")
            print_structure(tx, indent=3) 
            
            print(f"   [V] Processed #{global_idx}")

        offset += batch_size

    cursor.close()
    conn.close()
    print("\n🏁 Pipeline Completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", "-b", type=int, default=100)
    args = parser.parse_args()
    run_pipeline(args.batch)
