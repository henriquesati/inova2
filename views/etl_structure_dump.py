
import sys
import os
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
from utils.etl_common import batch_load_contratos, batch_load_related_data
from views.etl_empenhos import print_structure

def run_structure_dump():
    print("🚀 Starting Fullpipe Object Structure Dump...\n")
    print("   ℹ️  Dumping RAW In-Memory Object Structure (Classes, Dicts, Fields)")

    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. EXTRACT
    print("--- [E]XTRACT Phase (Sample 3) ---")
    # Load just 3 contracts for dumping
    contratos = batch_load_contratos(cursor, offset=0, batch_size=3)
    
    if not contratos:
        print("❌ No contracts found.")
        return

    print(f"📦 Extracted {len(contratos)} contracts.")

    # Load related data
    (entidades, fornecedores, empenhos, liquidacoes, nfes, pagamentos) = batch_load_related_data(cursor, contratos)

    # 2. TRANSFORM
    print("--- [T]RANSFORM & DUMP Phase ---")
    
    # Build batch
    emp_results = EmpenhoTransaction.build_from_batch(
            contratos, entidades, fornecedores, empenhos
    )

    for i, (contract, emp_res) in enumerate(zip(contratos, emp_results), 1):
        print(f"\n{'='*80}")
        print(f"🔄 OBJECT DUMP #{i} (Contract ID: {contract.id_contrato})")
        print(f"{'='*80}")

        if emp_res.is_err:
            print(f"   ⚠️ Empenho Tx Failed: {emp_res.error}")
            continue
            
        # Step 2: Liquidacao
        liq_tx_res = LiquidacaoTransaction.build_from_batch(emp_res.value, liquidacoes, nfes)
        if liq_tx_res.is_err:
             print(f"   ⚠️ Liquidacao Tx Failed: {liq_tx_res.error}")
             continue
        
        # Step 3: Pagamento
        payment_tx_res = PaymentTransaction.build_from_batch(liq_tx_res.value, pagamentos)
        if payment_tx_res.is_err:
            print(f"   ⚠️ Payment Tx Failed: {payment_tx_res.error}")
            continue

        tx = payment_tx_res.value
        
        # DUMP
        print_structure(tx, indent=0)

    cursor.close()
    conn.close()
    print("\n🏁 Structure Dump Completed.")

if __name__ == "__main__":
    run_structure_dump()
