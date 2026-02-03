import sys
import os
from typing import List

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.contrato import Contrato
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.transaction.transaction_pagamento import PaymentTransaction

from views.etl_liquidacao import print_structure, E_fetch_contracts

def run_pipeline():
    print("🚀 Starting Pagamento ETL Pipeline...\n")

    # 1. EXTRACT
    print("--- [E]XTRACT Phase ---")
    contracts_res = E_fetch_contracts(limit=10)
    
    if contracts_res.is_err:
        print(f"❌ Extraction Failed: {contracts_res.error}")
        return

    contracts = contracts_res.value
    print(f"📦 Extracted {len(contracts)} contracts.\n")

    # 2. TRANSFORM & VIEW
    print("--- [T]RANSFORM & [V]IEW Phase ---")
    
    for i, contract in enumerate(contracts, 1):
        print(f"\n🔄 Processing Item #{i} (Contract ID: {contract.id_contrato})...")

        # Step 1: Empenho
        empenho_tx_res = EmpenhoTransaction.build_from_contract(Result.ok(contract))
        if empenho_tx_res.is_err:
            print(f"   ⚠️ Empenho Tx Failed: {empenho_tx_res.error}")
            continue
            
        # Step 2: Liquidacao
        liquidacao_tx_res = LiquidacaoTransaction.build_from_empenho_transaction(empenho_tx_res)
        if liquidacao_tx_res.is_err:
             print(f"   ⚠️ Liquidacao Tx Failed: {liquidacao_tx_res.error}")
             continue
        
        # Step 3: Pagamento
        payment_tx_res = PaymentTransaction.build_from_liquidacao_transaction(liquidacao_tx_res)
        if payment_tx_res.is_err:
            print(f"   ⚠️ Payment Tx Failed: {payment_tx_res.error}")
            continue

        tx = payment_tx_res.value
        total_pags = sum(len(x.pagamentos) for x in tx.pagamentos_por_empenho.values())
        
        print(f"\n   🔍 Structure Dump #{i} (Pagamentos Found: {total_pags}):")
        print_structure(tx, indent=3)
        
        print(f"   ✅ [V] Processed #{i}")

    print("\n🏁 Pipeline Completed.")

if __name__ == "__main__":
    run_pipeline()
