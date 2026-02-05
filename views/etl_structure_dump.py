import sys
import os
from typing import List

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.contrato import Contrato
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.transaction.transaction_pagamento import PaymentTransaction

# Reuse the print_structure utility from etl_liquidacao
from views.etl_liquidacao import print_structure, E_fetch_contracts

def run_structure_dump():
    print("🚀 Starting Fullpipe Object Structure Dump...\n")
    print("   ℹ️  Dumping RAW In-Memory Object Structure (Classes, Dicts, Fields)")

    # 1. EXTRACT
    print("--- [E]XTRACT Phase (Sample 3) ---")
    contracts_res = E_fetch_contracts(limit=3)
    
    if contracts_res.is_err:
        print(f"❌ Extraction Failed: {contracts_res.error}")
        return

    contracts = contracts_res.value

    # 2. TRANSFORM
    print("--- [T]RANSFORM & DUMP Phase ---")
    
    for i, contract in enumerate(contracts, 1):
        print(f"\n{'='*80}")
        print(f"🔄 OBJECT DUMP #{i} (Contract ID: {contract.id_contrato})")
        print(f"{'='*80}")

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
        
        # DUMP
        print_structure(tx, indent=0)

    print("\n🏁 Structure Dump Completed.")

if __name__ == "__main__":
    run_structure_dump()
