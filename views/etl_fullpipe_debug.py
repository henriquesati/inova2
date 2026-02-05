"""
ETL Full Pipeline - Debug Mode com logs detalhados
Carrega via batch (7 queries), mas loga estrutura completa a cada 1 segundo
"""
import sys
import os
import time
from typing import Dict, List
from collections import defaultdict

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from db_connection import get_db_connection
from models.contrato import Contrato
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from models.pagamento import Pagamento
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.transaction.transaction_pagamento import PaymentTransaction
from clientside.domains.empenho import executar_empenho_rules as ValidaEmpenho
from clientside.domains.liquidação import Valida as ValidaLiquidacao
from clientside.domains.pagamento import Valida as ValidaPagamento


def batch_load_all(limit: int = None):
    """Carrega todos os dados em 7 queries."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    q = "SELECT * FROM contrato"
    if limit:
        q += f" LIMIT {limit}"
    cursor.execute(q)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    contratos = []
    for row in rows:
        res = Contrato.create(dict(zip(cols, row)))
        if res.is_ok:
            contratos.append(res.value)
    
    if not contratos:
        cursor.close()
        conn.close()
        return [], {}, {}, {}, {}, {}, {}
    
    contract_ids = [c.id_contrato for c in contratos]
    entidade_ids = list(set(c.id_entidade for c in contratos))
    fornecedor_ids = list(set(c.id_fornecedor for c in contratos))
    
    # ENTIDADES
    cursor.execute(f"SELECT * FROM entidade WHERE id_entidade = ANY(%s)", (entidade_ids,))
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    entidades_map = {}
    for row in rows:
        res = Entidade.from_row(dict(zip(cols, row)))
        if res.is_ok:
            entidades_map[res.value.id_entidade] = res.value
    
    # FORNECEDORES
    cursor.execute(f"SELECT * FROM fornecedor WHERE id_fornecedor = ANY(%s)", (fornecedor_ids,))
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    fornecedores_map = {}
    for row in rows:
        res = Fornecedor.from_row(dict(zip(cols, row)))
        if res.is_ok:
            fornecedores_map[res.value.id_fornecedor] = res.value
    
    # EMPENHOS
    cursor.execute(f"SELECT * FROM empenho WHERE id_contrato = ANY(%s)", (contract_ids,))
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    empenhos_por_contrato: Dict[int, List[Empenho]] = defaultdict(list)
    all_empenho_ids = []
    for row in rows:
        res = Empenho.from_row(dict(zip(cols, row)))
        if res.is_ok:
            emp = res.value
            empenhos_por_contrato[emp.id_contrato].append(emp)
            all_empenho_ids.append(emp.id_empenho)
    
    # LIQUIDAÇÕES
    liquidacoes_por_empenho: Dict[str, List[LiquidacaoNotaFiscal]] = defaultdict(list)
    all_chaves_danfe = []
    if all_empenho_ids:
        cursor.execute(f"SELECT * FROM liquidacao_nota_fiscal WHERE id_empenho = ANY(%s)", (all_empenho_ids,))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        for row in rows:
            res = LiquidacaoNotaFiscal.from_row(dict(zip(cols, row)))
            if res.is_ok:
                liq = res.value
                liquidacoes_por_empenho[liq.id_empenho].append(liq)
                if liq.chave_danfe:
                    all_chaves_danfe.append(liq.chave_danfe)
    
    # NFEs
    nfes_map: Dict[str, Nfe] = {}
    if all_chaves_danfe:
        cursor.execute(f"SELECT * FROM nfe WHERE chave_nfe = ANY(%s)", (all_chaves_danfe,))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        for row in rows:
            res = Nfe.from_row(dict(zip(cols, row)))
            if res.is_ok:
                nfe = res.value
                nfes_map[nfe.chave_nfe] = nfe
    
    # PAGAMENTOS
    pagamentos_por_empenho: Dict[str, List[Pagamento]] = defaultdict(list)
    if all_empenho_ids:
        cursor.execute(f"SELECT * FROM pagamento WHERE id_empenho = ANY(%s)", (all_empenho_ids,))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        for row in rows:
            res = Pagamento.from_row(dict(zip(cols, row)))
            if res.is_ok:
                pag = res.value
                pagamentos_por_empenho[pag.id_empenho].append(pag)
    
    cursor.close()
    conn.close()
    return (contratos, entidades_map, fornecedores_map, dict(empenhos_por_contrato),
            dict(liquidacoes_por_empenho), nfes_map, dict(pagamentos_por_empenho))


def log_stage(stage: str, detail: str = ""):
    print(f"\n{'='*60}")
    print(f"📌 STAGE: {stage}")
    if detail:
        print(f"   {detail}")
    print(f"{'='*60}\n")


def run_debug_pipeline(limit: int = None, interval: float = 1.0):
    """Pipeline com logs detalhados, imprime progresso a cada `interval` segundos."""
    print("🚀 DEBUG PIPELINE - Logs Detalhados (Batch Loading)")
    print("   Contrato → Empenho → Liquidação → Pagamento\n")
    
    print("⏳ Carregando dados em batch (7 queries)...")
    start = time.time()
    (contratos, entidades, fornecedores, empenhos,
     liquidacoes, nfes, pagamentos) = batch_load_all(limit)
    load_time = time.time() - start
    print(f"✅ Carregado em {load_time:.2f}s: {len(contratos)} contratos\n")
    
    tx_results = EmpenhoTransaction.build_from_batch(contratos, entidades, fornecedores, empenhos)
    
    stats = {
        "total": len(contratos),
        "empenho_ok": 0, "empenho_err": 0,
        "liquidacao_ok": 0, "liquidacao_err": 0,
        "pagamento_ok": 0, "pagamento_err": 0,
    }
    
    last_log = time.time()
    processed = []
    
    for i, (contrato, emp_tx_result) in enumerate(zip(contratos, tx_results), 1):
        now = time.time()
        if now - last_log >= interval:
            if processed:
                print(f"\n⏱️  [{now - start:.1f}s] Processados: {processed}")
                processed = []
            last_log = now
        
        processed.append(f"C{contrato.id_contrato}")
        
        print(f"\n{'─'*60}")
        print(f"🔄 Contrato #{i} (ID: {contrato.id_contrato})")
        print(f"   Valor: {contrato.valor} | Data: {contrato.data}")
        print(f"{'─'*60}")
        
        # ══════════════ EMPENHO ══════════════
        log_stage("EMPENHO", f"Contrato {contrato.id_contrato}")
        
        if emp_tx_result.is_err:
            print(f"   ❌ Build Error: {emp_tx_result.error}")
            stats["empenho_err"] += 1
            continue
        
        emp_tx = emp_tx_result.value
        print(f"   📊 Empenhos: {len(emp_tx.empenhos)}")
        for eid, emp in list(emp_tx.empenhos.items())[:3]:
            print(f"      • {eid}: R${emp.valor}")
        if len(emp_tx.empenhos) > 3:
            print(f"      ... e mais {len(emp_tx.empenhos) - 3}")
        
        emp_validated = ValidaEmpenho(emp_tx)
        
        if emp_validated.is_err:
            print(f"   ❌ Validation: {emp_validated.error}")
            stats["empenho_err"] += 1
            continue
        
        print(f"   ✅ Empenho OK")
        stats["empenho_ok"] += 1
        
        # ══════════════ LIQUIDAÇÃO ══════════════
        log_stage("LIQUIDAÇÃO", "Batch build")
        
        liq_tx_result = LiquidacaoTransaction.build_from_batch(
            emp_validated.value, liquidacoes, nfes
        )
        
        if liq_tx_result.is_err:
            print(f"   ❌ Build Error: {liq_tx_result.error}")
            stats["liquidacao_err"] += 1
            continue
        
        liq_tx = liq_tx_result.value
        total_itens = sum(len(v) for v in liq_tx.itens_liquidados.values())
        print(f"   📊 Itens liquidados: {total_itens}")
        
        liq_validated = ValidaLiquidacao(liq_tx)
        
        if liq_validated.is_err:
            print(f"   ❌ Validation: {liq_validated.error}")
            stats["liquidacao_err"] += 1
            continue
        
        print(f"   ✅ Liquidação OK")
        stats["liquidacao_ok"] += 1
        
        # ══════════════ PAGAMENTO ══════════════
        log_stage("PAGAMENTO", "Batch build")
        
        pag_tx_result = PaymentTransaction.build_from_batch(
            liq_validated.value, pagamentos
        )
        
        if pag_tx_result.is_err:
            print(f"   ❌ Build Error: {pag_tx_result.error}")
            stats["pagamento_err"] += 1
            continue
        
        pag_tx = pag_tx_result.value
        total_pags = sum(len(v) for v in pag_tx.pagamentos_por_empenho.values())
        print(f"   📊 Pagamentos: {total_pags}")
        
        pag_validated = ValidaPagamento(pag_tx)
        
        if pag_validated.is_err:
            print(f"   ❌ Validation: {pag_validated.error}")
            stats["pagamento_err"] += 1
            continue
        
        print(f"   ✅ Pagamento OK")
        stats["pagamento_ok"] += 1
        
        print(f"\n   🏆 CONTRATO {contrato.id_contrato} - PIPELINE COMPLETO!")
    
    if processed:
        print(f"\n⏱️  [{time.time() - start:.1f}s] Processados: {processed}")
    
    # ══════════════ RESUMO ══════════════
    total_time = time.time() - start
    print(f"\n{'='*60}")
    print("📊 RESUMO FINAL")
    print(f"{'='*60}")
    print(f"   Total Contratos: {stats['total']}")
    print(f"   ├─ Empenho:    ✅ {stats['empenho_ok']} / ❌ {stats['empenho_err']}")
    print(f"   ├─ Liquidação: ✅ {stats['liquidacao_ok']} / ❌ {stats['liquidacao_err']}")
    print(f"   └─ Pagamento:  ✅ {stats['pagamento_ok']} / ❌ {stats['pagamento_err']}")
    print(f"\n⏱️  Tempo total: {total_time:.2f}s ({len(contratos)/total_time:.1f} contratos/s)")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", "-l", type=int, default=10, help="Limite de contratos")
    p.add_argument("--interval", "-i", type=float, default=1.0, help="Intervalo de log")
    args = p.parse_args()
    run_debug_pipeline(limit=args.limit, interval=args.interval)

