"""
ETL Full Pipeline - Batch Loading Optimizado COMPLETO
7 queries em vez de N*10+ queries (N = numero de contratos)
"""
import sys
import os
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


# ═══════════════════════════════════════════════════════════════════════════
# BATCH LOADERS - 7 queries para carregar TUDO
# ═══════════════════════════════════════════════════════════════════════════

def log_contrato_estrutura(contrato: Contrato, entidade: Entidade = None, 
                          fornecedor: Fornecedor = None, empenhos: List[Empenho] = None,
                          liquidacoes: Dict[str, List[LiquidacaoNotaFiscal]] = None,
                          nfes: Dict[str, Nfe] = None, pagamentos: Dict[str, List[Pagamento]] = None):
    """Loga a estrutura completa de um contrato no console."""
    print("\n" + "=" * 80)
    print(f"📋 CONTRATO #{contrato.id_contrato}")
    print("=" * 80)
    
    # Contrato
    print(f"\n  🔹 ID Contrato:     {contrato.id_contrato}")
    print(f"  🔹 Número:          {getattr(contrato, 'numero_contrato', 'N/A')}")
    print(f"  🔹 Valor:           R$ {getattr(contrato, 'valor_contrato', 0):,.2f}")
    print(f"  🔹 ID Entidade:     {contrato.id_entidade}")
    print(f"  🔹 ID Fornecedor:   {contrato.id_fornecedor}")
    
    # Entidade
    if entidade:
        print(f"\n  🏛️  ENTIDADE:")
        print(f"      Nome: {getattr(entidade, 'nome_entidade', 'N/A')}")
        print(f"      CNPJ: {getattr(entidade, 'cnpj', 'N/A')}")
    
    # Fornecedor
    if fornecedor:
        print(f"\n  🏢 FORNECEDOR:")
        print(f"      Nome: {getattr(fornecedor, 'nome_fornecedor', 'N/A')}")
        print(f"      CNPJ: {getattr(fornecedor, 'cpf_cnpj', 'N/A')}")
    
    # Empenhos
    if empenhos:
        print(f"\n  📝 EMPENHOS ({len(empenhos)}):")
        for emp in empenhos:
            print(f"      - ID: {emp.id_empenho}")

            print(f"        Valor: R$ {emp.valor:,.2f}")
            print(f"        Data: {emp.data_empenho}")
            
            # Liquidações do empenho
            liqs = liquidacoes.get(emp.id_empenho, []) if liquidacoes else []
            if liqs:
                print(f"        📄 Liquidações ({len(liqs)}):")
                for liq in liqs:
                    print(f"           - ID: {liq.id_liquidacao_empenhonotafiscal}")
                    print(f"             Valor: R$ {liq.valor:,.2f}")
                    print(f"             Data: {liq.data_emissao}")
                    if liq.chave_danfe and nfes:
                        nfe = nfes.get(liq.chave_danfe)
                        if nfe:
                            print(f"             NFe: {nfe.chave_nfe[:20]}...")
                            print(f"             NFe Valor: R$ {nfe.valor_total_nfe:,.2f}")
            
            # Pagamentos do empenho
            pags = pagamentos.get(emp.id_empenho, []) if pagamentos else []
            if pags:
                print(f"        💰 Pagamentos ({len(pags)}):")
                for pag in pags:
                    print(f"           - ID: {pag.id_pagamento}")
                    print(f"             Valor: R$ {pag.valor:,.2f}")
                    print(f"             Data: {pag.data_pagamento_emp}")
    else:
        print(f"\n  📝 EMPENHOS: Nenhum")
    
    print("\n")


def batch_load_contratos(cursor, offset: int, batch_size: int = 100) -> List[Contrato]:
    """Carrega um batch de contratos com offset."""
    cursor.execute(f"SELECT * FROM contrato ORDER BY id_contrato LIMIT {batch_size} OFFSET {offset}")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    contratos = []
    for row in rows:
        res = Contrato.create(dict(zip(cols, row)))
        if res.is_ok:
            contratos.append(res.value)
    return contratos


def batch_load_related_data(cursor, contratos: List[Contrato]):
    """
    Carrega dados relacionados para um batch de contratos.
    Retorna dicts indexados para O(1) lookup.
    """
    if not contratos:
        return {}, {}, {}, {}, {}, {}
    
    contract_ids = [c.id_contrato for c in contratos]
    entidade_ids = list(set(c.id_entidade for c in contratos))
    fornecedor_ids = list(set(c.id_fornecedor for c in contratos))
    
    # ENTIDADES
    cursor.execute(f"SELECT * FROM entidade WHERE id_entidade = ANY(%s)", (entidade_ids,))
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    entidades_map: Dict[int, Entidade] = {}
    for row in rows:
        res = Entidade.from_row(dict(zip(cols, row)))
        if res.is_ok:
            entidades_map[res.value.id_entidade] = res.value
    
    # FORNECEDORES
    cursor.execute(f"SELECT * FROM fornecedor WHERE id_fornecedor = ANY(%s)", (fornecedor_ids,))
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    fornecedores_map: Dict[int, Fornecedor] = {}
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
    
    # LIQUIDAÇÕES (por empenho)
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
    
    return (
        entidades_map,
        fornecedores_map,
        dict(empenhos_por_contrato),
        dict(liquidacoes_por_empenho),
        nfes_map,
        dict(pagamentos_por_empenho)
    )


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def run_full_pipeline(batch_size: int = 100):
    """Pipeline completo que processa TODOS os contratos em batches."""
    import time
    start = time.time()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Contar total de contratos
    cursor.execute("SELECT COUNT(*) FROM contrato")
    total_contratos = cursor.fetchone()[0]
    print(f"\n{'='*80}")
    print(f"🚀 FULLPIPE - Processando TODOS os {total_contratos} contratos")
    print(f"   Batch size: {batch_size}")
    print(f"{'='*80}\n")
    
    stats = {"emp_ok": 0, "emp_err": 0, "liq_ok": 0, "liq_err": 0, "pag_ok": 0, "pag_err": 0}
    errors = defaultdict(int)
    offset = 0
    total_processed = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        batch_start = time.time()
        
        # Carregar batch de contratos
        contratos = batch_load_contratos(cursor, offset, batch_size)
        if not contratos:
            break
        
        print(f"\n{'─'*80}")
        print(f"📦 BATCH {batch_num}: contratos {offset+1} a {offset+len(contratos)}")
        print(f"{'─'*80}")
        
        # Carregar dados relacionados
        (entidades, fornecedores, empenhos, 
         liquidacoes, nfes, pagamentos) = batch_load_related_data(cursor, contratos)
        
        # BUILD BATCH
        tx_results = EmpenhoTransaction.build_from_batch(
            contratos, entidades, fornecedores, empenhos
        )
        
        for i, (contrato, emp_result) in enumerate(zip(contratos, tx_results), 1):
            # Logar estrutura completa do contrato
            log_contrato_estrutura(
                contrato,
                entidades.get(contrato.id_entidade),
                fornecedores.get(contrato.id_fornecedor),
                empenhos.get(contrato.id_contrato, []),
                liquidacoes,
                nfes,
                pagamentos
            )
            
            e, l, p = ".", ".", "."
            err = ""
            
            if emp_result.is_err:
                e, err = "B", emp_result.error
            else:
                emp_v = ValidaEmpenho(emp_result.value)
                if emp_v.is_err:
                    e, err = "✗", emp_v.error
                else:
                    e = "✓"
                    # BATCH BUILD para Liquidação
                    liq = LiquidacaoTransaction.build_from_batch(
                        emp_v.value, liquidacoes, nfes
                    )
                    if liq.is_err:
                        l, err = "B", liq.error
                    else:
                        liq_v = ValidaLiquidacao(liq.value)
                        if liq_v.is_err:
                            l, err = "✗", liq_v.error
                        else:
                            l = "✓"
                            # BATCH BUILD para Pagamento
                            pag = PaymentTransaction.build_from_batch(
                                liq_v.value, pagamentos
                            )
                            if pag.is_err:
                                p, err = "B", pag.error
                            else:
                                pag_v = ValidaPagamento(pag.value)
                                if pag_v.is_err:
                                    p, err = "✗", pag_v.error
                                else:
                                    p = "✓"
            
            # Stats
            if e == "✓": stats["emp_ok"] += 1
            elif e != ".": stats["emp_err"] += 1
            if l == "✓": stats["liq_ok"] += 1
            elif l != ".": stats["liq_err"] += 1
            if p == "✓": stats["pag_ok"] += 1
            elif p != ".": stats["pag_err"] += 1
            
            if err:
                errors[err.split("(")[0].strip()[:40]] += 1
            
            total_idx = offset + i
            print(f"  ▶ [{total_idx:4d}/{total_contratos}] C{contrato.id_contrato:4d} | E:{e} L:{l} P:{p}")
        
        batch_time = time.time() - batch_start
        total_processed += len(contratos)
        print(f"\n  ✅ Batch {batch_num} concluído em {batch_time:.2f}s ({len(contratos)/batch_time:.1f} contratos/s)")
        print(f"     Progresso: {total_processed}/{total_contratos} ({100*total_processed/total_contratos:.1f}%)")
        
        offset += batch_size
    
    cursor.close()
    conn.close()
    
    # RESUMO FINAL
    total_time = time.time() - start
    print(f"\n{'='*80}")
    print(f"📊 RESUMO FINAL")
    print(f"{'='*80}")
    print(f"  Total contratos: {total_processed}")
    print(f"  Batches:         {batch_num}")
    print(f"\n  ✓ EMP:{stats['emp_ok']:4d}  LIQ:{stats['liq_ok']:4d}  PAG:{stats['pag_ok']:4d}")
    print(f"  ✗ EMP:{stats['emp_err']:4d}  LIQ:{stats['liq_err']:4d}  PAG:{stats['pag_err']:4d}")
    print(f"\n  ⏱️  Total: {total_time:.2f}s ({total_processed/total_time:.1f} contratos/s)")
    
    if errors:
        print(f"\n  🔴 TOP ERROS:")
        for err, count in sorted(errors.items(), key=lambda x: -x[1])[:5]:
            print(f"     [{count:4d}x] {err}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--batch", "-b", type=int, default=100, help="Tamanho do batch (default: 100)")
    args = p.parse_args()
    run_full_pipeline(batch_size=args.batch)

