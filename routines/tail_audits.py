import sys
import os
from typing import Set, Dict, List
import dataclasses

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from db_connection import get_db_connection
from result import Result

def fetch_all_ids(table_name: str, id_column: str) -> Set[str]:
    """Fetches all IDs from a table to form the 'Universe' set."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column} FROM {table_name}")
    rows = cursor.fetchall()
    ids = {str(row[0]) for row in rows if row[0] is not None}
    cursor.close()
    conn.close()
    return ids

def fetch_valid_references(source_table: str, target_table: str, source_fk: str, target_pk: str) -> Set[str]:
    """
    Fetches IDs from source that successfully JOIN to target.
    These are the 'Valid/Connected' items.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    query = f"""
        SELECT t1.{source_fk} 
        FROM {source_table} t1
        JOIN {target_table} t2 ON t1.{source_fk} = t2.{target_pk}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    valid_refs = {str(row[0]) for row in rows}
    cursor.close()
    conn.close()
    return valid_refs


def log_section(title):
    print(f"\n{'='*70}")
    print(f"🕵️  AUDITORIA: {title}")
    print(f"{'='*70}")

def log_comparison(source_table, source_count, target_table, orphan_count):
    print(f"   ℹ️  Comparando: [{source_table}] --> [{target_table}]")
    print(f"   📊 Total em {source_table}: {source_count}")
    print(f"   ❌ Órfãos detectados: {orphan_count}")

def job_floating_payments():
    log_section("Pagamentos Flutuantes (Floating Payment Hunter)")
    print("   🔎 Objetivo: Identificar pagamentos sem vínculo válido com empenhos.")
    
    all_pagamentos_ids = fetch_all_ids("pagamento", "id_pagamento")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check 1: Pagamento -> Empenho
    query = "SELECT id_pagamento FROM pagamento WHERE id_empenho IS NULL"
    cursor.execute(query)
    null_empenho = {str(row[0]) for row in cursor.fetchall()}
    
    query_join = "SELECT p.id_pagamento FROM pagamento p JOIN empenho e ON p.id_empenho = e.id_empenho"
    cursor.execute(query_join)
    valid_join = {str(row[0]) for row in cursor.fetchall()}
    
    orphans = all_pagamentos_ids - valid_join
    ghost_fk = orphans - null_empenho
    
    log_comparison("Pagamento", len(all_pagamentos_ids), "Empenho", len(orphans))
    
    if null_empenho:
        print(f"      ↳ ⚠️  {len(null_empenho)} pagamentos com ID_EMPENHO = NULL")
    if ghost_fk:
        print(f"      ↳ ⚠️  {len(ghost_fk)} pagamentos apontando para Empenhos inexistentes!")
    
    cursor.close()
    conn.close()

def job_zombie_liquidations():
    log_section("Liquidações Zumbi (Zombie Liquidation Hunter)")
    print("   🔎 Objetivo: Identificar liquidações de empenhos cujos contratos não existem.")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM liquidacao_nota_fiscal")
    total_liq = cursor.fetchone()[0]
    
    query = """
        SELECT l.id_liquidacao_empenhonotafiscal, e.id_empenho 
        FROM liquidacao_nota_fiscal l
        JOIN empenho e ON l.id_empenho = e.id_empenho 
        LEFT JOIN contrato c ON e.id_contrato = c.id_contrato
        WHERE c.id_contrato IS NULL
    """
    cursor.execute(query)
    zombies = cursor.fetchall()
    
    log_comparison("Liquidação", total_liq, "Contrato (via Empenho)", len(zombies))
    
    if zombies:
        print(f"   ⚠️  ALERTA: {len(zombies)} liquidações ligadas a contratos fantasmas.")
    
    cursor.close()
    conn.close()

def job_orphaned_referencias_cruzadas():
    log_section("Referências Cruzadas (Cross-Reference Orphans)")
    print("   🔎 Objetivo: Validar integridade de tabelas associativas (NFe, Fornecedores).")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check 1: NFe_Pagamento -> Pagamento
    print("\n   [1] Verificando NFe_Pagamento -> Pagamento")
    # ANOMALY FOUND: IDs differ (NP-x vs PGT-x). No direct FK.
    print("   ⚠️  IMPOSSÍVEL VALIDAR VINCULO DIRETO (ID MISMATCH):")
    print("      • Tabela 'pagamento' usa IDs do tipo 'PGT-x'")
    print("      • Tabela 'nfe_pagamento' usa IDs do tipo 'NP-x'")
    print("      • Não existe chave estrangeira explícita unindo as tabelas.")
    print("      ↳ Conclusão: Impossível rastrear se o pagamento da NFe corresponde ao pagamento Bancário via ID.")


    # Check 2: Contrato -> Entidade
    print("\n   [2] Verificando Contrato -> Entidade")
    cursor.execute("SELECT COUNT(*) FROM contrato")
    total_contratos = cursor.fetchone()[0]
    
    query_entidade = """
        SELECT c.id_contrato 
        FROM contrato c 
        LEFT JOIN entidade e ON c.id_entidade = e.id_entidade 
        WHERE e.id_entidade IS NULL
    """
    cursor.execute(query_entidade)
    orphans_entidade = cursor.fetchall()
    log_comparison("Contrato", total_contratos, "Entidade", len(orphans_entidade))

    # Check 3: Contrato -> Fornecedor
    print("\n   [3] Verificando Contrato -> Fornecedor")
    query_forn = """
        SELECT c.id_contrato 
        FROM contrato c 
        LEFT JOIN fornecedor f ON c.id_fornecedor = f.id_fornecedor 
        WHERE f.id_fornecedor IS NULL
    """
    cursor.execute(query_forn)
    orphans_forn = cursor.fetchall()
    log_comparison("Contrato", total_contratos, "Fornecedor", len(orphans_forn))
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    print("🚀 Iniciando Auditoria Expandida (Tail Jobs V2)...")
    try:
        job_floating_payments()
        job_zombie_liquidations()
        job_orphaned_referencias_cruzadas()
        print("\n✅ Auditoria Completa Finalizada.")
    except Exception as e:
        print(f"\n💥 Erro: {e}")

