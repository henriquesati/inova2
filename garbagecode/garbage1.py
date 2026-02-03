
import sys
import os
from typing import List, Dict, Any

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from db_connection import get_db_connection
from result import Result
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from clientside.transaction.empenho_transaction import ContratoEmpenhado as EmpenhoContext
from clientside.domains.empenho import executar_empenho_rules

def fetch_empenho_data_rows(limit: int = 5) -> Result[List[Dict[str, Any]]]:
    """
    Fetches raw data rows joining empenho, entidade, and fornecedor.
    Returns a Result wrapping the list of dictionaries.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            e.id_entidade, e.nome as entidade_nome, e.estado, e.municipio, e.cnpj,
            f.id_fornecedor, f.nome as fornecedor_nome, f.documento
        FROM empenho emp
        JOIN entidade e ON emp.id_entidade = e.id_entidade
        -- Assuming empenhos link to fornecedores via contrato or directly. 
        -- Based on schema, empenho has CPF/CNPJCredor, but let's join logic or mock specific relation for demo.
        -- Actually schema shows empenho has id_entidade but NOT logic direct to Fornecedor table easily without joining Contrato?
        -- Let's try simpler join or mock if schema is complex.
        -- Schema: empenho -> contrato -> fornecedor
        JOIN contrato c ON emp.id_contrato = c.id_contrato
        JOIN fornecedor f ON c.id_fornecedor = f.id_fornecedor
        LIMIT %s;
    """
    
    try:
        cursor.execute(query, (limit,))
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return Result.ok(results)
    except Exception as e:
        return Result.err(f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

def build_context(row: Dict[str, Any]) -> Result[EmpenhoContext]:
    """
    Maps a raw DB row to an EmpenhoContext domain object.
    """
    try:
        # Build Entidade
        entidade = Entidade(
            id_entidade=row['id_entidade'],
            nome=row['entidade_nome'],
            estado=row['estado'],
            municipio=row['municipio'],
            cnpj=row['cnpj']
        )
        
        # Build Fornecedor
        fornecedor = Fornecedor(
            id_fornecedor=row['id_fornecedor'],
            nome=row['fornecedor_nome'],
            documento=row['documento']
        )
        
        # Create Context
        ctx = EmpenhoContext(
            entidade=entidade,
            fornecedor=fornecedor
        )
        return Result.ok(ctx)
    except Exception as e:
        return Result.err(f"Mapping error: {e}")

def run_pipeline():
    print("🚀 Starting Functional Pipeline...")

    # 1. Fetch Data
    data_result = fetch_empenho_data_rows(5)
    
    if data_result.is_err:
        print(f"❌ Failed to fetch data: {data_result.error}")
        return

    rows = data_result.value
    print(f"📦 Fetched {len(rows)} rows.")

    for i, row in enumerate(rows, 1):
        print(f"\n🔄 Processing Record #{i}...")
        
        # 2. Build Context (Map)
        ctx_result = build_context(row)
        
        if ctx_result.is_err:
             print(f"   ⚠️ Context Build Error: {ctx_result.error}")
             continue
             
        ctx = ctx_result.value
        
        # 3. Rename/Execute Domain Rules
        rules_result = executar_empenho_rules(ctx)
        
        if rules_result.is_ok:
            print(f"   ✅ Validated! Entidade: {ctx.entidade.nome} | Fornecedor: {ctx.fornecedor.nome}")
        else:
            print(f"   🚫 Validation Failed: {rules_result.error}")

if __name__ == "__main__":
    run_pipeline()
