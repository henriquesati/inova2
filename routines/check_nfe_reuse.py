import sys
import os
from collections import defaultdict

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from db_connection import get_db_connection

def analyze_nfe_reuse():
    print("🕵️  Iniciando Análise Recursiva de NFe (Cross-Contract Check)...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Agrupar Liquidações por Chave DANFE
    print("   📊 Buscando NFe's usadas em múltiplas liquidações...")
    
    # Query: Lista todas as Chaves DANFE e os Contratos distintos associados a elas
    query = """
        SELECT 
            l.chave_danfe,
            c.id_contrato,
            e.id_empenho,
            l.id_liquidacao_empenhonotafiscal,
            f.documento AS cnpj_fornecedor_contrato
        FROM liquidacao_nota_fiscal l
        JOIN empenho e ON l.id_empenho = e.id_empenho
        JOIN contrato c ON e.id_contrato = c.id_contrato
        JOIN fornecedor f ON c.id_fornecedor = f.id_fornecedor
        WHERE l.chave_danfe IS NOT NULL
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # 2. Processamento em Memória (Simulando a Lógica de Domínio)
    # Map: ChaveNFe -> Set of Contratos
    nfe_usage_map = defaultdict(list)
    
    for row in rows:
        chave_nfe = row[0]
        usage_info = {
            "contrato": row[1],
            "empenho": row[2],
            "liq_id": row[3],
            "cnpj_fornecedor": row[4]
        }
        nfe_usage_map[chave_nfe].append(usage_info)
        
    # 3. Análise de "Duplicidade em Contratos Distintos"
    anomalies_found = 0
    
    for chave, usos in nfe_usage_map.items():
        # Extrair contratos únicos
        contratos_distintos = {u["contrato"] for u in usos}
        
        if len(contratos_distintos) > 1:
            print(f"\n   ⚠️  ALERTA: NFe Compartilhada entre Contratos Distintos!")
            print(f"       CHAVE: {chave}")
            print(f"       CONTRATOS ENVOLVIDOS: {contratos_distintos}")
            
            # Check de Consistência de CNPJ (Sua sugestão: if cpfcnpj != cnpj emitente)
            # Aqui verificamos se os contratos ao menos são do mesmo fornecedor
            cnpjs_distintos = {u["cnpj_fornecedor"] for u in usos}
            if len(cnpjs_distintos) > 1:
                print(f"       🚨 CRÍTICO: NFe usada por Fornecedores DIFERENTES! {cnpjs_distintos}")
            
            anomalies_found += 1
            
    if anomalies_found == 0:
        print("\n   ✅ Nenhuma NFe reutilizada entre contratos diferentes foi encontrada.")
    else:
        print(f"\n   ❌ Encontrados {anomalies_found} casos de reúso de NFe.")

    conn.close()

if __name__ == "__main__":
    analyze_nfe_reuse()
