from typing import Dict, List
from collections import defaultdict
from models.contrato import Contrato
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from models.pagamento import Pagamento

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
