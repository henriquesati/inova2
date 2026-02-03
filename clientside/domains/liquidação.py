from typing import List, Callable, Optional
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction as LiquidacaoContext, EmpenhoLiquidado
from result import Result

# se tiver tempo melhorar execução da iteração da lista O(N*r) pra O(n)
#è necessario uma regra que faça o aggregate do valor dos empenhos, só nao sei se faço nesse layer ou no layer de empenhos normal
# --- Item-Level Rules (Validate a single EmpenhoLiquidado) ---

def _rule_lfe_posterior_empenho(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida se a Liquidação (LFE) é posterior ou igual à Data de Emissão do Empenho."""
    if item.liquidacao.data_emissao < item.empenho.data_empenho:
        return f"Liquidação ({item.liquidacao.data_emissao}) anterior ao Empenho ({item.empenho.data_empenho}) - ID Emp: {item.empenho.id_empenho}"
    return None

def _rule_lfe_posterior_contrato(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida se a Liquidação (LFE) é posterior ou igual à Data do Contrato."""
    contrato = ctx.empenho_transaction.contrato
    if item.liquidacao.data_emissao < contrato.data:
        return f"Liquidação ({item.liquidacao.data_emissao}) anterior ao Contrato ({contrato.data})"
    return None

def _rule_valor_lfe_limite_empenho(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida se o Valor da Liquidação não excede o Valor do Empenho."""
    if item.liquidacao.valor > item.empenho.valor:
        return f"Valor da Liquidação ({item.liquidacao.valor}) excede Valor do Empenho ({item.empenho.valor}) - ID Emp: {item.empenho.id_empenho}"
    return None

def _rule_nfe_anterior_lfe(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida se NFe DataHora Emissão <= LFE Data Emissão (Liquidação)."""
    if item.nfe and item.nfe.data_hora_emissao:
        d_nfe = item.nfe.data_hora_emissao.date() if hasattr(item.nfe.data_hora_emissao, "date") else item.nfe.data_hora_emissao
        if d_nfe > item.liquidacao.data_emissao:
            return f"NFe Data ({d_nfe}) posterior à Liquidação ({item.liquidacao.data_emissao})"
    return None

def _rule_nfe_posterior_contrato(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida se NFe DataHora >= Data Contrato."""
    contrato = ctx.empenho_transaction.contrato
    if item.nfe and item.nfe.data_hora_emissao:
        d_nfe = item.nfe.data_hora_emissao.date() if hasattr(item.nfe.data_hora_emissao, "date") else item.nfe.data_hora_emissao
        if d_nfe < contrato.data:
            return f"NFe emitida ({d_nfe}) antes da data do contrato ({contrato.data})"
    return None

def _rule_nfe_cnpj_match_fornecedor(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida se NFe CNPJ Emitente == Fornecedor do Contrato Documento."""
    fornecedor = ctx.empenho_transaction.fornecedor
    if item.nfe:
        if item.nfe.cnpj_emitente != fornecedor.documento:
            return f"CNPJ Emitente NFe ({item.nfe.cnpj_emitente}) diverge do Fornecedor Contrato ({fornecedor.documento})"
    return None

def _rule_empenho_credor_match_fornecedor(ctx: LiquidacaoContext, item: EmpenhoLiquidado) -> Optional[str]:
    """Valida consistência nominal (Empenho Credor vs Fornecedor Nome)."""
    fornecedor = ctx.empenho_transaction.fornecedor
    if item.empenho.credor != fornecedor.nome:
         return f"Empenho Credor ({item.empenho.credor}) diverge do Fornecedor Nome ({fornecedor.nome})"
    return None


ITEM_RULES: List[Callable[[LiquidacaoContext, EmpenhoLiquidado], Optional[str]]] = [
    _rule_lfe_posterior_empenho,
    _rule_lfe_posterior_contrato,
    _rule_valor_lfe_limite_empenho,
    _rule_nfe_anterior_lfe,
    _rule_nfe_posterior_contrato,
    _rule_nfe_cnpj_match_fornecedor,
    _rule_empenho_credor_match_fornecedor
]

def Valida(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """
    Roda validações otimizadas: itera 1 vez sobre os itens e aplica todas as regras de item.
    """
    # 1. Iterate over items once
    for item in ctx.empenhos_liquidados:
        for rule in ITEM_RULES:
            error = rule(ctx, item)
            if error:
                return Result.err(error)
                
    return Result.ok(ctx)
