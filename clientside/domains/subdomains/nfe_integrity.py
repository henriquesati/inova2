"""
Subdomínio: Integridade e Validações de NFe (Nota Fiscal Eletrônica)
Unifica regras de:
1. Integridade Física (Relação com Liquidação)
2. Consistência Interna (Valores e Pagamentos)
"""
from typing import List, Callable, Any, Dict, Set, Optional
from decimal import Decimal
from result import Result
import sys
import os

# --- Imports de Contexto (Transactions/Models)
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction as LiquidacaoContext, ItemLiquidacao
from models.nfe import Nfe

# --- Functional Integrity Helper ---

def check_unique_impl(items: List[Any], key_fn: Callable[[Any], Any], error_msg_fn: Callable[[List[Any]], str]) -> Result[None]:
    """Helper genérico para checagem de unicidade."""
    values = [key_fn(item) for item in items if key_fn(item) is not None]
    if len(values) != len(set(values)):
        from collections import Counter
        counts = Counter(values)
        dupes = [k for k, v in counts.items() if v > 1]
        return Result.err(error_msg_fn(dupes))
    return Result.ok(None)

# --- 1. Validações de Integridade (Liquidação <-> NFe) ---

def check_unique_liquidation_ids(items: List[ItemLiquidacao]) -> Result[List[ItemLiquidacao]]:
    """Verifica unicidade de IDs de Liquidação."""
    res = check_unique_impl(
        items,
        key_fn=lambda x: x.liquidacao.id_liquidacao_empenhonotafiscal,
        error_msg_fn=lambda dupes: f"Liquidações duplicadas detectadas: IDs {dupes}"
    )
    return res.map(lambda _: items)

def check_liquidacao_danfe_1to1(items: List[ItemLiquidacao]) -> Result[List[ItemLiquidacao]]:
    """
    Garante relação 1–1 entre ID Liquidação e Chave DANFE.
    Uma única Liquidação não pode apontar para múltiplas DANFEs distintas.
    """
    liq_to_danfe: Dict[str, Set[str]] = {}

    for item in items:
        liq_id = str(item.liquidacao.id_liquidacao_empenhonotafiscal)
        danfe = item.liquidacao.chave_danfe

        if not danfe:
            continue

        if liq_id not in liq_to_danfe:
            liq_to_danfe[liq_id] = set()

        liq_to_danfe[liq_id].add(danfe)

        if len(liq_to_danfe[liq_id]) > 1:
            return Result.err(
                f"Violação 1–1: Liquidação {liq_id} associada a múltiplas DANFEs {liq_to_danfe[liq_id]}"
            )

    return Result.ok(items)

def check_integrity_nfe_liquidacao(ctx: LiquidacaoContext) -> Result[None]:
    """
    [Pipeline] Verifica integridade estrutural da relação Liquidação-NFe.
    Substitui antigo: _check_duplicates
    """
    if not isinstance(ctx.itens_liquidados, list):
         return Result.ok(None)
    
    return (
        Result.ok(ctx.itens_liquidados)
        .bind(check_unique_liquidation_ids)
        .bind(check_liquidacao_danfe_1to1)
        # check_unique_danfe_keys REMOVIDO (Suporte a Liquidação Parcial)
        .map(lambda _: None)
    )

# --- 2. Validações de Consistência Interna (NFe Values) ---

def check_nfe_pagamento_consistency(nfe: Optional[Nfe]) -> Result[None]:
    """
    Valida consistência entre NFe e seus NfePagamentos (se existirem).
    sum(NfePagamento.valor_pagamento) == Nfe.valor_total_nfe
    """
    if not nfe:
        return Result.ok(None)
    
    from models.nfe_pagamento import NfePagamento
    
    nfe_pags_result = NfePagamento.get_by_FK_chave_nfe(nfe.chave_nfe)
    if nfe_pags_result.is_err:
        return Result.ok(None)
    
    nfe_pags = nfe_pags_result.value
    if not nfe_pags:
        return Result.ok(None)
    
    soma_nfe_pag = sum((np.valor_pagamento for np in nfe_pags), Decimal(0))
    
    # Importante: Usar comparador financeiro seguro se disponível, ou Decimal direto
    # Pela consistência com o resto do projeto, mantemos Decimal direto aqui ou
    # poderíamos importar financial_utils. Vamos manter simples como estava.
    if soma_nfe_pag != nfe.valor_total_nfe:
        return Result.err(
            f"[FRAUDE!] Soma NfePagamentos ({soma_nfe_pag}) ≠ NFe.valor_total ({nfe.valor_total_nfe}) "
            f"- NFe: {nfe.chave_nfe}"
        )
    
    return Result.ok(None)
