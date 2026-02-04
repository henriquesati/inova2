"""
Subdomínio: Validações de Integridade NFe-Liquidação
Responsável por verificar a integridade e unicidade das relações entre
Liquidações e Notas Fiscais Eletrônicas.
"""
from typing import List, Callable, Any, Dict, Set
from result import Result
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction as LiquidacaoContext, ItemLiquidacao


# --- Functional Integrity Checkers ---

def check_unique_impl(items: List[Any], key_fn: Callable[[Any], Any], error_msg_fn: Callable[[List[Any]], str]) -> Result[None]:
    """Helper genérico para checagem de unicidade."""
    values = [key_fn(item) for item in items if key_fn(item) is not None]
    if len(values) != len(set(values)):
        from collections import Counter
        counts = Counter(values)
        dupes = [k for k, v in counts.items() if v > 1]
        return Result.err(error_msg_fn(dupes))
    return Result.ok(None)

def check_unique_liquidation_ids(items: List[ItemLiquidacao]) -> Result[List[ItemLiquidacao]]:
    """Verifica unicidade de IDs de Liquidação."""
    res = check_unique_impl(
        items,
        key_fn=lambda x: x.liquidacao.id_liquidacao_empenhonotafiscal,
        error_msg_fn=lambda dupes: f"Liquidações duplicadas detectadas: IDs {dupes}"
    )
    # Retorna os items originais para chaining se sucesso
    return res.map(lambda _: items)

def check_unique_danfe_keys(items: List[ItemLiquidacao]) -> Result[List[ItemLiquidacao]]:
    """Verifica unicidade de Chaves DANFE (evita múltiplas NFe para a mesma transação se regra de negócio exigir)."""
    # Se chave_danfe for None, ignora. Se duplicada, erro.
    res = check_unique_impl(
        items,
        key_fn=lambda x: x.liquidacao.chave_danfe,
        error_msg_fn=lambda dupes: f"Chaves DANFE duplicadas detectadas: {dupes}"
    )
    return res.map(lambda _: items)

def check_liquidacao_danfe_1to1(items: List[ItemLiquidacao]) -> Result[List[ItemLiquidacao]]:
    """
    Garante relação 1–1 entre Liquidação e DANFE.
    Uma Liquidação não pode estar associada a múltiplas DANFEs.
    """
    liq_to_danfe: Dict[str, Set[str]] = {}

    for item in items:
        # Check strict typing to avoid bugs if IDs are ints
        liq_id = str(item.liquidacao.id_liquidacao_empenhonotafiscal)
        danfe = item.liquidacao.chave_danfe

        if not danfe:
            continue  # ausência de DANFE é permitida (opcionalidade)

        if liq_id not in liq_to_danfe:
            liq_to_danfe[liq_id] = set()

        liq_to_danfe[liq_id].add(danfe)

        if len(liq_to_danfe[liq_id]) > 1:
            return Result.err(
                f"Violação 1–1: Liquidação {liq_id} associada a múltiplas DANFEs {liq_to_danfe[liq_id]}"
            )

    return Result.ok(items)

def _check_duplicates(ctx: LiquidacaoContext) -> Result[None]:
    """
    Verifica integridade dos dados via Pipeline Funcional (Monad Bind).
    1. Unicidade de IDs de Liquidação.
    2. Relação 1-1 Liquidação-DANFE.
    3. Unicidade de Chaves DANFE (global).
    """
    if not isinstance(ctx.itens_liquidados, list):
         # Se já for dict, assumimos validado ou bypass
         return Result.ok(None)
    
    # Pipeline: Inicia com items -> Valida IDs -> Valida DANFEs -> Fim
    return (
        Result.ok(ctx.itens_liquidados)
        .bind(check_unique_liquidation_ids)
        .bind(check_liquidacao_danfe_1to1)   # ← ESTA É A REGRA QUE FALTAVA
        .bind(check_unique_danfe_keys)       # opcional, dependendo da rigidez desejada
        .map(lambda _: None)
    )
