"""
Domain de Validação - Pagamento

Valida regras de negócio para o estágio de Pagamento.
Acessa contextos anteriores via composability do PaymentTransaction:
    tx.liquidacao_transaction.empenho_transaction.{contrato, fornecedor, entidade}

Regras implementadas:
1. Pagamento requer Liquidação existente
2. Σ(Pagamentos) ≤ Σ(Liquidações) por Empenho
3. Data Pagamento ≥ min(Data Liquidação) por Empenho
4. IDs de Pagamento únicos
5. Σ(Pagamentos) ≤ Contrato.valor
6. Pagamento.valor > 0
7. Data Pagamento ≤ Hoje (não futuro)
8. Data Pagamento ≥ Contrato.data
9. Data Pagamento ≥ min(Empenho.data) 
"""
import sys
import os
from typing import Dict, Optional, List, FrozenSet
from decimal import Decimal
from datetime import date, datetime
from dataclasses import dataclass

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from clientside.transaction.transaction_pagamento import PaymentTransaction, PagamentoItem
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction, ItemLiquidacao
from clientside.domains.subdomains.financial_utils import quantize_money, sums_match_limit


##!! priorizing
##nessa validação eu quis evitar o oerro do ultimo domain e priorizar a legibilidade e clareza ao inves de performance

@dataclass
class PaymentValidationFragment:
    """
    Fragmento imutável de validação.
    Agrega todas as métricas necessárias para validação declarativa.
    Construído uma vez, usado por todas as regras, descartado ao final.
    """
    total_liquidado_por_empenho: Dict[str, Decimal]
    total_pago_por_empenho: Dict[str, Decimal]
    total_pago_global: Decimal
    valor_contrato: Decimal
    
    min_data_liquidacao_por_empenho: Dict[str, date]
    min_data_pagamento_por_empenho: Dict[str, date]
    max_data_pagamento: Optional[date]
    data_contrato: date
    min_data_empenho: Optional[date]
    

    empenhos_com_nfe: FrozenSet[str]
    all_pagamento_ids: List[str]
    all_pagamento_valores: List[Decimal]



def build_validation_fragment(tx: PaymentTransaction) -> PaymentValidationFragment:
    """
    Constrói o fragmento de validação via single-pass sobre os dados.
    Princípio: Agregar uma vez, validar N vezes.
    """
    liq_tx = tx.liquidacao_transaction
    emp_tx = liq_tx.empenho_transaction
    
    tot_liq: Dict[str, Decimal] = {}
    min_data_liq: Dict[str, date] = {}
    has_nfe: set = set()
    
    for id_emp, inner in liq_tx.itens_liquidados.items():
        soma = Decimal(0)
        min_d = None
        
        for item in inner.values():
            soma += item.liquidacao.valor
            d = item.liquidacao.data_emissao
            if d and (min_d is None or d < min_d):
                min_d = d
            if item.nfe:
                has_nfe.add(id_emp)
        
        tot_liq[id_emp] = soma
        if min_d:
            min_data_liq[id_emp] = min_d


    tot_pago: Dict[str, Decimal] = {}
    min_data_pag: Dict[str, date] = {}
    all_pag_ids: List[str] = []
    all_pag_valores: List[Decimal] = []
    max_data_pag: Optional[date] = None
    total_pago_global = Decimal(0)
    
    for id_emp, items in tx.pagamentos_por_empenho.items():
        soma_p = Decimal(0)
        min_d_p = None
        
        for pag_item in items:
            valor = pag_item.pagamento.valor
            soma_p += valor
            total_pago_global += valor
            all_pag_ids.append(pag_item.id_pagamento)
            all_pag_valores.append(valor)
            
            d_p = pag_item.pagamento.data_pagamento_emp
            if d_p:
                if min_d_p is None or d_p < min_d_p:
                    min_d_p = d_p
                if max_data_pag is None or d_p > max_data_pag:
                    max_data_pag = d_p
        
        tot_pago[id_emp] = soma_p
        if min_d_p:
            min_data_pag[id_emp] = min_d_p


    min_data_emp: Optional[date] = None
    for emp in emp_tx.empenhos.values():
        if emp.data_empenho:
            if min_data_emp is None or emp.data_empenho < min_data_emp:
                min_data_emp = emp.data_empenho

    return PaymentValidationFragment(
        total_liquidado_por_empenho=tot_liq,
        total_pago_por_empenho=tot_pago,
        total_pago_global=total_pago_global,
        valor_contrato=emp_tx.contrato.valor,
        min_data_liquidacao_por_empenho=min_data_liq,
        min_data_pagamento_por_empenho=min_data_pag,
        max_data_pagamento=max_data_pag,
        data_contrato=emp_tx.contrato.data,
        min_data_empenho=min_data_emp,
        empenhos_com_nfe=frozenset(has_nfe),
        all_pagamento_ids=all_pag_ids,
        all_pagamento_valores=all_pag_valores
    )




def check_pagamento_requires_liquidacao(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 1: Pagamento só existe se houver liquidação no empenho."""
    for id_empenho in frag.total_pago_por_empenho.keys():
        if id_empenho not in frag.total_liquidado_por_empenho:
            return Result.err(
                f"[INCONSISTÊNCIA] Pagamento em Empenho {id_empenho} sem liquidação registrada."
            )
    return Result.ok(None)


def check_pagamento_ids_unique(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 4: IDs de pagamento são únicos no agregado."""
    ids = frag.all_pagamento_ids
    if len(ids) != len(set(ids)):
        from collections import Counter
        counts = Counter(ids)
        dupes = [k for k, v in counts.items() if v > 1]
        return Result.err(f"[DUPLICIDADE] Pagamentos duplicados: IDs {dupes}")
    return Result.ok(None)




def check_pagamento_not_exceeds_liquidacao(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 2: Σ(Pagamentos) ≤ Σ(Liquidações) por Empenho."""
    for id_empenho, total_pago in frag.total_pago_por_empenho.items():
        total_liq = frag.total_liquidado_por_empenho.get(id_empenho, Decimal(0))
        
        if not sums_match_limit(total_pago, total_liq):
            return Result.err(
                f"[FRAUDE?] Pagamentos ({quantize_money(total_pago)}) excedem "
                f"Liquidações ({quantize_money(total_liq)}) - Empenho: {id_empenho}"
            )
    return Result.ok(None)


def check_total_pago_not_exceeds_contrato(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 5: Σ(Pagamentos) ≤ Contrato.valor (global)."""
    if not sums_match_limit(frag.total_pago_global, frag.valor_contrato):
        return Result.err(
            f"[FRAUDE?] Total Pago ({quantize_money(frag.total_pago_global)}) excede "
            f"Valor do Contrato ({quantize_money(frag.valor_contrato)})"
        )
    return Result.ok(None)


def check_pagamento_valor_positivo(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 6: Todo Pagamento.valor > 0."""
    for i, valor in enumerate(frag.all_pagamento_valores):
        if valor <= Decimal(0):
            pag_id = frag.all_pagamento_ids[i] if i < len(frag.all_pagamento_ids) else "?"
            return Result.err(
                f"[INVÁLIDO] Pagamento {pag_id} com valor não-positivo: {valor}"
            )
    return Result.ok(None)




def check_pagamento_date_after_liquidacao(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 3: min(Data Pagamento) ≥ min(Data Liquidação) por Empenho."""
    for id_empenho, min_pag_date in frag.min_data_pagamento_por_empenho.items():
        min_liq_date = frag.min_data_liquidacao_por_empenho.get(id_empenho)
        
        if min_liq_date and min_pag_date < min_liq_date:
            return Result.err(
                f"[FRAUDE?] Pagamento ({min_pag_date}) anterior à Liquidação ({min_liq_date}) "
                f"- Empenho: {id_empenho}"
            )
    return Result.ok(None)


def check_pagamento_date_not_future(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 7: max(Data Pagamento) ≤ Hoje."""
    if frag.max_data_pagamento and frag.max_data_pagamento > date.today():
        return Result.err(
            f"[SUSPEITO] Pagamento com data futura detectado: {frag.max_data_pagamento}"
        )
    return Result.ok(None)


def check_pagamento_date_after_contrato(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 8: min(Data Pagamento) ≥ Contrato.data."""
    for id_emp, min_pag in frag.min_data_pagamento_por_empenho.items():
        if min_pag < frag.data_contrato:
            return Result.err(
                f"[FRAUDE?] Pagamento ({min_pag}) anterior ao Contrato ({frag.data_contrato}) "
                f"- Empenho: {id_emp}"
            )
    return Result.ok(None)


def check_pagamento_date_after_empenho(frag: PaymentValidationFragment) -> Result[None]:
    """Regra 9: min(Data Pagamento) ≥ min(Empenho.data)."""
    if not frag.min_data_empenho:
        return Result.ok(None)
    
    for id_emp, min_pag in frag.min_data_pagamento_por_empenho.items():
        if min_pag < frag.min_data_empenho:
            return Result.err(
                f"[FRAUDE?] Pagamento ({min_pag}) anterior ao Empenho ({frag.min_data_empenho}) "
                f"- Empenho: {id_emp}"
            )
    return Result.ok(None)




PAGAMENTO_VALIDATION_RULES = [
    # Integridade
    check_pagamento_requires_liquidacao,
    check_pagamento_ids_unique,
    # Financeiro
    check_pagamento_not_exceeds_liquidacao,
    check_total_pago_not_exceeds_contrato,
    check_pagamento_valor_positivo,
    # Temporal
    check_pagamento_date_after_liquidacao,
    check_pagamento_date_not_future,
    check_pagamento_date_after_contrato,
    check_pagamento_date_after_empenho,
]


def apply_rules(frag: PaymentValidationFragment, rules: list) -> Result[None]:
    """
    Aplica lista de regras em sequência (circuit-break on first error).
    Pattern: Functional Composition with Early Exit.
    """
    for rule in rules:
        result = rule(frag)
        if result.is_err:
            return result
    return Result.ok(None)




def Valida(tx: PaymentTransaction) -> Result[PaymentTransaction]:
    """
    Valida PaymentTransaction aplicando todas as regras de negócio.
    
    Pipeline:
    1. Create Fragment (aggregate once)
    2. Apply Rules (validate N times)
    3. Discard Fragment (implicit)
    4. Return validated tx or error
    """
    fragment = build_validation_fragment(tx)
    
    validation_result = apply_rules(fragment, PAGAMENTO_VALIDATION_RULES)
    
    if validation_result.is_err:
        return Result.err(validation_result.error)
    
    return Result.ok(tx)
