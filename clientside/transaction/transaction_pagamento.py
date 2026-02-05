"""
PaymentTransaction - Estágio final do lifecycle de despesa pública.

Este módulo implementa o agregado imutável que representa pagamentos,
vinculados contextualmente à fase de liquidação/empenho.

Design:
- Aggregate imutável (frozen dataclass)
- Fetch e construção de dados
- Validação delegada ao domain layer (domains/pagamento.py)
"""
from dataclasses import dataclass, field
from typing import List, Dict, Tuple
from decimal import Decimal
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.pagamento import Pagamento
from models.nfe_pagamento import NfePagamento
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction, ItemLiquidacao




@dataclass(frozen=True)
class PagamentoItem:
    """
    Representação imutável de um item de pagamento.
    """
    id_pagamento: str
    pagamento: Pagamento
    nfe_pagamentos: Tuple[NfePagamento, ...]


@dataclass(frozen=True)
class PaymentTransaction:
    """
    Aggregate Final Imutável.
    Contém a fonte da verdade (LiquidacaoTransaction) e os Pagamentos processados.
    
    Nota: Pagamentos são agrupados por EMPENHO, pois o modelo de dados
    não possui vínculo direto Pagamento -> Liquidação.
    """
    liquidacao_transaction: LiquidacaoTransaction
    
    # Dict[id_empenho, Tuple[PagamentoItem, ...]]
    pagamentos_por_empenho: Dict[str, Tuple[PagamentoItem, ...]]


    @staticmethod
    def _fetch_pagamentos_and_nfe(id_empenho: str) -> List[PagamentoItem]:
        """Fetch & Compose: Pagamento + NfePagamentos -> PagamentoItem"""
        res_pags = Pagamento.get_by_FK_id_empenho(id_empenho)
        if res_pags.is_err: 
            return []
        
        items = []
        for pag in res_pags.value:
            items.append(PagamentoItem(
                id_pagamento=pag.id_pagamento,
                pagamento=pag,
                nfe_pagamentos=() 
            ))
        return items

    @staticmethod
    def build_from_liquidacao_transaction(
        tx_result: Result[LiquidacaoTransaction]
    ) -> Result["PaymentTransaction"]:
        """
        Pipeline funcional para construir PaymentTransaction.
        
        Fluxo:
        1. Recebe LiquidacaoTransaction validada
        2. Fetch pagamentos por empenho
        """
        if tx_result.is_err: 
            return Result.err(tx_result.error)
        
        liq_tx = tx_result.value
        
        pagamentos_map: Dict[str, Tuple[PagamentoItem, ...]] = {}
        
        all_empenho_ids = liq_tx.itens_liquidados.keys()
        
        for id_emp in all_empenho_ids:
            items = PaymentTransaction._fetch_pagamentos_and_nfe(id_emp)
            if items:
                pagamentos_map[id_emp] = tuple(items)
            

        return Result.ok(PaymentTransaction(
            liquidacao_transaction=liq_tx,
            pagamentos_por_empenho=pagamentos_map
        ))

    @staticmethod
    def build_from_batch(
        liquidacao_transaction: LiquidacaoTransaction,
        pagamentos_por_empenho: Dict[str, List[Pagamento]]
    ) -> Result["PaymentTransaction"]:
        """
        Batch builder: cria PaymentTransaction a partir de dados pré-carregados.
        Elimina N+1 queries.
        
        Args:
            liquidacao_transaction: LiquidacaoTransaction validada
            pagamentos_por_empenho: Dict[id_empenho -> List[Pagamento]]
        """
        pagamentos_map: Dict[str, Tuple[PagamentoItem, ...]] = {}
        
        all_empenho_ids = liquidacao_transaction.itens_liquidados.keys()
        
        for id_emp in all_empenho_ids:
            pagamentos = pagamentos_por_empenho.get(id_emp, [])
            if pagamentos:
                items = [
                    PagamentoItem(
                        id_pagamento=pag.id_pagamento,
                        pagamento=pag,
                        nfe_pagamentos=()
                    )
                    for pag in pagamentos
                ]
                pagamentos_map[id_emp] = tuple(items)

        return Result.ok(PaymentTransaction(
            liquidacao_transaction=liquidacao_transaction,
            pagamentos_por_empenho=pagamentos_map
        ))

