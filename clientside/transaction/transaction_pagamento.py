from dataclasses import dataclass, field
from typing import List, Optional, Dict
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.pagamento import Pagamento
from models.nfe_pagamento import NfePagamento
from models.empenho import Empenho
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction, ItemLiquidacao

@dataclass(frozen=True)
class PagamentoEfetuado:
    empenho: Empenho
    itens: List[ItemLiquidacao]
    pagamentos: List[Pagamento]
    nfe_pagamentos: List[NfePagamento]

@dataclass
class PaymentTransaction:
    liquidacao_transaction: LiquidacaoTransaction
    pagamentos_por_empenho: Dict[str, PagamentoEfetuado]

    @staticmethod
    def _get_pagamentos_empenho(id_empenho: str) -> List[Pagamento]:
        res = Pagamento.get_by_FK_id_empenho(id_empenho)
        return res.value if res.is_ok else []

    @staticmethod
    def _get_pagamentos_nfe(chave_nfe: Optional[str]) -> List[NfePagamento]:
        if not chave_nfe:
            return []
        res = NfePagamento.get_by_FK_chave_nfe(chave_nfe)
        return res.value if res.is_ok else []

    @staticmethod
    def _fetch_pagamento_context(empenho: Empenho, itens: List[ItemLiquidacao]) -> Result[PagamentoEfetuado]:
        """
        Aggregates financial records for a given empenho context (aggregated from liquidations).
        """
        # 1. Fetch Pagamentos (linked to Empenho)
        pagamentos = PaymentTransaction._get_pagamentos_empenho(empenho.id_empenho)
        
        # 2. Fetch NfePagamentos (linked to NFe from all liquidations)
        all_nfe_pagamentos: List[NfePagamento] = []
        for item in itens:
            chave_nfe = item.nfe.chave_nfe if item.nfe else None
            nfe_pags = PaymentTransaction._get_pagamentos_nfe(chave_nfe)
            all_nfe_pagamentos.extend(nfe_pags)
                
        return Result.ok(PagamentoEfetuado(
            empenho=empenho,
            itens=itens,
            pagamentos=pagamentos,
            nfe_pagamentos=all_nfe_pagamentos
        ))

    @staticmethod
    def build_from_liquidacao_transaction(tx_result: Result[LiquidacaoTransaction]) -> Result["PaymentTransaction"]:
        if tx_result.is_err:
            return Result.err(tx_result.error)

        transaction = tx_result.value
        pagamentos_por_empenho: Dict[str, PagamentoEfetuado] = {}

        # 1. Map Empenhos (Already a Dict)
        empenho_map = transaction.empenho_transaction.empenhos
        
        # 2. Iterate pre-grouped liquidations
        # Structure: Dict[id_empenho, Dict[id_liq, Item]]
        for id_empenho, inner_dict in transaction.itens_liquidados.items():
            
            empenho = empenho_map.get(id_empenho)
            if not empenho:
                 return Result.err(f"Empenho {id_empenho} found in liquidations but not in transaction empenhos list.")
            
            # List of items for this empenho
            itens = list(inner_dict.values())
            
            res = PaymentTransaction._fetch_pagamento_context(empenho, itens)
            if res.is_ok:
                pagamentos_por_empenho[id_empenho] = res.value
            else:
                 return Result.err(f"Error fetching payments for empenho {id_empenho}: {res.error}")

        return Result.ok(PaymentTransaction(
            liquidacao_transaction=transaction,
            pagamentos_por_empenho=pagamentos_por_empenho
        ))
