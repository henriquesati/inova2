from dataclasses import dataclass
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
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction, EmpenhoLiquidado

#List[PagamentoEfetuado] fica o(n2), péssimo

@dataclass(frozen=True)
class PagamentoEfetuado:
    empenho_liquidado: EmpenhoLiquidado
    pagamentos: List[Pagamento]
    nfe_pagamentos: List[NfePagamento]

@dataclass
class PaymentTransaction:
    liquidacao_transaction: LiquidacaoTransaction
    pagamentos_por_empenho: Dict[str, PagamentoEfetuado]

    @staticmethod
    def _get_pagamentos_empenho(id_empenho: str) -> List[Pagamento]:
        """Fetch List[Pagamento] safe unwrap."""
        res = Pagamento.get_by_FK_id_empenho(id_empenho)
        return res.value if res.is_ok else []

    @staticmethod
    def _get_pagamentos_nfe(chave_nfe: Optional[str]) -> List[NfePagamento]:
        """Fetch List[NfePagamento] safe unwrap."""
        if not chave_nfe:
            return []
        res = NfePagamento.get_by_FK_chave_nfe(chave_nfe)
        return res.value if res.is_ok else []

    @staticmethod
    def _fetch_pagamento_context(empenho_liquidado: EmpenhoLiquidado) -> Result[PagamentoEfetuado]:
        """
        Aggregates financial records for a given liquidated empenho context.
        """
        # 1. Fetch Pagamentos (linked to Empenho)
        pagamentos = PaymentTransaction._get_pagamentos_empenho(empenho_liquidado.empenho.id_empenho)
        
        # 2. Fetch NfePagamentos (linked to NFe)
        chave_nfe = empenho_liquidado.nfe.chave_nfe if empenho_liquidado.nfe else None
        nfe_pagamentos = PaymentTransaction._get_pagamentos_nfe(chave_nfe)
                
        return Result.ok(PagamentoEfetuado(
            empenho_liquidado=empenho_liquidado,
            pagamentos=pagamentos,
            nfe_pagamentos=nfe_pagamentos
        ))

    @staticmethod
    def build_from_liquidacao_transaction(tx_result: Result[LiquidacaoTransaction]) -> Result["PaymentTransaction"]:
        """
        Builds PaymentTransaction from a LiquidacaoTransaction result using Dict mapping.
        """
        if tx_result.is_err:
            return Result.err(tx_result.error)

        transaction = tx_result.value
        pagamentos_por_empenho: Dict[str, PagamentoEfetuado] = {}

        for item in transaction.empenhos_liquidados.values():
            res = PaymentTransaction._fetch_pagamento_context(item)
            if res.is_ok:
                pagamentos_por_empenho[item.empenho.id_empenho] = res.value
            else:
                 return Result.err(f"Error fetching payments for empenho {item.empenho.id_empenho}: {res.error}")

        return Result.ok(PaymentTransaction(
            liquidacao_transaction=transaction,
            pagamentos_por_empenho=pagamentos_por_empenho
        ))
