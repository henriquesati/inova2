from dataclasses import dataclass
from typing import List, Optional
import sys
import os

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.pagamento import Pagamento
from models.nfe_pagamento import NfePagamento
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction, EmpenhoLiquidado

@dataclass(frozen=True)
class PagamentoEfetuado:
    empenho_liquidado: EmpenhoLiquidado
    pagamentos: List[Pagamento]
    nfe_pagamentos: List[NfePagamento]

@dataclass
class PaymentTransaction:
    liquidacao_transaction: LiquidacaoTransaction
    pagamentos_efetuados: List[PagamentoEfetuado]

    @staticmethod
    def _fetch_pagamentos_efetuados(empenho_liquidado: EmpenhoLiquidado) -> Result[PagamentoEfetuado]:
        """
        Fetches related Pagamento and NfePagamento records for a given liquidated empenho context.
        """
        # 1. Fetch Pagamentos (linked to Empenho)
        res_pag = Pagamento.get_by_FK_id_empenho(empenho_liquidado.empenho.id_empenho)
        pagamentos = res_pag.value if res_pag.is_ok else []
        
        # 2. Fetch NfePagamentos (linked to NFe, if exists)
        nfe_pagamentos = []
        if empenho_liquidado.nfe:
            res_nfe_pag = NfePagamento.get_by_FK_chave_nfe(empenho_liquidado.nfe.chave_nfe)
            if res_nfe_pag.is_ok:
                nfe_pagamentos = res_nfe_pag.value
                
        return Result.ok(PagamentoEfetuado(
            empenho_liquidado=empenho_liquidado,
            pagamentos=pagamentos,
            nfe_pagamentos=nfe_pagamentos
        ))
        ##aqui mais uma vez há repetição de estruturas em memória :(

    @staticmethod
    def build_from_liquidacao_transaction(tx_result: Result[LiquidacaoTransaction]) -> Result["PaymentTransaction"]:
        """
        Builds PaymentTransaction from a LiquidacaoTransaction result.
        """
        if tx_result.is_err:
            return Result.err(tx_result.error)

        transaction = tx_result.value
        pagamentos_efetuados: List[PagamentoEfetuado] = []

        for item in transaction.empenhos_liquidados:
            res = PaymentTransaction._fetch_pagamentos_efetuados(item)
            if res.is_ok:
                pagamentos_efetuados.append(res.value)
            else:
                 return Result.err(f"Error fetching payments for empenho {item.empenho.id_empenho}: {res.error}")

        return Result.ok(PaymentTransaction(
            liquidacao_transaction=transaction,
            pagamentos_efetuados=pagamentos_efetuados
        ))
