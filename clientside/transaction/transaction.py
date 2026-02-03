from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional

from domain.models.entidade import Entidade
from domain.models.fornecedor import Fornecedor
from domain.models.contrato import Contrato
from domain.models.empenho import Empenho
from domain.models.liquidacao import Liquidacao
from domain.models.pagamento import Pagamento
from domain.shared.result import Result


class EstadoDespesa(Enum):
    INICIAL = "inicial"
    EMPENHO = "empenho"
    LIQUIDACAO = "liquidacao"
    PAGAMENTO = "pagamento"


@dataclass(frozen=True)
class DespesaContext:
    estado: EstadoDespesa

    entidade: Optional[Entidade] = None
    fornecedor: Optional[Fornecedor] = None
    contrato: Optional[Contrato] = None
    empenho: Optional[Empenho] = None
    liquidacao: Optional[Liquidacao] = None
    pagamento: Optional[Pagamento] = None

    def para_empenho(self) -> Result["DespesaContext"]:
        if self.estado != EstadoDespesa.INICIAL:
            return Result.err(
                f"Transição inválida: esperado INICIAL, recebido {self.estado}"
            )

        return Result.ok(
            replace(self, estado=EstadoDespesa.EMPENHO)
        )

    def para_liquidacao(self) -> Result["DespesaContext"]:
        if self.estado != EstadoDespesa.EMPENHO:
            return Result.err(
                f"Transição inválida: esperado EMPENHO, recebido {self.estado}"
            )

        if self.empenho is None:
            return Result.err("Não é possível liquidar sem empenho")

        return Result.ok(
            replace(self, estado=EstadoDespesa.LIQUIDACAO)
        )

    def para_pagamento(self) -> Result["DespesaContext"]:
        if self.estado != EstadoDespesa.LIQUIDACAO:
            return Result.err(
                f"Transição inválida: esperado LIQUIDACAO, recebido {self.estado}"
            )

        if self.liquidacao is None:
            return Result.err("Não é possível pagar sem liquidação")

        return Result.ok(
            replace(self, estado=EstadoDespesa.PAGAMENTO)
        )
