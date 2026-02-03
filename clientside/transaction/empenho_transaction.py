import sys
import os
from dataclasses import dataclass, field
from typing import List

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.entidade import Entidade
from models.fornecedor import Fornecedor
from models.contrato import Contrato
from models.empenho import Empenho
from result import Result


def carregar_entidade(contrato: Contrato) -> Result[Entidade]:
    return contrato.get_entidade_FK()


def carregar_fornecedor(contrato: Contrato) -> Result[Fornecedor]:
    return contrato.get_fornecedor_FK()

##ficar aten to ao uso de map() aqui, porque models/empenho pode resultar falha
def carregar_empenhos(contrato: Contrato) -> Result[List[Empenho]]:
    return contrato.get_empenhos_FK()


# Domain Aggregate

@dataclass(frozen=True)
class EmpenhoTransaction:
    """
    Aggregate imutável que representa a fase de Empenho da despesa pública.
    Agrupa Contrato, Entidade, Fornecedor e todos os Empenhos associados.
    """
    entidade: Entidade
    fornecedor: Fornecedor
    contrato: Contrato
    empenhos: List[Empenho] = field(default_factory=list)

    @staticmethod
    def build_from_contract(
        contract_result: Result[Contrato]
    ) -> Result["EmpenhoTransaction"]:
        """
        Orquestra a construção da TransactionEmpenho a partir de um Contrato válido,
        """

        def build(contrato: Contrato) -> Result["EmpenhoTransaction"]:
            return (
                carregar_entidade(contrato)
                .bind(lambda entidade:
                    carregar_fornecedor(contrato)
                    .bind(lambda fornecedor:
                        carregar_empenhos(contrato)
                        .map(lambda empenhos:
                            EmpenhoTransaction(
                                entidade=entidade,
                                fornecedor=fornecedor,
                                contrato=contrato,
                                empenhos=empenhos
                            )
                        )
                    )
                )
            )

        return contract_result.bind(build)

    def __post_init__(self):
        # Invariantes internas do agregado
        assert self.contrato.id_entidade == self.entidade.id_entidade
        assert self.contrato.id_fornecedor == self.fornecedor.id_fornecedor

        for emp in self.empenhos:
            assert emp.id_contrato == self.contrato.id_contrato
