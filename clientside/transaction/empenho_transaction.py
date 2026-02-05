import sys
import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.entidade import Entidade
from models.fornecedor import Fornecedor
from models.contrato import Contrato
from models.empenho import Empenho
from result import Result

#se houver tempo aprimorar failfast validations pra validar contratos com invalidações mais estritas mais rapidos
def carregar_entidade(contrato: Contrato) -> Result[Entidade]:
    return contrato.get_entidade_FK()


def carregar_fornecedor(contrato: Contrato) -> Result[Fornecedor]:
    return contrato.get_fornecedor_FK()


def carregar_empenhos_list(contrato: Contrato) -> Result[List[Empenho]]:
    """Carrega lista bruta do banco"""
    return contrato.get_empenhos_FK()


def indexar_empenhos(lista_empenhos: List[Empenho]) -> Dict[str, Empenho]:
    """Transforma Lista de Empenhos em Dicionário indexado por ID (O(1))"""
    return {e.id_empenho: e for e in lista_empenhos}


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
    # Armazenamento otimizado para O(1) Access
    empenhos: Dict[str, Empenho] = field(default_factory=dict)

    def get_empenho_by_id(self, id_empenho: str) -> Optional[Empenho]:
        """Recupera um empenho pelo ID com complexidade O(1)."""
        return self.empenhos.get(id_empenho)

    @staticmethod
    def build_from_contract(
        contract_result: Result[Contrato]
    ) -> Result["EmpenhoTransaction"]:
        """
        Orquestra a construção da TransactionEmpenho a partir de um Contrato válido,
        utilizando pipeline funcional.
        """

        def build(contrato: Contrato) -> Result["EmpenhoTransaction"]:
            return (
                carregar_entidade(contrato)
                .bind(lambda entidade:
                    carregar_fornecedor(contrato)
                    .bind(lambda fornecedor:
                        carregar_empenhos_list(contrato)
                        .map(indexar_empenhos) # Converte List -> Dict
                        .map(lambda empenhos_dict:
                            EmpenhoTransaction(
                                entidade=entidade,
                                fornecedor=fornecedor,
                                contrato=contrato,
                                empenhos=empenhos_dict
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

        # Validar consistência de todos empenhos (iterando values do dict)
        for emp in self.empenhos.values():
            assert emp.id_contrato == self.contrato.id_contrato

    @staticmethod
    def build_from_batch(
        contratos: List[Contrato],
        entidades_map: Dict[int, Entidade],
        fornecedores_map: Dict[int, Fornecedor],
        empenhos_por_contrato: Dict[int, List[Empenho]]
    ) -> List[Result["EmpenhoTransaction"]]:
        """
        Batch builder: cria múltiplas EmpenhoTransactions a partir de dados pré-carregados.
        Elimina N+1 queries - todos os dados já vêm em memória.
        
        Args:
            contratos: Lista de Contratos
            entidades_map: Dict[id_entidade -> Entidade]
            fornecedores_map: Dict[id_fornecedor -> Fornecedor]
            empenhos_por_contrato: Dict[id_contrato -> List[Empenho]]
        
        Returns:
            Lista de Result[EmpenhoTransaction], um para cada contrato
        """
        results: List[Result["EmpenhoTransaction"]] = []
        
        for contrato in contratos:
            # Buscar entidade
            entidade = entidades_map.get(contrato.id_entidade)
            if not entidade:
                results.append(Result.err(f"Entidade {contrato.id_entidade} não encontrada"))
                continue
            
            # Buscar fornecedor
            fornecedor = fornecedores_map.get(contrato.id_fornecedor)
            if not fornecedor:
                results.append(Result.err(f"Fornecedor {contrato.id_fornecedor} não encontrado"))
                continue
            
            # Buscar empenhos do contrato
            empenhos_list = empenhos_por_contrato.get(contrato.id_contrato, [])
            empenhos_dict = {e.id_empenho: e for e in empenhos_list}
            
            try:
                tx = EmpenhoTransaction(
                    entidade=entidade,
                    fornecedor=fornecedor,
                    contrato=contrato,
                    empenhos=empenhos_dict
                )
                results.append(Result.ok(tx))
            except AssertionError as e:
                results.append(Result.err(f"Invariante violada: {e}"))
        
        return results
