import sys
import os
from dataclasses import dataclass
from typing import List, Optional, Dict, Union
from result import Result

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.empenho import Empenho
from models.nfe import Nfe
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from clientside.transaction.empenho_transaction import EmpenhoTransaction

# ESTRUTURA DE DADOS APÓS NORMALIZAÇÃO:
# Dict[str, Dict[str, ItemLiquidacao]]
# Onde:
#   - Outer Key: id_empenho (Agrupa por contexto de empenho)
#   - Inner Key: str(id_liquidacao) -> Garante unicidade dentro do grupo
#   - Value: ItemLiquidacao(liquidacao, nfe)
#
# Isso facilita validações de agregados (soma de liquidacoes vs empenho)

from collections import defaultdict

@dataclass(frozen=True)
class ItemLiquidacao:
    liquidacao: LiquidacaoNotaFiscal
    nfe: Optional[Nfe]

# TypeAlias para clareza (suporta ambos estagio Raw e Normalized)
# Raw: List[ItemLiquidacao]
# Normalized: Dict[id_empenho, Dict[id_liquidacao, ItemLiquidacao]]
ItensLiquidados = Union[List[ItemLiquidacao], Dict[str, Dict[str, ItemLiquidacao]]]

@dataclass
class LiquidacaoTransaction:
    empenho_transaction: EmpenhoTransaction
    # Inicialmente Lista (Raw), depois Dict Aninhado (Normalized)
    itens_liquidados: ItensLiquidados

    @staticmethod
    def _fetch_liquidacoes(id_empenho: str) -> List[LiquidacaoNotaFiscal]:
        res = LiquidacaoNotaFiscal.get_by_FK_id_empenho(id_empenho)
        return res.value if res.is_ok else []

    @staticmethod
    def _fetch_nfe(chave_danfe: str) -> Optional[Nfe]:
        res = Nfe.get_by_chave_nfe_FK(chave_danfe)
        return res.value if res.is_ok else None

    def normalize(self):
        """
        design choices !! nessa estruturação eu prefiro armazenar id_empenho, o objetoliquidacao
        pelo id_liquidacao (Dict[str, ItemLiquidacao]]), mas isso esconderia possiveis anomalias no banco de duplicação.
        pra tratar isso eu provavelmente vou romper a imutabilidade de objeto no dominio, alterando o estado interno 
        de armazenamento, ou originando uma nova estrutura no objeto!
        """
        if isinstance(self.itens_liquidados, list):
            # Dict[id_empenho, Dict[id_liq, Item]]
            normalized: Dict[str, Dict[str, ItemLiquidacao]] = defaultdict(dict)
            
            for item in self.itens_liquidados:
                id_emp = item.liquidacao.id_empenho
                id_liq = str(item.liquidacao.id_liquidacao_empenhonotafiscal)
                
                normalized[id_emp][id_liq] = item
                
            self.itens_liquidados = dict(normalized)

    @staticmethod
    def build_from_empenho_transaction(transaction_result: Result[EmpenhoTransaction]) -> Result["LiquidacaoTransaction"]:
        
        if transaction_result.is_err:
            return Result.err(transaction_result.error)

        transaction = transaction_result.value
        # Inicializa como Lista para capturar tudo (incluindo possíveis duplicatas)
        itens_liquidados: List[ItemLiquidacao] = []

        for empenho in transaction.empenhos.values():
            # 1. Busca todas liquidações do empenho
            liquidacoes = LiquidacaoTransaction._fetch_liquidacoes(empenho.id_empenho)
            
            # 2. Para cada liquidação, busca o contexto (NFe) e registra
            for liq in liquidacoes:
                nfe = LiquidacaoTransaction._fetch_nfe(liq.chave_danfe)
                item = ItemLiquidacao(liquidacao=liq, nfe=nfe)
                itens_liquidados.append(item)

        return Result.ok(LiquidacaoTransaction(
            empenho_transaction=transaction,
            itens_liquidados=itens_liquidados
        ))
