import sys
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Union
from result import Result
from collections import defaultdict

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.empenho import Empenho
from models.nfe import Nfe
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from clientside.transaction.empenho_transaction import EmpenhoTransaction

@dataclass(frozen=True)
class ItemLiquidacao:
    liquidacao: LiquidacaoNotaFiscal
    nfe: Optional[Nfe]

@dataclass(frozen=True)
class LiquidacaoTransaction:
    empenho_transaction: EmpenhoTransaction
    # Estrutura Imutável: Dict[id_empenho, Dict[id_liquidacao, ItemLiquidacao]]
    itens_liquidados: Dict[str, Dict[str, ItemLiquidacao]]
    # Índice otimizado para validações de NFe
    items_by_nfe: Dict[str, List[ItemLiquidacao]] = field(default_factory=dict)

    @staticmethod
    def _fetch_liquidacoes(id_empenho: str) -> List[LiquidacaoNotaFiscal]:
        res = LiquidacaoNotaFiscal.get_by_FK_id_empenho(id_empenho)
        return res.value if res.is_ok else []

    @staticmethod
    def _fetch_nfe(chave_danfe: str) -> Optional[Nfe]:
        res = Nfe.get_by_chave_nfe_FK(chave_danfe)
        return res.value if res.is_ok else None

    @staticmethod
    def build_from_empenho_transaction(transaction_result: Result[EmpenhoTransaction]) -> Result["LiquidacaoTransaction"]:
        
        if transaction_result.is_err:
            return Result.err(transaction_result.error)

        transaction = transaction_result.value
        
        # Estruturas de construção
        itens_liquidados: Dict[str, Dict[str, ItemLiquidacao]] = defaultdict(dict)
        items_by_nfe: Dict[str, List[ItemLiquidacao]] = defaultdict(list)

        for empenho in transaction.empenhos.values():
            id_emp = empenho.id_empenho
            
            # 1. Busca todas liquidações do empenho
            liquidacoes = LiquidacaoTransaction._fetch_liquidacoes(id_emp)
            
            # 2. Para cada liquidação, constrói item e popula estruturas
            for liq in liquidacoes:
                nfe = LiquidacaoTransaction._fetch_nfe(liq.chave_danfe)
                item = ItemLiquidacao(liquidacao=liq, nfe=nfe)
                
                # Normalização principal
                id_liq = str(liq.id_liquidacao_empenhonotafiscal)
                itens_liquidados[id_emp][id_liq] = item
                
                # Indexação secundária por NFe
                if nfe:
                    items_by_nfe[nfe.chave_nfe].append(item)

        return Result.ok(LiquidacaoTransaction(
            empenho_transaction=transaction,
            itens_liquidados=dict(itens_liquidados),
            items_by_nfe=dict(items_by_nfe)
        ))

    @staticmethod
    def build_from_batch(
        empenho_transaction: EmpenhoTransaction,
        liquidacoes_por_empenho: Dict[str, List[LiquidacaoNotaFiscal]],
        nfes_map: Dict[str, Nfe]
    ) -> Result["LiquidacaoTransaction"]:
        """
        Batch builder: cria LiquidacaoTransaction a partir de dados pré-carregados.
        Elimina N+1 queries.
        
        Args:
            empenho_transaction: EmpenhoTransaction validada
            liquidacoes_por_empenho: Dict[id_empenho -> List[LiquidacaoNotaFiscal]]
            nfes_map: Dict[chave_nfe -> Nfe]
        """
        itens_liquidados: Dict[str, Dict[str, ItemLiquidacao]] = defaultdict(dict)
        items_by_nfe: Dict[str, List[ItemLiquidacao]] = defaultdict(list)

        for empenho in empenho_transaction.empenhos.values():
            id_emp = empenho.id_empenho
            liquidacoes = liquidacoes_por_empenho.get(id_emp, [])
            
            for liq in liquidacoes:
                nfe = nfes_map.get(liq.chave_danfe)
                item = ItemLiquidacao(liquidacao=liq, nfe=nfe)
                
                id_liq = str(liq.id_liquidacao_empenhonotafiscal)
                itens_liquidados[id_emp][id_liq] = item
                
                if nfe:
                    items_by_nfe[nfe.chave_nfe].append(item)

        return Result.ok(LiquidacaoTransaction(
            empenho_transaction=empenho_transaction,
            itens_liquidados=dict(itens_liquidados),
            items_by_nfe=dict(items_by_nfe)
        ))

