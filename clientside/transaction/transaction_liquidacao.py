import sys
import os
from dataclasses import dataclass
from typing import List, Optional, Dict
from result import Result

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.empenho import Empenho
from models.nfe import Nfe
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from clientside.transaction.empenho_transaction import EmpenhoTransaction

##nesse modulo há duplicação do ob jeto Empenho. Pensei em alterar a estrutura de dados que armazena Empenhos em EmpenhosTransaction para um map() com o id_empenho como chave
##acesso se tornaria 0(1) e facilitaria aces  so posterior
# e gara ntir acesso controlado via getters para manter imutabilidade, e a partir dai armazenar List[EmpenhosLiquidados] via referencia ao ID de empenhos
#mas não sei se vai dar tempo, então To-Do
@dataclass(frozen=True)
class EmpenhoLiquidado:
    empenho: Empenho
    liquidacao: LiquidacaoNotaFiscal
    nfe: Optional[Nfe] # NFe is optional if not found or not strictly required by hard constraint yet

@dataclass
class LiquidacaoTransaction:
    empenho_transaction: EmpenhoTransaction
    empenhos_liquidados: Dict[str, EmpenhoLiquidado]

    @staticmethod
    def _fetch_liquidacao_pair(empenho: Empenho) -> Result[Optional[EmpenhoLiquidado]]:
        """
        Tries to fetch Liquidacao and NFe for a given Empenho.
        If Liquidacao not found, returns Ok(None) - meaning this empenho is not liquidated yet.
        """
        res_liq = LiquidacaoNotaFiscal.get_by_FK_id_empenho(empenho.id_empenho)
        
        if res_liq.is_err:
             # Logic decision: Is it an error or just 'not liquidated'? 
             # For now, let's assume it's valid to not be liquidated.
             # But if get_by_FK returns error for "not found", we handle it.
             # Assuming 'not found' might appear as empty or specific error. 
             # For simplicity here: if error, we treat as not liquidated (None).
             return Result.ok(None)
        
        liquidacao = res_liq.value
        
        # Fetch NFe
        res_nfe = Nfe.get_by_chave_nfe_FK(liquidacao.chave_danfe)
        nfe = res_nfe.value if res_nfe.is_ok else None
        
        return Result.ok(EmpenhoLiquidado(
            empenho=empenho,
            liquidacao=liquidacao,
            nfe=nfe
        ))

    @staticmethod
    def build_from_empenho_transaction(transaction_result: Result[EmpenhoTransaction]) -> Result["LiquidacaoTransaction"]:
        
        if transaction_result.is_err:
            return Result.err(transaction_result.error)

        transaction = transaction_result.value
        empenhos_liquidados: Dict[str, EmpenhoLiquidado] = {}

        for empenho in transaction.empenhos:
            # Fetch for each empenho
            res_pair = LiquidacaoTransaction._fetch_liquidacao_pair(empenho)
            if res_pair.is_err:
                # Decide if we block the whole transaction or just skip.
                # Let's return error to be safe/strict for now.
                return Result.err(f"Error fetching liquidacao for empenho {empenho.id_empenho}: {res_pair.error}")
            
            if res_pair.value:
                empenhos_liquidados[empenho.id_empenho] = res_pair.value

        return Result.ok(LiquidacaoTransaction(
            empenho_transaction=transaction,
            empenhos_liquidados=empenhos_liquidados
        ))
