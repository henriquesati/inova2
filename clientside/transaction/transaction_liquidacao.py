
import sys
import os
from dataclasses import dataclass
from typing import Optional
from result import Result

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.empenho import Empenho
from models.nfe import Nfe
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal

@dataclass
class LiquidacaoTransaction:
    empenho: Empenho
    liquidacao: LiquidacaoNotaFiscal
    nfe: Nfe

    @staticmethod
    def _fetch_liquidacao_nota_fiscal(empenho: Empenho) -> Result[LiquidacaoNotaFiscal]:
        return LiquidacaoNotaFiscal.get_by_FK_id_empenho(empenho.id_empenho)

    @staticmethod
    def _fetch_nfe(liquidacao_nota_fiscal: LiquidacaoNotaFiscal) -> Result[Nfe]:
        return Nfe.get_by_chave_nfe(liquidacao_nota_fiscal.chave_danfe)

    @staticmethod
    def build_from_empenho_result(empenho_result: Result[Empenho]) -> Result["LiquidacaoTransaction"]:
        return empenho_result.bind(lambda empenho: 
            LiquidacaoTransaction._fetch_liquidacao_nota_fiscal(empenho).bind(
                lambda liquidacao_nota_fiscal: LiquidacaoTransaction._fetch_nfe(liquidacao_nota_fiscal).bind(
                    lambda nfe: LiquidacaoTransaction.build(empenho, liquidacao_nota_fiscal, nfe)
                )
            )
        )

    @staticmethod
    def build(empenho: Empenho, liquidacao_nota_fiscal: LiquidacaoNotaFiscal, nfe: Nfe) -> Result["LiquidacaoTransaction"]:
        return Result.ok(LiquidacaoTransaction(
            empenho=empenho,
            liquidacao=liquidacao_nota_fiscal,
            nfe=nfe
        ))


