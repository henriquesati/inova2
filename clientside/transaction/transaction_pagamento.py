
import sys 
import os
from dataclasses import dataclass

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.pagamento import Pagamento
from models.nfe_pagamento import NfePagamento

from typing import Optional

@dataclass
class PagamentoTransaction:
    liquidacao: Optional[LiquidacaoNotaFiscal]
    pagamento: Optional[Pagamento]
    nfe_pagamento: Optional[NfePagamento]

    def __post_init__(self):
        # Exemplo de lógica adicional se necessário
        pass
