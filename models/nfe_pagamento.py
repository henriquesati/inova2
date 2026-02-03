from dataclasses import dataclass
from decimal import Decimal

@dataclass
class NfePagamento:
    id: str
    chave_nfe: str
    tipo_pagamento: str
    valor_pagamento: Decimal
