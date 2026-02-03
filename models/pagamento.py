from dataclasses import dataclass
from datetime import date
from decimal import Decimal

@dataclass
class Pagamento:
    id_pagamento: str
    id_empenho: str
    data_pagamento_emp: date
    valor: Decimal
