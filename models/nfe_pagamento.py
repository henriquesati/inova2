from dataclasses import dataclass
from decimal import Decimal
from typing import List
from result import Result
from db_connection import get_db_connection

@dataclass
class NfePagamento:
    id: str
    chave_nfe: str
    tipo_pagamento: str
    valor_pagamento: Decimal

    @staticmethod
    def _fetch_raw_by_nfe(chave_nfe: str) -> Result[List[dict]]:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nfe_pagamento WHERE chave_nfe = %s", (chave_nfe,))
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            cursor.close()
            conn.close()
            return Result.ok([dict(zip(cols, row)) for row in rows])
        except Exception as e:
             return Result.err(f"DB Error fetching NfePagamento: {e}")

    @staticmethod
    def from_row(row: dict) -> Result["NfePagamento"]:
        try:
            return Result.ok(NfePagamento(
                id=str(row["id"]),
                chave_nfe=str(row["chave_nfe"]),
                tipo_pagamento=row["tipo_pagamento"],
                valor_pagamento=row["valor_pagamento"]
            ))
        except Exception as e:
            return Result.err(f"Parse Error NfePagamento: {e}")

    @staticmethod
    def get_by_FK_chave_nfe(chave_nfe: str) -> Result[List["NfePagamento"]]:
        """Fetch FK (List) -> Map Row"""
        return (
            NfePagamento._fetch_raw_by_nfe(chave_nfe)
            .bind(lambda rows: Result.ok([
                NfePagamento.from_row(row).value 
                for row in rows 
                if NfePagamento.from_row(row).is_ok
            ]))
        )
