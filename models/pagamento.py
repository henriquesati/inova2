from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List
from result import Result
from db_connection import get_db_connection

@dataclass
class Pagamento:
    id_pagamento: str
    id_empenho: str
    data_pagamento_emp: date
    valor: Decimal

    @staticmethod
    def _fetch_raw_by_empenho(id_empenho: str) -> Result[List[dict]]:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pagamento WHERE id_empenho = %s", (id_empenho,))
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            cursor.close()
            conn.close()
            return Result.ok([dict(zip(cols, row)) for row in rows])
        except Exception as e:
             return Result.err(f"DB Error fetching Pagamento: {e}")

    @staticmethod
    def from_row(row: dict) -> Result["Pagamento"]:
        try:
            return Result.ok(Pagamento(
                id_pagamento=str(row["id_pagamento"]),
                id_empenho=str(row["id_empenho"]),
                data_pagamento_emp=row["datapagamentoempenho"],  # DB column name
                valor=row["valor"]
            ))
        except Exception as e:
            return Result.err(f"Parse Error Pagamento: {e}")

    @staticmethod
    def get_by_FK_id_empenho(id_empenho: str) -> Result[List["Pagamento"]]:
        """Fetch FK (List) -> Map Row"""
        return (
            Pagamento._fetch_raw_by_empenho(id_empenho)
            .bind(lambda rows: Result.ok([
                Pagamento.from_row(row).value 
                for row in rows 
                if Pagamento.from_row(row).is_ok
            ]))
        )
