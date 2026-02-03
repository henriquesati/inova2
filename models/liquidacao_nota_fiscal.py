from typing import List
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from result import Result
from db_connection import get_db_connection

@dataclass
class LiquidacaoNotaFiscal:
    id_liquidacao_empenhonotafiscal: int
    chave_danfe: str #FK to NFE
    data_emissao: date
    valor: Decimal
    id_empenho: str #FK to Empenho

    @staticmethod
    def _fetch_raw_fk(id_empenho: str) -> Result[tuple]:
        """Busca dados crus no banco filtrando por FK e retorna (row, description). Retorna apenas o primeiro."""
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM liquidacao_nota_fiscal WHERE id_empenho = %s LIMIT 1", (id_empenho,))
            row = cursor.fetchone()
            description = cursor.description
            return Result.ok((row, description))
        except Exception as e:
            return Result.err(f"Erro de conexão/consulta: {str(e)}")
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def _validate_db_return(data: tuple) -> Result[dict]:
        """Valida o retorno do banco. Se row é None, retorna erro."""
        row, description = data
        if row is None:
             return Result.err("LiquidacaoNotaFiscal não encontrada para este empenho.")
        
        columns = [desc[0] for desc in description]
        return Result.ok(dict(zip(columns, row)))

    @staticmethod
    def from_row(row: dict) -> Result["LiquidacaoNotaFiscal"]:
        try:
            obj = LiquidacaoNotaFiscal(
                id_liquidacao_empenhonotafiscal=int(row["id_liquidacao_empenhonotafiscal"]),
                chave_danfe=row["chave_danfe"],
                data_emissao=row["data_emissao"],
                valor=row["valor"],
                id_empenho=str(row["id_empenho"])
            )
            return Result.ok(obj)
        except Exception as e:
            return Result.err(f"Erro ao instanciar LiquidacaoNotaFiscal: {str(e)}")

    @staticmethod
    def get_by_FK_id_empenho(id_empenho: str) -> Result["LiquidacaoNotaFiscal"]:
        return (
            LiquidacaoNotaFiscal._fetch_raw_fk(id_empenho)
            .bind(LiquidacaoNotaFiscal._validate_db_return)
            .bind(LiquidacaoNotaFiscal.from_row)
        )
