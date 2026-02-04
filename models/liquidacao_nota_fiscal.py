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
    def _fetch_raw_fk(id_empenho: str) -> Result[List[tuple]]:
        """Busca dados crus no banco filtrando por FK e retorna lista de (row, description)."""
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM liquidacao_nota_fiscal WHERE id_empenho = %s", (id_empenho,))
            rows = cursor.fetchall()
            description = cursor.description
            return Result.ok([(row, description) for row in rows])
        except Exception as e:
            return Result.err(f"Erro de conexão/consulta: {str(e)}")
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def _validate_db_return(data_list: List[tuple]) -> Result[List[dict]]:
        """Valida e processa lista de retornos do banco."""
        processed_data = []
        for row, description in data_list:
            if row:
                columns = [desc[0] for desc in description]
                processed_data.append(dict(zip(columns, row)))
        return Result.ok(processed_data)

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
    def get_by_FK_id_empenho(id_empenho: str) -> Result[List["LiquidacaoNotaFiscal"]]:
        """Fetch all Liquidacoes for an Empenho."""
        raw_res = LiquidacaoNotaFiscal._fetch_raw_fk(id_empenho)
        if raw_res.is_err:
             return Result.err(raw_res.error)
        
        dict_res = LiquidacaoNotaFiscal._validate_db_return(raw_res.value)
        if dict_res.is_err:
             return Result.err(dict_res.error)
             
        liquidacoes = []
        for row in dict_res.value:
            obj_res = LiquidacaoNotaFiscal.from_row(row)
            if obj_res.is_ok:
                liquidacoes.append(obj_res.value)
        return Result.ok(liquidacoes)
