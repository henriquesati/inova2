from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from result import Result
from db_connection import get_db_connection

@dataclass
class Nfe:
    id: int
    chave_nfe: str
    numero_nfe: str
    data_hora_emissao: datetime
    cnpj_emitente: str
    valor_total_nfe: Decimal

    def validate(self) -> Result["Nfe"]:
        """Executa validações do modelo Nfe e retorna Result."""
        return (
            Result.ok(self)
            .bind(lambda _: self._validate_chave_nfe())
            .bind(lambda _: self._validate_numero_nfe())
            .bind(lambda _: self._validate_cnpj())
        )

    def _validate_chave_nfe(self) -> Result["Nfe"]:
        # Chave NFE usually 44 chars
        if self.chave_nfe and len(self.chave_nfe) > 50:
             return Result.err(f"Nfe inválida: chave_nfe excede 50 caracteres")
        return Result.ok(self)

    def _validate_numero_nfe(self) -> Result["Nfe"]:
        if self.numero_nfe and len(self.numero_nfe) > 20: 
             return Result.err(f"Nfe inválida: numero_nfe excede 20 caracteres")
        return Result.ok(self)

    def _validate_cnpj(self) -> Result["Nfe"]:
        if self.cnpj_emitente and len(self.cnpj_emitente) > 20:
             return Result.err(f"Nfe inválida: cnpj_emitente excede 20 caracteres")
        return Result.ok(self)

    @staticmethod
    def _fetch_raw(chave_nfe: str) -> Result[tuple]:
        """Busca dados crus no banco filtrando por chave_nfe e retorna (row, description). LIMIT 1."""
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nfe WHERE chave_nfe = %s LIMIT 1", (chave_nfe,))
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
             return Result.err("Nfe não encontrada.")
        
        columns = [desc[0] for desc in description]
        return Result.ok(dict(zip(columns, row)))

    @staticmethod
    def from_row(row: dict) -> Result["Nfe"]:
        """Mapeia um dicionário para um objeto Nfe."""
        try:
            obj = Nfe(
                id=int(row["id"]),
                chave_nfe=row["chave_nfe"],
                numero_nfe=row["numero_nfe"],
                data_hora_emissao=row["data_hora_emissao"],
                cnpj_emitente=row["cnpj_emitente"],
                valor_total_nfe=row["valor_total_nfe"]
            )
            return obj.validate()
        except Exception as e:
            return Result.err(f"Erro ao instanciar Nfe: {str(e)}")

    @staticmethod
    def get_by_chave_nfe(chave_nfe: str) -> Result["Nfe"]:
        """Pipeline declarativo: Fetch (First) -> Validate -> Map"""
        return (
            Nfe._fetch_raw(chave_nfe)
            .bind(Nfe._validate_db_return)
            .bind(Nfe.from_row)
        )
#!Design choices -  Escolhi retornar apenas o 1 resultado na busca pela FK da entidade, fazendo a validação de rompimento de 1-1 posteriormente
#na verdade, ainda preciso verificar se: existe relação 1-1 e se o rompimento compromete integridade da transação