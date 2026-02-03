from dataclasses import dataclass

from result import Result
from db_connection import get_db_connection

@dataclass
class Fornecedor:
    id_fornecedor: int
    nome: str
    documento: str

    def validate(self) -> Result["Fornecedor"]:
        """Executa validações do modelo Fornecedor e retorna Result."""
        return (
            Result.ok(self)
            .bind(lambda _: self._validate_id())
            .bind(lambda _: self._validate_nome())
            .bind(lambda _: self._validate_documento())
        )

    def _validate_id(self) -> Result["Fornecedor"]:
        if not self.id_fornecedor:
            return Result.err("Fornecedor inválido: id_fornecedor é obrigatório")
        return Result.ok(self)

    def _validate_nome(self) -> Result["Fornecedor"]:
        if self.nome and len(self.nome) > 255:
             return Result.err(f"Fornecedor inválido: nome excede 255 caracteres (recebido: {len(self.nome)})")
        return Result.ok(self)

    def _validate_documento(self) -> Result["Fornecedor"]:
        # Varchar(20)
        if self.documento and len(self.documento) > 20:
             return Result.err(f"Fornecedor inválido: documento excede 20 caracteres (recebido: {len(self.documento)})")
        return Result.ok(self)

    @staticmethod
    def from_row(row: dict) -> Result["Fornecedor"]:
        try:
            obj = Fornecedor(
                id_fornecedor=int(row["id_fornecedor"]) if row.get("id_fornecedor") else None,
                nome=row.get("nome"),
                documento=row.get("documento")
            )
            return obj.validate()
        except Exception as e:
            return Result.err(f"Erro ao instanciar Fornecedor: {str(e)}")

    @staticmethod
    def _fetch_raw(id_fornecedor: int) -> Result[tuple]:
        """Busca o dado cru no banco e retorna (row, description)."""
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM fornecedor WHERE id_fornecedor = %s", (id_fornecedor,))
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
        """Valida o retorno do banco: Se row é None, retorna erro. Se ok, mapeia para dict."""
        row, description = data
        if row is None:
             return Result.err("Fornecedor não encontrado.")
        
        columns = [desc[0] for desc in description]
        return Result.ok(dict(zip(columns, row)))

    @staticmethod
    def get_by_id(id_fornecedor: int) -> Result["Fornecedor"]:
        """Pipeline declarativo: Fetch -> Validate -> Map"""
        return (
            Fornecedor._fetch_raw(id_fornecedor)
            .bind(Fornecedor._validate_db_return)
            .bind(Fornecedor.from_row)
        )
