from dataclasses import dataclass
from result import Result
from db_connection import get_db_connection

@dataclass
class Entidade:
    id_entidade: int
    nome: str
    estado: str
    municipio: str
    cnpj: str

    def validate(self) -> Result["Entidade"]:
        """Executa validações do modelo Entidade e retorna Result."""
        return (
            Result.ok(self)
            .bind(lambda _: self._validate_id())
            .bind(lambda _: self._validate_nome())
            .bind(lambda _: self._validate_estado())
            .bind(lambda _: self._validate_municipio())
            .bind(lambda _: self._validate_cnpj())
        )

    def _validate_id(self) -> Result["Entidade"]:
        if not self.id_entidade:
            return Result.err("Entidade inválida: id_entidade é obrigatório")
        return Result.ok(self)

    def _validate_nome(self) -> Result["Entidade"]:
        if self.nome and len(self.nome) > 255:
             return Result.err(f"Entidade inválida: nome excede 255 caracteres (recebido: {len(self.nome)})")
        return Result.ok(self)

    def _validate_estado(self) -> Result["Entidade"]:
        if self.estado and len(self.estado) > 50:
             return Result.err(f"Entidade inválida: estado excede 50 caracteres (recebido: {len(self.estado)})")
        return Result.ok(self)

    def _validate_municipio(self) -> Result["Entidade"]:
         if self.municipio and len(self.municipio) > 100:
             return Result.err(f"Entidade inválida: municipio excede 100 caracteres (recebido: {len(self.municipio)})")
         return Result.ok(self)

    def _validate_cnpj(self) -> Result["Entidade"]:
        if self.cnpj and len(self.cnpj) > 20:
             return Result.err(f"Entidade inválida: cnpj excede 20 caracteres (recebido: {len(self.cnpj)})")
        return Result.ok(self)

    @staticmethod
    def from_row(row: dict) -> Result["Entidade"]:
        try:
            ent = Entidade(
                id_entidade=int(row["id_entidade"]) if row.get("id_entidade") else None,
                nome=row.get("nome"),
                estado=row.get("estado"),
                municipio=row.get("municipio"),
                cnpj=row.get("cnpj")
            )
            return ent.validate()
        except Exception as e:
            return Result.err(f"Erro ao instanciar Entidade: {str(e)}")

    @staticmethod
    def _fetch_raw(id_entidade: int) -> Result[tuple]:
        """Busca o dado cru no banco e retorna (row, description)."""
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM entidade WHERE id_entidade = %s", (id_entidade,))
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
             return Result.err("Entidade não encontrada.")
        
        columns = [desc[0] for desc in description]
        return Result.ok(dict(zip(columns, row))) ##tuple  --> dictionary

    @staticmethod
    def get_by_id(id_entidade: int) -> Result["Entidade"]:
        return (
            Entidade._fetch_raw(id_entidade)
            .bind(Entidade._validate_db_return)
            .bind(Entidade.from_row)
        )
