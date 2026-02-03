from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional, List
from result import Result
from db_connection import get_db_connection

@dataclass
class Contrato:
    id_contrato: int
    valor: Decimal
    data: date
    objeto: str
    id_entidade: int
    id_fornecedor: int

    
    def _validate_id(self) -> Result["Contrato"]:
        if not self.id_contrato:
            return Result.err("Contrato inválido: id_contrato é obrigatório")
        return Result.ok(self)

    def _validate_fks(self) -> Result["Contrato"]:
        if not isinstance(self.id_entidade, int):
             return Result.err(f"Contrato inválido: id_entidade deve ser inteiro (recebido: {type(self.id_entidade)})")
        if not isinstance(self.id_fornecedor, int):
             return Result.err(f"Contrato inválido: id_fornecedor deve ser inteiro (recebido: {type(self.id_fornecedor)})")
        return Result.ok(self)

    def _validate_objeto(self) -> Result["Contrato"]:
        if self.objeto and len(self.objeto) > 255:
            return Result.err(f"Contrato inválido: objeto excede 255 caracteres (recebido: {len(self.objeto)})")
        return Result.ok(self)

    def _validate_valor(self) -> Result["Contrato"]:
        if self.valor is None:
            return Result.err("Contrato inválido: valor é obrigatório")
        
        # Tentativa de conversão segura
        if not isinstance(self.valor, Decimal):
            try:
                self.valor = Decimal(self.valor)
            except (InvalidOperation, TypeError):
                 return Result.err(f"Contrato inválido: valor '{self.valor}' não é um Decimal válido")

        # Numeric(15,2) -> Máximo: 9.999.999.999.999,99
        MAX_VALOR = Decimal("9999999999999.99")
        
        if self.valor > MAX_VALOR:
             return Result.err(f"Contrato inválido: valor excede limite de Numeric(15,2) ({MAX_VALOR})")
        
        return Result.ok(self)

    @staticmethod
    def create(row: dict) -> Result["Contrato"]:
        """
        Factory que substitui o construtor direto e o from_row antigo.
        Retorna um Result que contém o Contrato validado ou uma string de erro.
        """
        try:
            contrato = Contrato(
                id_contrato=int(row["id_contrato"]) if row.get("id_contrato") else None, # type: ignore
                valor=row.get("valor"), # type: ignore (será convertido no validate_valor)
                data=row.get("data"),   # type: ignore
                objeto=row.get("objeto"), # type: ignore
                id_entidade=row.get("id_entidade"), # type: ignore
                id_fornecedor=row.get("id_fornecedor") # type: ignore
            )
        except Exception as e:
            return Result.err(f"Erro estrutural ao criar objeto: {str(e)}")

        # Pipeline de Validação (Railway Oriented Programming)
        # O Result.ok inicia o trilho de sucesso.
        # Cada .bind executa a próxima validação APENAS se a anterior foi sucesso.
        return (
            Result.ok(contrato)
            .bind(lambda c: c._validate_id())
            .bind(lambda c: c._validate_fks())
            .bind(lambda c: c._validate_objeto())
            .bind(lambda c: c._validate_valor())
        )

    @staticmethod
    def _fetch_raw(id_contrato: int) -> Result[tuple]:
        """Busca o dado cru no banco e retorna (row, description)."""
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM contrato WHERE id_contrato = %s", (id_contrato,))
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
             return Result.err("Contrato não encontrado.")
        
        columns = [desc[0] for desc in description]
        return Result.ok(dict(zip(columns, row)))

    @staticmethod
    def get_by_id(id_contrato: int) -> Result["Contrato"]:
        """Pipeline declarativo: Fetch -> Validate -> Map"""
        return (
            Contrato._fetch_raw(id_contrato)
            .bind(Contrato._validate_db_return)
            .bind(Contrato.create)
        )

    def get_fornecedor_FK(self) -> Result["Fornecedor"]: 
        from models.fornecedor import Fornecedor
        return Fornecedor.get_by_id(self.id_fornecedor)

    def get_entidade_FK(self) -> Result["Entidade"]: 
        from models.entidade import Entidade
        return Entidade.get_by_id(self.id_entidade)

    def get_empenhos_FK(self) -> Result[List["Empenho"]]: # type: ignore
        from models.empenho import Empenho
        return Empenho.get_by_contract_id(self.id_contrato)