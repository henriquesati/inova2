from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional, List
from result import Result
from db_connection import get_db_connection

@dataclass
class Empenho:
    id_empenho: str
    ano: int
    data_empenho: date
    cpf_cnpj_credor: str
    credor: str
    valor: Decimal
    id_entidade: int
    id_contrato: Optional[int] = None

    def get_contrato_FK(self) -> Result["Contrato"]: 
        from models.contrato import Contrato
        if not self.id_contrato:
            return Result.err("Empenho não possui contrato vinculado.")
        return Contrato.get_by_id(self.id_contrato)

    def get_entidade_FK(self) -> Result["Entidade"]: 
        from models.entidade import Entidade
        return self.get_contrato_FK().bind(lambda c: c.get_entidade_FK())

    def get_fornecedor_FK(self) -> Result["Fornecedor"]: 
        from models.fornecedor import Fornecedor
        return self.get_contrato_FK().bind(lambda c: c.get_fornecedor_FK())

    @staticmethod
    def from_row(row: dict) -> Result["Empenho"]:
        try:
             # Basic instantiation - validation could be added later similar to other models
            empenho = Empenho(
                id_empenho=row["id_empenho"],
                ano=row["ano"],
                data_empenho=row["data_empenho"],
                cpf_cnpj_credor=row.get("cpf_cnpj_credor") or row.get("cpfcnpjcredor"),
                credor=row["credor"],
                valor=row["valor"], 
                id_entidade=row["id_entidade"],
                id_contrato=row.get("id_contrato")
            )
            return Result.ok(empenho)
        except Exception as e:
            return Result.err(f"Erro ao instanciar Empenho: {str(e)}")
##talvez haja uma redudancia entre dict(zip) e a função from_row(), analisar se hovuer tempo
    @staticmethod
    def get_by_contract_id(id_contrato: int) -> Result[List["Empenho"]]:
        """Busca TODOS os empenhos vinculados a um contrato."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM empenho WHERE id_contrato = %s", (id_contrato,))
            rows = cursor.fetchall()
            
            # Captura a description antes de fechar, embora fetchall traga os dados
            description = cursor.description
            
            cursor.close()
            conn.close()

            if not rows:
                 return Result.ok([])

            columns = [desc[0] for desc in description]
            
            empenhos = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                res = Empenho.from_row(row_dict)
                if res.is_err:
                    return Result.err(f"Erro ao converter linha de empenho: {res.error}")
                empenhos.append(res.value)
            
            return Result.ok(empenhos)
            
        except Exception as e:
            return Result.err(f"Erro ao buscar Empenhos por contrato: {str(e)}")
            
        except Exception as e:
            return Result.err(f"Erro ao buscar Empenhos por contrato: {str(e)}")
