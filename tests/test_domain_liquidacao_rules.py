
import unittest
from decimal import Decimal
from datetime import date
from unittest.mock import MagicMock

# Adjust path
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from models.contrato import Contrato
from models.fornecedor import Fornecedor
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction, ItemLiquidacao
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.domains.liquidação import Valida

class TestLiquidacaoDomainRules(unittest.TestCase):

    def setUp(self):
        # Base Objects
        self.empenho = Empenho(
            id_empenho="EMP-1", ano=2024, data_empenho=date(2024, 1, 1),
            cpf_cnpj_credor="111", credor="Fornecedor Teste", valor=Decimal("1000.00"), id_entidade=1, id_contrato=100
        )
        self.contrato = Contrato(
            id_contrato=100, valor=Decimal("10000.00"), data=date(2023, 1, 1), 
            objeto="Teste", id_entidade=1, id_fornecedor=10
        )
        self.fornecedor = Fornecedor(
            id_fornecedor=10, nome="Fornecedor Teste", documento="111"
        )
        
        # Mocks Transaction Context
        self.emp_tx = MagicMock(spec=EmpenhoTransaction)
        self.emp_tx.empenhos = {"EMP-1": self.empenho}
        self.emp_tx.contrato = self.contrato
        self.emp_tx.fornecedor = self.fornecedor

    def test_partial_liquidation_success(self):
        """
        Cenario: 1 NFe de 1000.00
        Liq 1: 400.00 (OK)
        Liq 2: 600.00 (OK)
        Total Liquidado = 1000.00 == NFe Total (Limite)
        Should PASS.
        """
        nfe = Nfe(
            id=1, chave_nfe="KEY-1000", numero_nfe="1", data_hora_emissao=None,
            cnpj_emitente="111", valor_total_nfe=Decimal("1000.00")
        )
        # NFe Date needs to be consistent (>= Empenho)
        nfe.data_hora_emissao = date(2024, 1, 2)

        liq1 = LiquidacaoNotaFiscal(
            id_liquidacao_empenhonotafiscal=1, chave_danfe="KEY-1000",
            data_emissao=date(2024, 1, 5), valor=Decimal("400.00"), id_empenho="EMP-1"
        )
        liq2 = LiquidacaoNotaFiscal(
            id_liquidacao_empenhonotafiscal=2, chave_danfe="KEY-1000",
            data_emissao=date(2024, 1, 10), valor=Decimal("600.00"), id_empenho="EMP-1"
        )

        items = [
            ItemLiquidacao(liquidacao=liq1, nfe=nfe),
            ItemLiquidacao(liquidacao=liq2, nfe=nfe)
        ]

        tx = LiquidacaoTransaction(empenho_transaction=self.emp_tx, itens_liquidados=items)
        
        # Execute Validation
        res = Valida(tx)
        
        if res.is_err:
            self.fail(f"Valid Partial Liquidation failed unexpectedly: {res.error}")

    def test_mandatory_nfe_failure(self):
        """
        Cenario: Liquidação sem NFe associada (nfe=None).
        Should FAIL (New Mandatory Rule).
        """
        liq1 = LiquidacaoNotaFiscal(
            id_liquidacao_empenhonotafiscal=1, chave_danfe=None, # No Key
            data_emissao=date(2024, 1, 5), valor=Decimal("400.00"), id_empenho="EMP-1"
        )

        items = [
            ItemLiquidacao(liquidacao=liq1, nfe=None) # NFe Missing
        ]

        tx = LiquidacaoTransaction(empenho_transaction=self.emp_tx, itens_liquidados=items)
        
        res = Valida(tx)
        
        self.assertTrue(res.is_err, "Should fail when NFe is missing")
        self.assertIn("sem NFe associada", res.error)

    def test_aggregate_limit_failure(self):
        """
        Cenario: 1 NFe de 1000.00
        Liq 1: 600.00
        Liq 2: 500.00
        """
        self.empenho.valor = Decimal("2000.00") # Ensure Empenho > Sum(Liq) so we test NFe limit specifically
        nfe = Nfe(
            id=1, chave_nfe="KEY-1000", numero_nfe="1", data_hora_emissao=date(2024, 1, 2),
            cnpj_emitente="111", valor_total_nfe=Decimal("1000.00")
        )

        liq1 = LiquidacaoNotaFiscal(
            id_liquidacao_empenhonotafiscal=1, chave_danfe="KEY-1000",
            data_emissao=date(2024, 1, 5), valor=Decimal("600.00"), id_empenho="EMP-1"
        )
        liq2 = LiquidacaoNotaFiscal(
            id_liquidacao_empenhonotafiscal=2, chave_danfe="KEY-1000",
            data_emissao=date(2024, 1, 10), valor=Decimal("500.00"), id_empenho="EMP-1"
        )

        items = [
            ItemLiquidacao(liquidacao=liq1, nfe=nfe),
            ItemLiquidacao(liquidacao=liq2, nfe=nfe)
        ]

        tx = LiquidacaoTransaction(empenho_transaction=self.emp_tx, itens_liquidados=items)
        
        res = Valida(tx)
        
        self.assertTrue(res.is_err, "Should fail when Sum(Liq) > NFe Total")
        self.assertIn("Soma das Liquidações", res.error)
        self.assertIn("excede valor da NFe", res.error)

if __name__ == "__main__":
    unittest.main()
