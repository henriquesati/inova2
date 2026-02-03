
import unittest
import sys
import os
from unittest.mock import MagicMock, patch
from dataclasses import dataclass
from decimal import Decimal
from datetime import date

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction

class TestLiquidacaoTransaction(unittest.TestCase):

    def setUp(self):
        # Sample Data Types
        self.empenho = Empenho(
            id_empenho="EMP-1", ano=2024, data_empenho=date.today(),
            cpf_cnpj_credor="123", credor="Test", valor=Decimal("100.00"), id_entidade=1
        )
        self.liquidacao = LiquidacaoNotaFiscal(
            id_liquidacao_empenhonotafiscal=5001, chave_danfe="KEY-123",
            data_emissao=date.today(), valor=Decimal("100.00"), id_empenho="EMP-1"
        )
        self.nfe = Nfe(
            id=10, chave_nfe="KEY-123", numero_nfe="123", data_hora_emissao=None,
            cnpj_emitente="000", valor_total_nfe=Decimal("100.00")
        )

    def test_build_success(self):
        """
        Test successful construction of LiquidacaoTransaction.
        """
        empenho_res = Result.ok(self.empenho)

        with patch("models.liquidacao_nota_fiscal.LiquidacaoNotaFiscal.get_by_FK_id_empenho") as mock_get_liq, \
             patch("models.nfe.Nfe.get_by_chave_nfe") as mock_get_nfe:
            
            mock_get_liq.return_value = Result.ok(self.liquidacao)
            mock_get_nfe.return_value = Result.ok(self.nfe)

            tx_res = LiquidacaoTransaction.build_from_empenho_result(empenho_res)

            if tx_res.is_err:
                self.fail(f"Transaction build failed: {tx_res.error}")
            
            tx = tx_res.value
            self.assertEqual(tx.empenho, self.empenho)
            self.assertEqual(tx.liquidacao, self.liquidacao)
            self.assertEqual(tx.nfe, self.nfe)

    def test_build_failures_parameterized(self):
        """
        Parameterized test for various failure scenarios during build.
        """
        # (case_name, empenho_res, mock_liq_res, mock_nfe_res, expected_error)
        test_cases = [
            (
                "Liquidacao Not Found",
                Result.ok(self.empenho),
                Result.err("Liquidacao not found"),
                None, # Nfe fetch shouldn't allow
                "Liquidacao not found"
            ),
            (
                "Nfe Not Found",
                Result.ok(self.empenho),
                Result.ok(self.liquidacao),
                Result.err("Nfe not found"),
                "Nfe not found"
            ),
            (
                "Initial Empenho Error",
                Result.err("Empenho error"),
                None, # Liquidacao fetch shouldn't allow
                None,
                "Empenho error"
            )
        ]

        for name, emp_res, liq_res, nfe_res, expected_err in test_cases:
            with self.subTest(case=name):
                with patch("models.liquidacao_nota_fiscal.LiquidacaoNotaFiscal.get_by_FK_id_empenho") as mock_get_liq, \
                     patch("models.nfe.Nfe.get_by_chave_nfe") as mock_get_nfe:
                    
                    if liq_res: mock_get_liq.return_value = liq_res
                    if nfe_res: mock_get_nfe.return_value = nfe_res

                    tx_res = LiquidacaoTransaction.build_from_empenho_result(emp_res)

                    self.assertTrue(tx_res.is_err, f"Expected error for {name}")
                    self.assertEqual(tx_res.error, expected_err)


if __name__ == "__main__":
    unittest.main()
