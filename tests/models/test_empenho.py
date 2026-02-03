import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from datetime import date
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from result import Result
from models.empenho import Empenho

class TestEmpenhoModel(unittest.TestCase):

    def setUp(self):
        self.sample_row = {
            "id_empenho": "2023NE001",
            "ano": 2023,
            "data_empenho": date(2023, 1, 15),
            "cpf_cnpj_credor": "12345678000199",
            "credor": "Fornecedor Teste Ltda",
            "valor": Decimal("1500.50"),
            "id_entidade": 1,
            "id_contrato": 100
        }

    def test_from_row_success(self):
        result = Empenho.from_row(self.sample_row)
        self.assertTrue(result.is_ok)
        empenho = result.value
        self.assertEqual(empenho.id_empenho, "2023NE001")
        self.assertEqual(empenho.valor, Decimal("1500.50"))
        self.assertEqual(empenho.id_contrato, 100)

    @patch('models.empenho.get_db_connection')
    def test_get_by_contract_id_success(self, mock_get_db):
        # Mock DB setup
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock query result
        mock_cursor.description = [
            ('id_empenho',), ('ano',), ('data_empenho',), ('cpf_cnpj_credor',), 
            ('credor',), ('valor',), ('id_entidade',), ('id_contrato',)
        ]
        mock_cursor.fetchall.return_value = [
            ("2023NE001", 2023, date(2023,1,15), "12345678000199", "FAI", Decimal("100"), 1, 100),
            ("2023NE002", 2023, date(2023,2,15), "12345678000199", "FAI", Decimal("200"), 1, 100)
        ]

        # Execute
        result = Empenho.get_by_contract_id(100)

        # Assert
        self.assertTrue(result.is_ok)
        empenhos = result.value
        self.assertEqual(len(empenhos), 2)
        self.assertEqual(empenhos[0].id_empenho, "2023NE001")
        self.assertEqual(empenhos[1].id_empenho, "2023NE002")
        
        # Verify SQL execution
        mock_cursor.execute.assert_called_with(
            "SELECT * FROM empenho WHERE id_contrato = %s", (100,)
        )
        mock_conn.close.assert_called()

    @patch('models.empenho.get_db_connection')
    def test_get_by_contract_id_empty(self, mock_get_db):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [('col',)] # Still need description even if empty often

        result = Empenho.get_by_contract_id(999)
        
        self.assertTrue(result.is_ok)
        self.assertEqual(result.value, [])

    @patch('models.contrato.Contrato.get_by_id')
    def test_get_contrato_FK_success(self, mock_contrato_get):
        # Setup
        empenho = Empenho.from_row(self.sample_row).value
        expected_contrato = MagicMock()
        mock_contrato_get.return_value = Result.ok(expected_contrato)

        # Execute
        result = empenho.get_contrato_FK()

        # Assert
        self.assertTrue(result.is_ok)
        self.assertEqual(result.value, expected_contrato)
        mock_contrato_get.assert_called_with(100)

    def test_get_contrato_FK_no_id(self):
        row_no_contract = self.sample_row.copy()
        row_no_contract['id_contrato'] = None
        empenho = Empenho.from_row(row_no_contract).value

        result = empenho.get_contrato_FK()
        
        self.assertTrue(result.is_err)
        self.assertIn("não possui contrato", result.error)

    @patch('models.empenho.Empenho.get_contrato_FK')
    def test_get_entidade_FK(self, mock_get_contrato_fk):
        # Mocking the chain: Empenho -> Contrato -> Entidade
        mock_contrato = MagicMock()
        mock_entidade = MagicMock()
        
        # Setup mocks
        mock_get_contrato_fk.return_value = Result.ok(mock_contrato)
        mock_contrato.get_entidade_FK.return_value = Result.ok(mock_entidade)

        empenho = Empenho.from_row(self.sample_row).value
        result = empenho.get_entidade_FK()

        self.assertTrue(result.is_ok)
        self.assertEqual(result.value, mock_entidade)
        mock_contrato.get_entidade_FK.assert_called_once()

    @patch('models.empenho.Empenho.get_contrato_FK')
    def test_get_fornecedor_FK(self, mock_get_contrato_fk):
        mock_contrato = MagicMock()
        mock_fornecedor = MagicMock()
        
        mock_get_contrato_fk.return_value = Result.ok(mock_contrato)
        mock_contrato.get_fornecedor_FK.return_value = Result.ok(mock_fornecedor)

        empenho = Empenho.from_row(self.sample_row).value
        result = empenho.get_fornecedor_FK()

        self.assertTrue(result.is_ok)
        self.assertEqual(result.value, mock_fornecedor)
        mock_contrato.get_fornecedor_FK.assert_called_once()

    @patch('models.empenho.get_db_connection')
    def test_get_by_contract_id_parameterized(self, mock_get_db):
        
        scenarios = [
            (
                "Nenhum empenho", 
                [], 
                0, 
                None
            ),
            (
                "Um empenho", 
                [("2023NE01", 2023, date(2023,1,1), "111", "Credor A", Decimal("100"), 1, 10)], 
                1, 
                "2023NE01"
            ),
            (
                "Múltiplos empenhos", 
                [
                    ("2023NE01", 2023, date(2023,1,1), "111", "Credor A", Decimal("100"), 1, 10),
                    ("2023NE02", 2023, date(2023,1,2), "222", "Credor B", Decimal("200"), 1, 10),
                    ("2023NE03", 2023, date(2023,1,3), "333", "Credor C", Decimal("300"), 1, 10)
                ], 
                3, 
                "2023NE03" # Check last one exists
            )
        ]

        for name, rows, expected_count, id_check in scenarios:
            with self.subTest(name=name):
                # Reset Mock for each iteration
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_get_db.return_value = mock_conn
                mock_conn.cursor.return_value = mock_cursor
                
                # Mock Returns
                mock_cursor.fetchall.return_value = rows
                mock_cursor.description = [
                    ('id_empenho',), ('ano',), ('data_empenho',), ('cpf_cnpj_credor',), 
                    ('credor',), ('valor',), ('id_entidade',), ('id_contrato',)
                ]

                # Call
                result = Empenho.get_by_contract_id(10)

                # Assert
                self.assertTrue(result.is_ok, f"Falha no cenário: {name}")
                empenhos = result.value
                self.assertEqual(len(empenhos), expected_count, f"Contagem incorreta no cenário: {name}")
                
                if id_check:
                    # Verify if the checked ID is present in the results
                    ids = [e.id_empenho for e in empenhos]
                    self.assertIn(id_check, ids, f"ID {id_check} não encontrado no cenário: {name}")

if __name__ == '__main__':
    unittest.main()
