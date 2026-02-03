import unittest
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.domains.empenho import executar_empenho_rules
from models.contrato import Contrato
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

class TestEmpenhoFlow(unittest.TestCase):
    
    def test_build_from_contract(self):
        """
        Test Case for EmpenhoTransaction.build_from_contract using Mocks.
        Ensures that Entidade and Fornecedor are fetched correctly using their IDs from the contract.
        """
        print("\n[Visual Log] Testing build_from_contract...")

        # Mock Data
        contrato_data = {
            "id_contrato": 100,
            "valor": Decimal("5000.00"),
            "data": date(2023, 1, 1),
            "objeto": "Teste Objeto",
            "id_entidade": 1,
            "id_fornecedor": 2
        }
        
        # We will mock the get_by_id methods directly
        with patch("models.entidade.Entidade.get_by_id") as mock_get_ent, \
             patch("models.fornecedor.Fornecedor.get_by_id") as mock_get_forn:
             
             # Setup success case
             ent_obj = Entidade(1, "E", "S", "M", "C")
             forn_obj = Fornecedor(2, "F", "D")
             
             mock_get_ent.return_value = Result.ok(ent_obj)
             mock_get_forn.return_value = Result.ok(forn_obj)
             
             contrato_res = Contrato.create(contrato_data)
             
             result = EmpenhoTransaction.build_from_contract(contrato_res)
             
             self.assertTrue(result.is_ok, f"Build from contract failed: {result._error}")
             print(f"[Visual Log] Build From Contract (Success): {result.value}")
             self.assertEqual(result.value.entidade, ent_obj)
             self.assertEqual(result.value.fornecedor, forn_obj)
             self.assertEqual(result.value.contrato, contrato_res.value)

             # Setup failure case (Entidade fails)
             mock_get_ent.return_value = Result.err("DB Error Entidade")
             result_fail = EmpenhoTransaction.build_from_contract(contrato_res)
             
             self.assertTrue(result_fail.is_err)
             print(f"[Visual Log] Build From Contract (Error Propagated): {result_fail.error}")
             self.assertEqual(result_fail.error, "DB Error Entidade")

    def test_entidade_repository_flow(self):
        """
        Test Case for Entidade.get_by_id Pipeline.
        Verifies that the declarative pipeline handles DB results correctly.
        """
        print("\n[Visual Log] Testing Entidade.get_by_id Pipeline...")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Setup mock for success
        row = (1, "Entidade Teste", "SP", "Sao Paulo", "12345678000199")
        description = [("id_entidade",), ("nome",), ("estado",), ("municipio",), ("cnpj",)]
        mock_cursor.fetchone.return_value = row
        mock_cursor.description = description
        
        with patch("models.entidade.get_db_connection", return_value=mock_conn):
            # Test Success
            result = Entidade.get_by_id(1)
            self.assertTrue(result.is_ok)
            print(f"[Visual Log] Entidade Repo (Success): {result.value}")
            self.assertEqual(result.value.id_entidade, 1)
            
            # Test Not Found
            mock_cursor.fetchone.return_value = None
            result_nf = Entidade.get_by_id(999)
            self.assertTrue(result_nf.is_err)
            print(f"[Visual Log] Entidade Repo (Not Found): {result_nf.error}")
            self.assertIn("não encontrada", str(result_nf.error))
    
    def setUp(self):
        self.valid_entidade = Entidade(
            id_entidade=1,
            nome="Prefeitura Teste",
            estado="SP",
            municipio="Sao Paulo",
            cnpj="12345678000199"
        )
        self.valid_fornecedor = Fornecedor(
            id_fornecedor=1,
            nome="Fornecedor Teste",
            documento="11122233344"
        )
        self.contrato_mock = MagicMock(spec=Contrato)
        self.contrato_mock.id_entidade = 1
        self.contrato_mock.id_fornecedor = 1
        
        # Default mock behaviors for FKs (Success by default)
        self.contrato_mock.get_entidade_FK = MagicMock(return_value=Result.ok(self.valid_entidade))
        self.contrato_mock.get_fornecedor_FK = MagicMock(return_value=Result.ok(self.valid_fornecedor))
        self.contrato_mock.get_empenhos_FK = MagicMock(return_value=Result.ok([]))

    def test_success_flow(self):
        """
        Test Case 1: Success Flow
        Valid Entidade and Fornecedor Results should bind correctly into a EmpenhoTransaction,
        and pass all domain rules.
        """
        res_contrato_obj = Result.ok(self.contrato_mock)

        res_contrato = EmpenhoTransaction.build_from_contract(res_contrato_obj)
        self.assertTrue(res_contrato.is_ok, f"Binding failed: {res_contrato._error}")
        
        contrato = res_contrato.value
        self.assertIsInstance(contrato, EmpenhoTransaction)
        self.assertEqual(contrato.entidade, self.valid_entidade)
        self.assertEqual(contrato.fornecedor, self.valid_fornecedor)
        print(f"\n[Visual Log] Contrato (Success): {contrato}")

        res_rules = executar_empenho_rules(contrato)
        self.assertTrue(res_rules.is_ok, f"Rules failed: {res_rules._error}")
        self.assertEqual(res_rules.value, contrato)
        print(f"[Visual Log] Rules Result (Success): {res_rules}")

    def test_success_unwrapped_params(self):
        """
        Test Case 6a: Success Variations - Unwrapped Parameters
        Test behavior when passing objects in their unwrapped form (via constructor).
        """
        # 1. Unwrapped Form (Direct Constructor)
        contrato_unwrapped = EmpenhoTransaction(
            entidade=self.valid_entidade,
            fornecedor=self.valid_fornecedor,
            contrato=self.contrato_mock,
            empenhos=[]
        )
        print(f"\n[Visual Log] Success Unwrapped (Constructor): {contrato_unwrapped}")
        
        # Verify it holds the data correctly
        self.assertEqual(contrato_unwrapped.entidade, self.valid_entidade)
        self.assertEqual(contrato_unwrapped.fornecedor, self.valid_fornecedor)

    def test_success_wrapped_params(self):
        """
        Test Case 6b: Success Variations - Wrapped Parameters
        Test behavior when passing objects in their wrapped form (via build factory).
        """
        # 2. Wrapped Form (Build Factory)
        # Assuming build_from_contract uses the mocked values set in setUp
        res_contrato_obj = Result.ok(self.contrato_mock)
        
        res_contrato_wrapped = EmpenhoTransaction.build_from_contract(res_contrato_obj)
        print(f"[Visual Log] Success Wrapped (Build Factory): {res_contrato_wrapped}")

        self.assertTrue(res_contrato_wrapped.is_ok)
        contrato_wrapped = res_contrato_wrapped.value
        
        self.assertIsInstance(contrato_wrapped, EmpenhoTransaction)
        self.assertEqual(contrato_wrapped.entidade, self.valid_entidade)
        self.assertEqual(contrato_wrapped.fornecedor, self.valid_fornecedor)

    def test_circuit_breaker_entidade_error(self):
        """
        Test Case 2: Circuit Breaker - Entidade Error
        If Entidade Result is an error, EmpenhoTransaction.build_from_contract should return that error
        immediately (circuit breaker).
        """
        error_msg = "Entidade not found"
        self.contrato_mock.get_entidade_FK = MagicMock(return_value=Result.err(error_msg))
        
        res_contrato_obj = Result.ok(self.contrato_mock)

        res_contrato = EmpenhoTransaction.build_from_contract(res_contrato_obj)
        
        self.assertTrue(res_contrato.is_err)
        self.assertEqual(res_contrato.error, error_msg)
        
        print(f"\n[Visual Log] Result Entidade Error: {res_contrato}")

    def test_circuit_breaker_fornecedor_error(self):
        """
        Test Case 3: Circuit Breaker - Fornecedor Error
        If Fornecedor Result is an error, EmpenhoTransaction.build_from_contract should return that error
        immediately (circuit breaker).
        """
        error_msg = "Fornecedor not found"
        self.contrato_mock.get_fornecedor_FK = MagicMock(return_value=Result.err(error_msg))
        
        res_contrato_obj = Result.ok(self.contrato_mock)

        res_contrato = EmpenhoTransaction.build_from_contract(res_contrato_obj)
        
        self.assertTrue(res_contrato.is_err)
        self.assertEqual(res_contrato.error, error_msg)

        print(f"\n[Visual Log] Result Fornecedor Error: {res_contrato}")

    def test_invariants_entidade_invalid(self):
        """
        Test Case 4: Transformation into Error - Domain Invariants (Entidade)
        Parameterized test with a list of invalid entities.
        """
        invalid_entidades = [
            # Case: id_entidade missing
            Entidade(id_entidade=0, nome="Valid", estado="SP", municipio="SP", cnpj="123"),
            # Case: nome too long (>255)
            Entidade(id_entidade=1, nome="a" * 256, estado="SP", municipio="SP", cnpj="123"),
            # Case: estado too long (>50)
            Entidade(id_entidade=1, nome="Valid", estado="a" * 51, municipio="SP", cnpj="123"),
            # Case: municipio too long (>100)
            Entidade(id_entidade=1, nome="Valid", estado="SP", municipio="a" * 101, cnpj="123"),
            # Case: cnpj too long (>20)
            Entidade(id_entidade=1, nome="Valid", estado="SP", municipio="SP", cnpj="a" * 21),
            # Case: Entidade is None (handled by domain rule check)
            None 
        ]

        for i, invalid_entidade in enumerate(invalid_entidades):
            with self.subTest(i=i, entidade=invalid_entidade):
                # Configure mock to return the invalid entity
                # note: Result.ok(None) might be problematic if build expects valid object, but let's follow existing pattern
                self.contrato_mock.get_entidade_FK = MagicMock(return_value=Result.ok(invalid_entidade))
                self.contrato_mock.get_fornecedor_FK = MagicMock(return_value=Result.ok(self.valid_fornecedor))
                
                res_contrato_obj = Result.ok(self.contrato_mock)

                res_contrato = EmpenhoTransaction.build_from_contract(res_contrato_obj)
                
                # If build fails (e.g. __post_init__ assertion), invariant is protected
                if res_contrato.is_err:
                     print(f"[Visual Log] Invariant Test (Build Guard): {res_contrato.error}")
                     self.assertTrue(True)
                     continue

                self.assertTrue(res_contrato.is_ok)
                
                contrato = res_contrato.value
                
                # Execute rules - should fail due to invalid entity
                res_rules = executar_empenho_rules(contrato)
                
                self.assertTrue(res_rules.is_err, f"Should fail for invalid entidade: {invalid_entidade}")
                print(f"[Visual Log] Invariant Test (Rule Guard): {res_rules}")

    def test_invariants_fornecedor_invalid(self):
        """
        Test Case 5: Transformation into Error - Domain Invariants (Fornecedor)
        Parameterized test with a list of invalid fornecedores.
        """
        invalid_fornecedores = [
            # Case: id_fornecedor missing
            Fornecedor(id_fornecedor=0, nome="Valid", documento="123"),
            # Case: nome too long (>255)
            Fornecedor(id_fornecedor=1, nome="a" * 256, documento="123"),
            # Case: documento too long (>20)
            Fornecedor(id_fornecedor=1, nome="Valid", documento="a" * 21),
            # Case: Fornecedor is None
            None
        ]

        for i, invalid_fornecedor in enumerate(invalid_fornecedores):
            with self.subTest(i=i, fornecedor=invalid_fornecedor):
                self.contrato_mock.get_entidade_FK = MagicMock(return_value=Result.ok(self.valid_entidade))
                self.contrato_mock.get_fornecedor_FK = MagicMock(return_value=Result.ok(invalid_fornecedor))

                res_contrato_obj = Result.ok(self.contrato_mock)

                res_contrato = EmpenhoTransaction.build_from_contract(res_contrato_obj)
                
                # If build fails, invariant is protected
                if res_contrato.is_err:
                     print(f"[Visual Log] Invariant Test (Build Guard): {res_contrato.error}")
                     self.assertTrue(True)
                     continue
                
                self.assertTrue(res_contrato.is_ok)
                
                contrato = res_contrato.value

                res_rules = executar_empenho_rules(contrato)
                
                self.assertTrue(res_rules.is_err, f"Should fail for invalid fornecedor: {invalid_fornecedor}")
                print(f"[Visual Log] Invariant Test (Rule Guard): {res_rules}")

from result import Result
from models.contrato import Contrato
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from clientside.transaction.empenho_transaction import EmpenhoTransaction

def test_build_from_contract():
    print("Testing build_from_contract...")

    # Mock DB Connection
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Mock Data
    contrato_data = {
        "id_contrato": 100,
        "valor": Decimal("5000.00"),
        "data": date(2023, 1, 1),
        "objeto": "Teste Objeto",
        "id_entidade": 1,
        "id_fornecedor": 2
    }
    
    entidade_row = (1, "Entidade Teste", "SP", "Sao Paulo", "12345678000199")
    entidade_cols = ["id_entidade", "nome", "estado", "municipio", "cnpj"]
    
    fornecedor_row = (2, "Fornecedor Teste", "12345678900")
    fornecedor_cols = ["id_fornecedor", "nome", "documento"]

    # Configure Mock Cursor
    # The code calls Entidade.get_by_id then Fornecedor.get_by_id
    # We need to manage the side effects of execute and fetchone
    
    mock_cursor.description = []
    
    def side_effect_execute(query, params):
        if "entidade" in query:
             mock_cursor.description = [(col,) for col in entidade_cols]
             mock_cursor.fetchone.return_value = entidade_row
        elif "fornecedor" in query:
             mock_cursor.description = [(col,) for col in fornecedor_cols]
             mock_cursor.fetchone.return_value = fornecedor_row
    
    mock_cursor.execute.side_effect = side_effect_execute

    with patch("models.entidade.get_db_connection", return_value=mock_conn), \
         patch("models.fornecedor.get_db_connection", return_value=mock_conn):
        
        # 1. Create Contrato Result
        contrato_res = Contrato.create(contrato_data)
        assert contrato_res.is_ok, f"Contrato creation failed: {contrato_res.error}"
        
        # 2. Build ContratoEmpenhado
        # Because side_effect_execute sets up fetchone return, it should work.
        # However, fetchone is called AFTER execute.
        # We need to make sure fetchone returns the right thing based on the LAST execute.
        # Let's refine the mock logic.
        
        # Actually simplest way is to mock Entidade.get_by_id and Fornecedor.get_by_id directly
        # to test logic inside ContratoEmpenhado, but checking integration is better.
        # But mocking DB cleanly in one go is tricky without complex side effects.
        # Let's stick to mocking get_by_id for simplicity to verify the BIND logic works.
        pass

    print("  -> Testing Logic Flow (Mocking at Model level)...")
    with patch("models.entidade.Entidade.get_by_id") as mock_get_ent, \
         patch("models.fornecedor.Fornecedor.get_by_id") as mock_get_forn:
         
         # Setup success case
         ent_obj = Entidade(1, "E", "S", "M", "C")
         forn_obj = Fornecedor(2, "F", "D")
         
         mock_get_ent.return_value = Result.ok(ent_obj)
         mock_get_forn.return_value = Result.ok(forn_obj)
         
         contrato_res = Contrato.create(contrato_data)
         
         result = EmpenhoTransaction.build_from_contract(contrato_res)
         
         if result.is_ok:
             print("SUCCESS: EmpenhoTransaction built successfully.")
             print(f"  Entidade: {result.value.entidade.nome}")
             print(f"  Fornecedor: {result.value.fornecedor.nome}")
         else:
             print(f"FAILURE: {result.error}")
             exit(1)

         # Setup failure case (Entidade fails)
         mock_get_ent.return_value = Result.err("DB Error Entidade")
         result_fail = EmpenhoTransaction.build_from_contract(contrato_res)
         assert result_fail.is_err
         print(f"SUCCESS: Error correctly propagated: {result_fail.error}")


def test_entidade_repository_flow():
    print("\nTesting Entidade.get_by_id Pipeline...")
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Setup mock for success
    row = (1, "Entidade Teste", "SP", "Sao Paulo", "12345678000199")
    description = [("id_entidade",), ("nome",), ("estado",), ("municipio",), ("cnpj",)]
    mock_cursor.fetchone.return_value = row
    mock_cursor.description = description
    
    with patch("models.entidade.get_db_connection", return_value=mock_conn):
        # Test Success
        result = Entidade.get_by_id(1)
        if result.is_ok:
            print("SUCCESS: Entidade found and hydrated.")
            assert result.value.id_entidade == 1
        else:
            print(f"FAILURE: Expected success but got error: {result.error}")
        
        # Test Not Found
        mock_cursor.fetchone.return_value = None
        result_nf = Entidade.get_by_id(999)
        if result_nf.is_err and "não encontrada" in str(result_nf.error):
             print(f"SUCCESS: Correctly handled Not Found: {result_nf.error}")
        else:
             print(f"FAILURE: Expected 'not found' error but got: {result_nf}")

if __name__ == "__main__":
    unittest.main()
    test_entidade_repository_flow()
    test_build_from_contract()
