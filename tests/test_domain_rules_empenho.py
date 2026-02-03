import unittest
import sys
import os

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)
    sys.path.append(os.path.join(project_root, 'inova')) # Just in case
    
# Adjust project root calculation if needed, simpler is usually better:
# Assuming tests/test_domain_rules_empenho.py is in /home/pedri/Desktop/inova/tests
# project root is /home/pedri/Desktop/inova
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from datetime import date
from decimal import Decimal
from result import Result
from models.contrato import Contrato
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from models.empenho import Empenho
from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.domains.empenho import (
    regra_fornecedor_consistente,
    regra_entidade_consistente,
    regra_empenhos_do_mesmo_contrato,
    regra_empenhos_unicos,
    regra_valor_total_empenhado,
    regra_temporal_empenho,
    executar_empenho_rules
)

class TestEmpenhoDomainRules(unittest.TestCase):
    def setUp(self):
        # Base Helpers
        self.ent = Entidade(id_entidade=1, nome="Entidade A", estado="SP", municipio="SP", cnpj="0001")
        self.forn = Fornecedor(id_fornecedor=10, nome="Fornecedor X", documento="12345678000199")
        self.contrato = Contrato(
            id_contrato=100,
            valor=Decimal("1000.00"),
            data=date(2024, 1, 1),
            objeto="Objeto",
            id_entidade=1,
            id_fornecedor=10
        )
        # Base Empenho (Valid)
        self.empenho_base = Empenho(
            id_empenho="EMP-001",
            ano=2024,
            data_empenho=date(2024, 1, 2),
            cpf_cnpj_credor="12345678000199", # Matches Fornecedor
            credor="Fornecedor X",
            valor=Decimal("500.00"),
            id_entidade=1, # Matches Entidade
            id_contrato=100 # Matches Contrato
        )
    
    def _make_ctx(self, empenhos=None):
        if empenhos is None:
            empenhos = [self.empenho_base]
        return EmpenhoTransaction(
            entidade=self.ent,
            fornecedor=self.forn,
            contrato=self.contrato,
            empenhos=empenhos
        )

    def test_fornecedor_consistente(self):
        # 1. Success
        ctx = self._make_ctx()
        self.assertTrue(regra_fornecedor_consistente(ctx).is_ok)

        # 2. Failure: Mismatch Documento
        bad_emp = self.empenho_base
        bad_emp.cpf_cnpj_credor = "99999999000199" # Mismatch
        ctx_fail = self._make_ctx([bad_emp])
        
        res = regra_fornecedor_consistente(ctx_fail)
        self.assertTrue(res.is_err)
        self.assertIn("Documento do credor", res.error)

    def test_entidade_consistente(self):
        # 1. Success
        ctx = self._make_ctx()
        self.assertTrue(regra_entidade_consistente(ctx).is_ok)
        
        # 2. Failure: Mismatch Entidade ID
        bad_emp = self.empenho_base
        bad_emp.id_entidade = 2 # Mismatch
        ctx_fail = self._make_ctx([bad_emp])
        
        res = regra_entidade_consistente(ctx_fail)
        self.assertTrue(res.is_err)
        self.assertIn("pertence a entidade diferente", res.error)

    def test_contrato_consistente(self):
        # 1. Success
        ctx = self._make_ctx()
        self.assertTrue(regra_empenhos_do_mesmo_contrato(ctx).is_ok)

        # 2. Failure: Mismatch Contrato ID
        # Note: EmpenhoTransaction.__post_init__ has an assertion for this.
        # We must verify that EITHER the constructor fails (invariant) OR the rule fails.
        bad_emp = self.empenho_base
        bad_emp.id_contrato = 999
        
        try:
            ctx_fail = self._make_ctx([bad_emp])
            # If constructor succeeds, rule MUST fail
            res = regra_empenhos_do_mesmo_contrato(ctx_fail)
            self.assertTrue(res.is_err)
            self.assertIn("não pertence ao contrato", res.error)
        except AssertionError:
            # If constructor fails, invariant is protected
            pass

    def test_empenhos_unicos(self):
        # 1. Success
        emp1 = self.empenho_base
        emp2 = Empenho(
            id_empenho="EMP-002", # Unique
            ano=2024, data_empenho=date(2024,1,3), 
            cpf_cnpj_credor="12345678000199", credor="F", 
            valor=Decimal("10.00"), id_entidade=1, id_contrato=100
        )
        ctx = self._make_ctx([emp1, emp2])
        self.assertTrue(regra_empenhos_unicos(ctx).is_ok)

        # 2. Failure: Duplicate ID
        emp_dup = emp1 
        ctx_fail = self._make_ctx([emp1, emp_dup])
        
        res = regra_empenhos_unicos(ctx_fail)
        self.assertTrue(res.is_err)
        # Use lowercase to match "Empenhos duplicados no agregado"
        self.assertIn("duplicados", res.error)

    def test_valor_total_limit(self):
        # 1. Success (500 <= 1000)
        ctx = self._make_ctx()
        self.assertTrue(regra_valor_total_empenhado(ctx).is_ok)

        # 2. Failure (1001 > 1000)
        bad_emp = self.empenho_base
        bad_emp.valor = Decimal("1001.00")
        ctx_fail = self._make_ctx([bad_emp])
        
        res = regra_valor_total_empenhado(ctx_fail)
        self.assertTrue(res.is_err)
        self.assertIn("excede valor do contrato", res.error)

    def test_temporal_order(self):
        # 1. Success (2024-01-02 >= 2024-01-01)
        ctx = self._make_ctx()
        self.assertTrue(regra_temporal_empenho(ctx).is_ok)

        # 2. Failure (2023-12-31 < 2024-01-01)
        bad_emp = self.empenho_base
        bad_emp.data_empenho = date(2023, 12, 31)
        ctx_fail = self._make_ctx([bad_emp])
        
        res = regra_temporal_empenho(ctx_fail)
        self.assertTrue(res.is_err)
        self.assertIn("anterior à data do contrato", res.error)

    def test_all_rules_execution(self):
        # 1. Success: All rules pass
        ctx = self._make_ctx()
        res = executar_empenho_rules(ctx)
        self.assertTrue(res.is_ok)

        # 2. Ensure new rules are integrated in execution flow
        # Fail one rule (e.g. temporal)
        bad_emp = self.empenho_base
        bad_emp.data_empenho = date(1999, 1, 1)
        ctx_fail = self._make_ctx([bad_emp])
        
        res = executar_empenho_rules(ctx_fail)
        self.assertTrue(res.is_err)
        self.assertIn("anterior à data", res.error)

if __name__ == "__main__":
    unittest.main()
