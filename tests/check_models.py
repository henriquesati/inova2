from models.contrato import Contrato
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from models.pagamento import Pagamento
from models.nfe_pagamento import NfePagamento
from models.fornecedor import Fornecedor
from models.entidade import Entidade
from datetime import date, datetime
from decimal import Decimal

# Helper function to print test results
def test_model(model_name, instance):
    print(f"[PASS] {model_name} created successfully: {instance}")

try:
    # Test Contrato
    c = Contrato(1, Decimal("1000.00"), date(2023, 1, 1), "Test Object", 1, 1)
    test_model("Contrato", c)

    # Test Empenho
    e = Empenho("EMP001", 2023, date(2023, 2, 1), "12345678000199", "Credor X", Decimal("500.00"), 1, 1)
    test_model("Empenho", e)

    # Test LiquidacaoNotaFiscal
    l = LiquidacaoNotaFiscal(1, "12345678901234567890123456789012345678901234", date(2023, 3, 1), Decimal("500.00"), "EMP001")
    test_model("LiquidacaoNotaFiscal", l)

    # Test Nfe
    n = Nfe(1, "KEY123", "NUM123", datetime(2023, 3, 1, 10, 0, 0), "12345678000199", Decimal("500.00"))
    test_model("Nfe", n)

    # Test Pagamento
    p = Pagamento("PAG001", "EMP001", date(2023, 4, 1), Decimal("500.00"))
    test_model("Pagamento", p)

    # Test NfePagamento
    np = NfePagamento("NP001", "KEY123", "Boleto", Decimal("500.00"))
    test_model("NfePagamento", np)

    # Test Fornecedor
    f = Fornecedor(1, "Fornecedor Y", "12345678000199")
    test_model("Fornecedor", f)

    # Test Entidade
    en = Entidade(1, "Orgao Z", "RJ", "Rio de Janeiro", "98765432000111")
    test_model("Entidade", en)

except Exception as e:
    print(f"[FAIL] Error creating models: {e}")
