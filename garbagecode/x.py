import sys
import os
from db_connection import get_db_connection

"""
Este script verifica, via information_schema, se existem constraints
(UNIQUE ou PRIMARY KEY) que garantem relações 1–1 entre entidades do domínio.

Foco:
- Empenho -> Liquidação
- Empenho -> Pagamento
- NFe -> NFePagamento
- Pagamento -> NFePagamento
"""

RELATION_CHECKS = [
    {
        "label": "Empenho -> Liquidação",
        "table": "liquidacao_nota_fiscal",
        "column": "id_empenho"
    },
    {
        "label": "Empenho -> Pagamento",
        "table": "pagamento",
        "column": "id_empenho"
    },
    {
        "label": "NFe -> NFePagamento",
        "table": "nfe_pagamento",
        "column": "chave_nfe"
    },
    {
        "label": "Pagamento -> NFePagamento",
        "table": "nfe_pagamento",
        "column": "id_pagamento"
    },
]

def check_constraints(cursor, table: str, column: str):
    """
    Retorna todas as constraints associadas a uma coluna específica.
    """
    cursor.execute("""
        SELECT
            tc.constraint_name,
            tc.constraint_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s
          AND kcu.column_name = %s;
    """, (table, column))

    return cursor.fetchall()


def analyze_relation(label: str, constraints):
    """
    Analisa se a relação é estruturalmente 1–1 ou 1–N.
    """
    has_unique = any(c[1] in ("UNIQUE", "PRIMARY KEY") for c in constraints)
    has_fk = any(c[1] == "FOREIGN KEY" for c in constraints)

    print(f"\n{label}")
    print("-" * len(label))

    if not constraints:
        print("❌ Nenhuma constraint encontrada")
        print("➡️ Relação NÃO é garantida nem como FK")
        return

    for name, ctype in constraints:
        print(f"- {ctype}: {name}")

    if has_unique and has_fk:
        print("✅ Relação estruturalmente 1–1 (FK + UNIQUE)")
    elif has_fk:
        print("⚠️ Relação 1–N (apenas FOREIGN KEY)")
    else:
        print("❌ Não há garantia relacional válida")


def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("Verificando constraints estruturais do domínio...\n")

        for check in RELATION_CHECKS:
            constraints = check_constraints(
                cursor,
                check["table"],
                check["column"]
            )
            analyze_relation(check["label"], constraints)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
