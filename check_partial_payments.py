
import sys
import os
from db_connection import get_db_connection

def check_multiple_payments():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print('Checking for NFes with multiple payments...')
    cursor.execute('''
        SELECT chave_nfe, COUNT(*) as qtd, SUM(valor_pagamento) as total_pago
        FROM nfe_pagamento
        GROUP BY chave_nfe
        HAVING COUNT(*) > 1
        LIMIT 5;
    ''')
    
    rows = cursor.fetchall()
    if not rows:
        print('No examples found of a single NFe having multiple payment records.')
    else:
        print(f'Found {len(rows)} examples. Showing first few:')
        for r in rows:
            print(f' - NFe {r[0]}: {r[1]} payments, Total: {r[2]}')

    conn.close()

if __name__ == '__main__':
    check_multiple_payments()

