import sys
import os
import math
from collections import defaultdict, Counter

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from db_connection import get_db_connection

def get_leading_digit(n):
    """Returns the first non-zero digit of a number."""
    s = str(n).replace('.', '').lstrip('0')
    if not s:
        return None
    return int(s[0])

def benford_analysis():
    print("🕵️  Iniciando Análise de Benford (Forensic Statistics)...")
    print("   ℹ️  Objetivo: Detectar manipulação artificial de valores (Lei de Benford).")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Fetch all payment values > 0
    cursor.execute("SELECT valor FROM pagamento WHERE valor > 0")
    rows = cursor.fetchall()
    
    values = [r[0] for r in rows if r[0] is not None]
    total_samples = len(values)
    
    if total_samples < 50:
        print(f"   ⚠️  Amostra muito pequena ({total_samples}) para significância estatística (Requerido: >50+).")
        conn.close()
        return

    # 1. Calculate Observed Frequencies
    leading_digits = [get_leading_digit(v) for v in values]
    counts = Counter(leading_digits)
    
    # 2. Benford's Expected Frequencies (for digits 1-9)
    # P(d) = log10(1 + 1/d)
    expected_probs = {d: math.log10(1 + 1/d) for d in range(1, 10)}
    
    print(f"\n   📊 Distribuição (Amostra: {total_samples} pagamentos):")
    print("   Digit | Observed % | Expected % | Delta % | Visual")
    print("   " + "-"*60)
    
    max_delta = 0
    suspect_digit = None
    
    for digit in range(1, 10):
        obs_count = counts.get(digit, 0)
        obs_prob = obs_count / total_samples
        exp_prob = expected_probs[digit]
        delta = abs(obs_prob - exp_prob)
        
        # Visual bar
        bar_len = int(obs_prob * 40)
        bar = "█" * bar_len
        
        print(f"     {digit}   |   {obs_prob*100:4.1f}%    |   {exp_prob*100:4.1f}%    |  {delta*100:4.1f}%  | {bar}")
        
        if delta > max_delta:
            max_delta = delta
            suspect_digit = digit

    # 3. Conclusion
    # A Delta > 0.05 (5%) is generally considered significant in simple tests, 
    # though strict Chi-Square would be better for formal analysis.
    print("   " + "-"*60)
    
    threshold = 0.05
    if max_delta > threshold:
        print(f"   ⚠️  ANOMALIA ESTATÍSTICA: O dígito {suspect_digit} desvia {max_delta*100:.1f}% do esperado.")
        print(f"       Isso pode indicar valores gerados artificialmente ou teto de gastos fixo.")
    else:
        print("   ✅ A distribuição segue razoavelmente a Lei de Benford (Dados Naturais).")
        
    conn.close()

if __name__ == "__main__":
    benford_analysis()
