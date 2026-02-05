
from decimal import Decimal, ROUND_HALF_UP

def quantize_money(value: Decimal) -> Decimal:
    """
    Padroniza o valor monetário para 2 casas decimais usando arredondamento padrão.
    Essencial para evitar erros de comparação como 100.00 != 100.00000001
    """
    if value is None:
        return Decimal("0.00")
        
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def sums_match_limit(sum_value: Decimal, limit: Decimal) -> bool:
    """
    Verifica se a soma excede o limite, com tolerância zero após quantização.
    Retorna True se sum <= limit.
    """
    return quantize_money(sum_value) <= quantize_money(limit)

def values_match(val1: Decimal, val2: Decimal) -> bool:
    """
    Verifica igualdade estrita entre valores monetários.
    """
    return quantize_money(val1) == quantize_money(val2)
