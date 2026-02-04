from typing import List, Callable, Optional, Dict, Any
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction as LiquidacaoContext, ItemLiquidacao
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from models.fornecedor import Fornecedor
from models.contrato import Contrato
from result import Result
##se der tentar otimizar pra O(N) com a single pass function
from decimal import Decimal
from datetime import date
from dataclasses import dataclass, field

#ainda na duvidas se implemento esse código de um jeito horrivel de ler usando O(n) ou se mudo
#pra algo mais declarativo usando O(n-r)
##tentando implementar um flow mais otimizado estraguei COMPLETAMENTE o padrão declarativo.
#se tiver tempo vou tentar reestruturar transactionliquidacao ou ao menos transformar em uma
#structure que torne o codigo mais intuitivo

@dataclass
class LiquidacaoAccumulator:
    total_valor: Decimal = Decimal(0)
    has_nfe: bool = False
    min_data_liq: Optional[date] = None

# --- Functional Helpers ---

def is_before(d1: date, d2: date) -> bool:
    """Retorna True se d1 for anterior a d2."""
    return d1 < d2

def is_after(d1: date, d2: date) -> bool:
    """Retorna True se d1 for posterior a d2."""
    return d1 > d2

def dates_match_predicate(d1, d2, predicate: Callable[[date, date], bool]) -> bool:
    """Aplica predicado genérico em duas datas."""
    if not d1 or not d2: return False
    # Handle datetime vs date
    dt1 = d1.date() if hasattr(d1, "date") else d1
    dt2 = d2.date() if hasattr(d2, "date") else d2
    return predicate(dt1, dt2)


def update_acc(acc: LiquidacaoAccumulator, item: ItemLiquidacao):
    """Atualiza o acumulador com os dados do item."""
    acc.total_valor += item.liquidacao.valor
    if item.nfe:
        acc.has_nfe = True

# --- Checkers (Pure Logic Checks) ---

def check_liquidation_dates(liq: LiquidacaoNotaFiscal, empenho: Empenho, contrato: Contrato) -> Result[None]:
    # LFE < Empenho
    if dates_match_predicate(liq.data_emissao, empenho.data_empenho, is_before):
        return Result.err(f"Liquidação ({liq.data_emissao}) anterior ao Empenho ({empenho.data_empenho}) - ID Emp: {empenho.id_empenho}")
    
    # LFE < Contrato
    if dates_match_predicate(liq.data_emissao, contrato.data, is_before):
        return Result.err(f"Liquidação ({liq.data_emissao}) anterior ao Contrato ({contrato.data})")
    
    return Result.ok(None)

def check_nfe_rules(nfe: Optional[Nfe], liq: LiquidacaoNotaFiscal, fornecedor: Fornecedor, contrato: Contrato, empenho: Empenho) -> Result[None]:
    
    NFE_DATE_RULES = [
    ("NFe <= Liquidação", lambda nfe_date, liq_date: nfe_date <= liq_date),
    ("NFe >= Empenho", lambda nfe_date, emp_date: nfe_date >= emp_date),
    ]

    if not nfe: return Result.ok(None)
    
    # CNPJ Match
    if nfe.cnpj_emitente != fornecedor.documento:
        return Result.err(f"CNPJ Emitente NFe ({nfe.cnpj_emitente}) diverge do Fornecedor Contrato ({fornecedor.documento})")

    # Datas NFe
    if nfe.data_hora_emissao:
        d_nfe = nfe.data_hora_emissao.date() if hasattr(nfe.data_hora_emissao, "date") else nfe.data_hora_emissao

        # Check explicit rules from configuration
        targets = {
            "NFe <= Liquidação": liq.data_emissao,
            "NFe >= Empenho": empenho.data_empenho
        }
        
        for rule_name, validator in NFE_DATE_RULES:
            target_date = targets.get(rule_name)
            if target_date:
                # Se validator retornar False, é erro
                if not dates_match_predicate(d_nfe, target_date, validator):
                     return Result.err(f"Violação Regra {rule_name}: NFe ({d_nfe}) vs Alvo ({target_date})")

        # NFe < Contrato (Mantido fora da lista pois é regra de negócio distinta das relativas a empenho/liq?)
        # Ou poderíamos adicionar à lista se quiséssemos.
        if dates_match_predicate(d_nfe, contrato.data, is_before):
             return Result.err(f"NFe emitida ({d_nfe}) antes da data do contrato ({contrato.data})")
    
    return Result.ok(None)

def check_aggregate_rules(acc: LiquidacaoAccumulator, empenho: Empenho) -> Result[None]:
    if acc.total_valor > empenho.valor:
        return Result.err(f"Soma Liquidações ({acc.total_valor}) excede Valor Empenho ({empenho.valor}) - ID Emp: {empenho.id_empenho}")
    return Result.ok(None)

def _validate_empenho_rules_single_pass(context_data: LiquidacaoContext, empenho_obj: Empenho, liquidacao_items: List[ItemLiquidacao]) -> Result[None]:
    """
    Valida regras utilizando padrão acc e pure functions
    """
    
    contrato = context_data.empenho_transaction.contrato
    fornecedor_obj = context_data.empenho_transaction.fornecedor
    
        
    # 2. Accumulator Initialization
    accumulator = LiquidacaoAccumulator()
    
    # 3. Single Pass Loop
    for item_data in liquidacao_items:
        liquidacao_obj = item_data.liquidacao
        nfe_obj = item_data.nfe
        
        # Update Accumulator
        update_acc(accumulator, item_data)
        
        # FAIL FAST: Validar limite de valor iterativamente
        # Se ultrapassar, retorna erro imediatamente, não processa o resto
        limit_validation_result = check_aggregate_rules(accumulator, empenho_obj)
        if limit_validation_result.is_err: return limit_validation_result
        
        # Item-Level Checks (composed helpers)
        dates_validation_result = check_liquidation_dates(liquidacao_obj, empenho_obj, contrato)
        if dates_validation_result.is_err: return dates_validation_result
        
        nfe_validation_result = check_nfe_rules(nfe_obj, liquidacao_obj, fornecedor_obj, contrato, empenho_obj)
        if nfe_validation_result.is_err: return nfe_validation_result

    return Result.ok(None)


# Import integrity checkers from subdomain
from clientside.domains.subdomains.nfe_liquidacao import _check_duplicates



def Valida(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """
    Roda validações: 
    1. Checa Integridade (Duplicidade).
    2. Normaliza estrutura (List -> Dict).
    3. Itera sobre a estrutura já agrupada por empenho (Normalized) e aplica validações.
    Estrutura: Dict[id_empenho, Dict[id_liq, ItemLiquidacao]]
    """
    # 0. Pré-validação de integridade
    check_res = _check_duplicates(ctx)
    if check_res.is_err:
        return Result.err(check_res.error)

    # 1. Normalização (Transição de Estado List -> Dict Aninhado)
    ctx.normalize()
 
    for empenho in ctx.empenho_transaction.empenhos.values():
        itens_dict = ctx.itens_liquidados.get(empenho.id_empenho)
        
        if itens_dict:
            itens = list(itens_dict.values())
            
            # Chama Validação Otimizada Single-Pass
            # Passa objeto empenho direto, sem re-lookup
            res = _validate_empenho_rules_single_pass(ctx, empenho, itens)
            if res.is_err:
                return Result.err(res.error)
                
    return Result.ok(ctx)
