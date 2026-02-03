from typing import List, Callable
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction as LiquidacaoContext
from models.nfe import Nfe
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from result import Result

def regra_liquidacao_existe(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Verifica se a Liquidação foi recuperada com sucesso."""
    if ctx.liquidacao is None:
        return Result.err("Liquidação não encontrada ou inválida.")
    return Result.ok(ctx)

def regra_nfe_valida(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Valida a NFe associada à liquidação."""
    if ctx.nfe is None:
        return Result.err("NFe obrigatória para Liquidação.")
    
    res = ctx.nfe.validate()
    if res.is_err:
        return Result.err(f"Regra NFe falhou: {res.error}")
        
    return Result.ok(ctx)

# --- CONSISTÊNCIA DE CHAVES ---

def regra_chave_nfe_consistente(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Verifica se a chave na Liquidação bate com a chave da NFe recuperada."""
    if ctx.liquidacao.chave_danfe != ctx.nfe.chave_nfe:
        return Result.err(f"Inconsistência de Chaves: Liq={ctx.liquidacao.chave_danfe} vs NFe={ctx.nfe.chave_nfe}")
    return Result.ok(ctx)

def regra_nfe_nao_duplicada(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Verifica se a NFe não está associada a múltiplas liquidações (unicidade 1-1)."""
    count = LiquidacaoNotaFiscal.count_by_chave_danfe(ctx.nfe.chave_nfe)
    if count > 1:
        return Result.err("NFe associada a múltiplas liquidações")
    return Result.ok(ctx)

# --- REGRAS DE VALOR ---

def regra_relacao_valores(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """
    Validações de Valor:
    1. Valor da Liquidação deve coincidir com da NFe (se ambos presentes).
    2. Valor da NFe não pode exceder valor do Contrato (se contrato acessível).
    """
    # 1. Liq vs NFe
    if ctx.liquidacao.valor and ctx.nfe.valor_total_nfe:
        if ctx.liquidacao.valor != ctx.nfe.valor_total_nfe:
             return Result.err(f"Divergência de valores: Liq={ctx.liquidacao.valor} vs NFe={ctx.nfe.valor_total_nfe}")

    # 2. NFe vs Contrato
    contrato = getattr(ctx.empenho, "contrato", None)
    if contrato:
        if ctx.nfe.valor_total_nfe > contrato.valor:
            return Result.err(f"Valor da NFe ({ctx.nfe.valor_total_nfe}) excede valor do Contrato ({contrato.valor})")

    return Result.ok(ctx)

# --- REGRAS TEMPORAIS ---

def regra_nfe_dentro_vigencia(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Verifica se NFe foi emitida dentro da vigência do contrato (após data de início)."""
    contrato = getattr(ctx.empenho, "contrato", None)
    if contrato:
        data_contrato = contrato.data
        if ctx.nfe.data_hora_emissao:
           # Garante comparação entre date objects
           d_nfe = ctx.nfe.data_hora_emissao.date() if hasattr(ctx.nfe.data_hora_emissao, "date") else ctx.nfe.data_hora_emissao
           if data_contrato > d_nfe:
                 return Result.err(f"NFe emitida antes do contrato: {d_nfe} < {data_contrato}")
    return Result.ok(ctx)

def regra_ordem_temporal_empenho_liquidacao(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Verifica se a Liquidação é posterior ou concomitante ao Empenho."""
    if ctx.liquidacao.data_emissao < ctx.empenho.data_empenho:
        return Result.err(f"Liquidação ({ctx.liquidacao.data_emissao}) anterior ao empenho ({ctx.empenho.data_empenho})")
    return Result.ok(ctx)

# --- REGRAS DE ENTIDADE/EMITENTE ---

def regra_emitente_nfe_compativel(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    """Verifica se o emitente da NFe é o Fornecedor do Empenho."""
    # O Fornecedor pode estar injetado no Empenho
    fornecedor = getattr(ctx.empenho, "fornecedor", None)
    if fornecedor and ctx.nfe.cnpj_emitente != fornecedor.documento:
        return Result.err(f"Emitente da NFe ({ctx.nfe.cnpj_emitente}) difere do fornecedor contratado ({fornecedor.documento})")
    return Result.ok(ctx)


LIQUIDACAO_CONTEXT_RULES: List[
    Callable[[LiquidacaoContext], Result[LiquidacaoContext]]
] = [
    # Estruturais Básicas
    regra_liquidacao_existe,
    regra_nfe_valida,
    
    # Consistência de Chaves / Unicidade
    regra_chave_nfe_consistente,
    regra_nfe_nao_duplicada,
    
    # Temporalidade
    regra_nfe_dentro_vigencia,
    regra_ordem_temporal_empenho_liquidacao,
    
    # Valores e Entidades
    regra_relacao_valores,
    regra_emitente_nfe_compativel,
]

def executar_liquidacao_rules(ctx: LiquidacaoContext) -> Result[LiquidacaoContext]:
    result: Result[LiquidacaoContext] = Result.ok(ctx)

    for regra in LIQUIDACAO_CONTEXT_RULES:
        result = result.bind(regra)

    return result
