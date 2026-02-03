from typing import List, Callable, TypeAlias

from clientside.transaction.empenho_transaction import EmpenhoTransaction as EmpenhoContext
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from result import Result

##aparentemente esse modulo emite validações duplicadas em objetos internos que se auto validam. corrigir se houver tempo !

def regra_entidade_valida(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    entidade: Entidade | None = ctx.entidade

    if entidade is None:
        return Result.err("Entidade é obrigatória para empenho")

    res = entidade.validate()
    if res.is_err:
        return Result.err(res.error)
        
    return Result.ok(ctx)


def regra_fornecedor_valido(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    fornecedor: Fornecedor | None = ctx.fornecedor

    if fornecedor is None:
        return Result.err("Fornecedor é obrigatório para empenho")

    res = fornecedor.validate()
    if res.is_err:
        return Result.err(res.error)

    return Result.ok(ctx)


EMP_CONTEXT_RULES: List[
    Callable[[EmpenhoContext], Result[EmpenhoContext]]
] = []

def regra_entidade_valida(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    entidade: Entidade | None = ctx.entidade

    if entidade is None:
        return Result.err("Entidade é obrigatória para empenho")

    res = entidade.validate()
    if res.is_err:
        return Result.err(res.error)
        
    return Result.ok(ctx)


def regra_fornecedor_valido(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    fornecedor: Fornecedor | None = ctx.fornecedor

    if fornecedor is None:
        return Result.err("Fornecedor é obrigatório para empenho")

    res = fornecedor.validate()
    if res.is_err:
        return Result.err(res.error)

    return Result.ok(ctx)

def regra_fornecedor_consistente(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    """
    Verifica se o credor do empenho corresponde ao fornecedor do contrato.
    AVISO: Divergências aqui podem indicar fraude ou erro de cadastro.
    """
    fornecedor = ctx.fornecedor
    
    for emp in ctx.empenhos:
        # Empenho uses 'cpf_cnpj_credor' string vs Fornecedor 'documento' string
        if hasattr(emp, "cpf_cnpj_credor") and emp.cpf_cnpj_credor:
            if emp.cpf_cnpj_credor != fornecedor.documento:
                return Result.err(
                    f"Documento do credor ({emp.cpf_cnpj_credor}) diverge do fornecedor ({fornecedor.documento}) no empenho {emp.id_empenho}"
                )
    
    return Result.ok(ctx)

def regra_entidade_consistente(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    entidade = ctx.entidade

    for emp in ctx.empenhos:
        if emp.id_entidade != entidade.id_entidade:
            return Result.err(
                f"Empenho {emp.id_empenho} pertence a entidade diferente do contrato"
            )

    return Result.ok(ctx)

def regra_empenhos_do_mesmo_contrato(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    contrato_id = ctx.contrato.id_contrato

    for emp in ctx.empenhos:
        if emp.id_contrato != contrato_id:
            return Result.err(
                f"Empenho {emp.id_empenho} não pertence ao contrato da transação"
            )

    return Result.ok(ctx)

def regra_empenhos_unicos(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    ids = [emp.id_empenho for emp in ctx.empenhos]

    if len(ids) != len(set(ids)):
        return Result.err("Empenhos duplicados no agregado")

    return Result.ok(ctx)

def regra_valor_total_empenhado(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    """
    O domínio não permite estourar o valor do contrato.
    """
    total_empenhado = sum(emp.valor for emp in ctx.empenhos if emp.valor is not None)

    if total_empenhado > ctx.contrato.valor:
        return Result.err(
            f"Total empenhado ({total_empenhado}) excede valor do contrato ({ctx.contrato.valor})"
        )

    return Result.ok(ctx)

def regra_temporal_empenho(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    data_contrato = ctx.contrato.data

    for emp in ctx.empenhos:
        if emp.data_empenho < data_contrato:
            return Result.err(
                f"Empenho {emp.id_empenho} ({emp.data_empenho}) anterior à data do contrato ({data_contrato})"
            )

    return Result.ok(ctx)


EMPENHO_CONTEXT_RULES: List[
    Callable[[EmpenhoContext], Result[EmpenhoContext]]
] = [
    regra_entidade_valida,
    regra_fornecedor_valido,
    regra_entidade_consistente,
    regra_fornecedor_consistente,
    regra_empenhos_do_mesmo_contrato,
    regra_empenhos_unicos,
    regra_valor_total_empenhado,
    regra_temporal_empenho,
]


def executar_empenho_rules(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    result: Result[EmpenhoContext] = Result.ok(ctx)

    for regra in EMPENHO_CONTEXT_RULES:
        result = result.bind(regra)

    return result
