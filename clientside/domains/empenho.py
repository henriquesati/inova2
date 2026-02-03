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


EMPENHO_CONTEXT_RULES: List[
    Callable[[EmpenhoContext], Result[EmpenhoContext]]
] = [
    regra_entidade_valida,
    regra_fornecedor_valido,
]


def executar_empenho_rules(ctx: EmpenhoContext) -> Result[EmpenhoContext]:
    result: Result[EmpenhoContext] = Result.ok(ctx)

    for regra in EMPENHO_CONTEXT_RULES:
        result = result.bind(regra)

    return result
