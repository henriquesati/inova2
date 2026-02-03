PYTHON ?= python3
SQLHELP := sqlhelp.py

# tabelas conhecidas (dicionário de dados)
TABLES := \
	contrato \
	empenho \
	liquidacao_nota_fiscal \
	nfe \
	pagamento \
	nfe_pagamento \
	fornecedor \
	entidade

.PHONY: help sql list $(addprefix sql,$(TABLES)) $(addprefix sql-,$(TABLES)) sql% sql-%

help:
	@echo ""
	@echo "Inova - SQL Helper"
	@echo "------------------"
	@echo ""
	@echo "Comandos:"
	@echo "  make list"
	@echo "  make sql TABLE=<tabela>"
	@echo "  make sql<tabela>              (ex: make sqlcontrato)"
	@echo "  make sql-<tabela>             (ex: make sql-nfe)"
	@echo ""
	@echo "Tabelas conhecidas:"
	@for t in $(TABLES); do echo "  - $$t"; done
	@echo ""

# lista tabelas suportadas
list:
	@for t in $(TABLES); do echo $$t; done

# modo universal (README)
# ex: make sql TABLE=contrato
# ex: make sql TABLE=-nfe
sql:
	@if [ -z "$(TABLE)" ]; then \
		echo "Erro: informe TABLE=<tabela> (ex: make sql TABLE=contrato)"; \
		exit 1; \
	fi
	$(PYTHON) $(SQLHELP) $(TABLE)

# alvos explícitos para todas as entidades do banco
$(addprefix sql,$(TABLES)):
	$(PYTHON) $(SQLHELP) $(patsubst sql%,%,$@)

# modo "flag style" (README): make sql-nfe -> python3 sqlhelp.py -nfe
$(addprefix sql-,$(TABLES)):
	$(PYTHON) $(SQLHELP) -$(patsubst sql-%,%,$@)

# fallback genérico (permite make sqlpagamento, make sqlcliente etc)
sql%:
	$(PYTHON) $(SQLHELP) $*

# fallback genérico (permite make sql-nfe, make sql-contrato etc)
sql-%:
	$(PYTHON) $(SQLHELP) -$*

# atalho para atualizar documentação
push-readme:
	git add README.md
	git commit -m "docs: update README" || echo "Nada para commitar no README"
	git push origin master

view-empenhos:
	$(PYTHON) views/etl_empenhos.py

