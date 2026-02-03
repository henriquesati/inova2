# Inova Project

Ferramentas e utilitários para apoiar o desenvolvimento do projeto **Inova**.

Este repositório contém scripts voltados para inspeção rápida do banco e validações auxiliares.

---

## SQL Helper (`sqlhelp.py`)

O `sqlhelp.py` é um utilitário de linha de comando para inspecionar rapidamente a **estrutura (schema)** de tabelas no PostgreSQL.

- nomes das colunas
- tipos
- nullable
- defaults

## 🛠️ Command Line Interface (CLI)

###  SQL helper
utilitário de linha de comando para inspecionar  o **schema** de tabelas no banco: variaveis, tipos, nullable, defaults etc.

**Uso:** `make sql-[nome_da_tabela]` ou `make sql[nome_da_tabela]`


| `make sqlnfe` => Inspeciona a estrutura da tabela `nfe`<br>
| `make sql-contrato` => Inspeciona a estrutura da tabela `contrato` <br>

### 📊 Views (ETL Output)
Scripts de feedback visual para inspecionar o output e transformações de alguns pipelines ETL em cada ciclo de vida da transação.

`make view-empenhos` => Exibe transformações na etapa de empenho<br>
`make view-liquidacao` => Exibe linkages Empenho → Liquidação<br>
`make view-pagamento` => Exibe fluxo Liquidação → Pagamento |

---

## Entidades do banco (tabelas)

O banco modela os estágios da despesa pública: **Contratação → Empenho → Liquidação → Pagamento**, além de cadastros auxiliares.

As tabelas presentes no banco e suas relações são:
relações apresentadas de geral e não relacionadas a invariantes de negócio.

## 📚 Relações entre Entidades do Domínio

- `entidade`  
<span style="color:gray"><strong>Entidade (1) ──── (N) Contrato</strong></span><br>
<span style="color:gray"><strong>Entidade (1) ──── (N) Empenho</strong></span>

- `fornecedor`  
<span style="color:gray"><strong>Fornecedor (1) ──── (N) Contrato</strong></span><br>
<span style="color:gray"><strong>Fornecedor (1) ──── (N) Empenho</strong></span><br>
<span style="color:gray"><strong>Fornecedor (1) ──── (N) NFe</strong></span>

- `contrato`  
<span style="color:gray"><strong>Contrato (1) ──── (N) Empenho</strong></span>

- `empenho`  
<span style="color:gray"><strong>Empenho (1) ──── (N) LiquidacaoNotaFiscal</strong></span><br>
<span style="color:gray"><strong>Empenho (1) ──── (N) Pagamento</strong></span>

- `liquidacao_nota_fiscal`  
<span style="color:gray"><strong>LiquidacaoNotaFiscal (1) ──── (1) NFe</strong></span>

- `nfe`  
<span style="color:gray"><strong>NFe (1) ──── (N) NFePagamento</strong></span>

- `pagamento`  
<span style="color:gray"><strong>Pagamento (1) ──── (N) NFePagamento</strong></span>
---

- `nfe_pagamento`  
<span style="color:gray"><strong>NFe (N) ──── (N) Pagamento</strong></span>

## 🧭 Relações Normativas do Domínio (Regras e Invariantes)
- `entidade`  
<span style="color:gray"><strong>Entidade (1) ──── (N) Contrato</strong></span><br>
<span style="color:gray"><strong>Contrato (1) ──── (1) Fornecedor</strong></span><br>
<span style="color:gray"><strong>Fornecedor (1) ──── (N) Empenho</strong></span><br>
<span style="color:gray"><strong>Empenho (1) ──── (N)  liquidacao nota fiscal</strong></span><br>
<span style="color:gray"><strong>liquidacao nota fiscal (1) ──── (1)  Nfe</strong></span><br>

### 🔴 Relações 1-to-1 Críticas (Invariantes de Domínio)

<span style="color:gray"><strong>LiquidacaoNotaFiscal (1) ──── (1) NFe</strong></span>

---

## 🧭 Abordagem
Adoto um estilo de representação e modelagem orientado à imutabilidade de dados e transformações determinísticas de estado, inspirado em princípios de **Railway Programming**, no qual cada transição ocorre de forma explícita, previsível e validada.  
Os objetos são tratados como estruturas imutáveis, e qualquer evolução ocorre por meio da geração de novas instâncias derivadas, que sõ são instanciados depois de passar por validações explícitas de contratos, regras de domínio e invariantes, mantendo previsibilidade de estado e reduzindo efeitos colaterais.

A abordagem se baseou na estruturação de um objeto Transaction que agrupa as entidades relacionadas à execução da despesa pública persistidas no banco de dados. Esse objeto foi fatiado em três instâncias transacionais distintas, cada uma representando um estágio específico do ciclo da despesa, com recortes e adaptações que facilitam a análise e a aplicação de regras diretamente no código.

**TransactionEmpenho →** Iniciação do objeto e alocaçãço de recursos  

**Transactionliquidation →** Instanciado a partir de TransactionEmpenho após validações e checagems, com adição de recursos pertinentes ao atual lifecycle do objeto.

**TransactionComplete →** Instanciado a partir de TransactionLiquidation após validações e checagem de checagens, com adição de recursos pertinentes ao atual lifecycle do objeto.  

Sempre que um objeto composto por dependências é instanciado — como uma Transaction que agrega múltiplas entidades — é seguro assumir que todos os objetos envolvidos já passaram por seus contratos de validação.  (checar referencia 1)

Dessa forma, a consistência do objeto agregado é garantida não só por suas regras, tanto quanto pelas regras internas e invariantes de cada componente que faz parte da agregação.
```bash

### 3. Ciclo de Vida do Contrato (Transaction Lifecycle)

Podemos definir o ciclo do contrato público como um objeto transação composto por estados sequenciais: **Início, Meio e Fim**.

#### 🟢 Início (TransactionEmpenho)
*   **Fase**: Inicial.
*   **Foco**: Reserva de orçamento e formalização do compromisso.
*   **Requisitos**: Validação de documentos básicos e verificações técnicas preliminares.

#### 🟡 Meio (TransactionLiquidação)
*   **Fase**: Intermediária (Alta Complexidade).
*   **Foco**: Reconhecimento da dívida após a entrega do bem ou serviço.
*   **Requisitos**: Consolidação de dados (notas fiscais, medições) e aferição técnica rigorosa.

#### 🔴 Fim (Pagamento)
*   **Fase**: Encerramento.
*   **Foco**: Liquidação financeira da obrigação.

---

### 🛡️ Domain Validation Rules & Invariants

As validações são centralizadas em contextos transacionais imutáveis (**Transaction Contexts**), permitindo que cada etapa do ciclo da despesa pública tenha invariantes explícitas e auditáveis.

**Benefícios:**
1.  **Rastreabilidade**: Falhas são detectadas em referência ao estágio da transação.
2.  **Desacoplamento**: Evolução do domínio sem efeitos colaterais em entidades não relacionadas.
3.  **Paradigma Funcional**: Código declarativo, legível e determinístico.

**Contextos Implementados:**
- `TransactionEmpenho`
- `TransactionLiquidacao`
- `PaymentTransaction`

### 4. Escopos de Teste e Validação

Perguntas críticas que o sistema de validação responde para garantir a integridade dos dados:

#### 💰 Integridade Financeira
- [ ] Há pagamentos registrados sem empenhos correspondentes?
- [ ] Existem contratos variando limites de valor? (Pagamentos > Contratado)

#### 🔗 Integridade Relacional e Temporal
- [ ] **Violação de Propriedade (1:1)**: Uma Nota Fiscal está sendo compartilhada incorretamente entre múltiplos contratos?
- [ ] **Coerência Cronológica**:
    - A data da NFe é compatível com a vigência do contrato?
    - Existem NFs emitidas *antes* da assinatura do contrato ou do empenho?