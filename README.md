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

`make view-transaction-empenhos` => Exibe transformações na etapa de empenho<br>
`make view-transaction-liquidacao` => Exibe linkages Empenho → Liquidação<br>
`make view-transaction-pagamento` => Exibe fluxo Liquidação → Pagamento<br>
`make fullpipe` => Pipeline completo: processa TODOS os contratos em batches de 100, logando estrutura completa<br>
`make fullpipedebug` => Pipeline debug com logs detalhados e delay configurável

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
<span style="color:gray"><strong>LiquidacaoNotaFiscal (n) ──── (1) NFe</strong></span>

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

**TransactionComplete →** Instanciado a partir de TransactionLiquidation após validações e checagens, com adição de recursos pertinentes ao atual lifecycle do objeto.  

Sempre que um objeto composto por dependências é instanciado — como uma Transaction que agrega múltiplas entidades — é seguro assumir que todos os objetos envolvidos já passaram por seus contratos de validação.  (checar referencia 1)

Dessa forma, a consistência do objeto agregado é garantida não só por suas regras, tanto quanto pelas regras internas e invariantes de cada componente que faz parte da agregação.

### 3. Ciclo de Vida do Contrato (Transaction Lifecycle)

Podemos definir o ciclo do contrato público como um objeto transação composto por estados sequenciais: **Início, Meio e Fim**.

#### 🔴 Início (TransactionEmpenho)
*   **Fase**: Inicial.
*   **Foco**: Reserva de orçamento e formalização do compromisso.
*   **Requisitos**: Validação de documentos básicos e verificações técnicas preliminares.

#### 🟡 Meio (TransactionLiquidação)
*   **Fase**: Intermediária (Alta Complexidade).
*   **Foco**: Reconhecimento da dívida após a entrega do bem ou serviço.
*   **Requisitos**: Consolidação de dados (notas fiscais, medições) e aferição técnica rigorosa.

#### 🟢 Fim (Pagamento)
*   **Fase**: Encerramento.
*   **Foco**: Liquidação financeira da obrigação.

---

### 🛡️ Domain Validation Rules & Invariants

As validações são centralizadas em contextos transacionais imutáveis (Transaction Contexts), permitindo que cada etapa do ciclo da despesa pública tenha invariantes explícitas e auditáveis.

 -Benefícios:
1.  Rastreabilidade: Falhas são detectadas em referência ao estágio da transação e em seus dominios específicos.
2.  Desacoplamento: Evolução do domínio E entidades sem efeitos colaterais em dominios E entidades não relacionadas.
3.  Paradigma Funcional: Código declarativo, legível e determinístico.
4.  Dominio Declarativo: O dominio é explicito e bem segmentado, sendo possível entender o fluxo de estados e suas respectivas regras claramente.

 - Contextos Implementados:
- `TransactionEmpenho`
- `TransactionLiquidacao`
- `PaymentTransaction`

 - Dominios de validação implementados:
- `EmpenhoDomain`
- `ContratoDomain`
- `PagamentoDomain`

 - SubDOmain de validação implementados:
  - FinancialUtiliy
  - nfeInegrity

-> Subdominios são usados para agrupar regras de negocio relacionadas e facilitar organização e reutilização somente
   exemplo: muitos domains fazem sucessivas validações de data. Para evitar repetição de funções iguais: declarar funçõs de data em um subdomain de reutilização e compartilhar entre mṕidulos
   não tive tempo de refatorar os dominios em subdominiosn adequadamente

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


### 5. Validações por Fase do Ciclo de Vida

#### 🔴 Fase Empenho (TransactionEmpenho)

**📋 Análise Domínio-Negócio (Despesa Pública):**
- [ ] O credor do empenho corresponde ao fornecedor vencedor da licitação/contratado?
- [ ] A soma dos empenhos excede o valor total do contrato firmado?
- [ ] Existem empenhos emitidos antes da assinatura do contrato?
- [ ] O empenho foi realizado pela mesma entidade pública contratante?
- [ ] Há empenhos duplicados para a mesma despesa?

**💻 Análise Domínio-Código:**

| Regra | Descrição | Tipo |
|-------|-----------|------|
| `regra_entidade_valida` | Entidade é obrigatória e válida | Integridade |
| `regra_fornecedor_valido` | Fornecedor é obrigatório e válido | Integridade |
| `regra_entidade_consistente` | Empenhos pertencem à mesma entidade do contrato | Consistência |
| `regra_fornecedor_consistente` | CPF/CNPJ do credor = documento do fornecedor contratado | Consistência |
| `regra_empenhos_do_mesmo_contrato` | Todos empenhos referem ao mesmo contrato | Integridade |
| `regra_empenhos_unicos` | Não há IDs de empenho duplicados | Unicidade |
| `regra_valor_total_empenhado` | Σ(Empenhos) ≤ Valor Contrato | Financeiro |
| `regra_temporal_empenho` | Data Empenho ≥ Data Contrato | Temporal |

---

#### 🟡 Fase Liquidação (TransactionLiquidação)

**📋 Análise Domínio-Negócio (Despesa Pública):**
- [ ] A liquidação foi realizada antes do empenho da despesa?
- [ ] A nota fiscal foi emitida por fornecedor diferente do contratado?
- [ ] A nota fiscal é anterior à formalização do contrato?
- [ ] A soma das liquidações excede o valor empenhado?
- [ ] Há liquidações parciais que somadas excedem o valor da NFe apresentada?
- [ ] A mesma nota fiscal está sendo usada para liquidar despesas de contratos diferentes?

**💻 Análise Domínio-Código:**

| Regra | Descrição | Tipo |
|-------|-----------|------|
| `check_integrity_nfe_liquidacao` | NFe única por liquidação (1:1) | Integridade |
| `check_liquidation_dates` | Liquidação posterior ao Empenho e Contrato | Temporal |
| `check_nfe_rules` | CNPJ emitente NFe = Fornecedor contratado | Consistência |
| `check_nfe_rules` | NFe ≤ Liquidação e NFe ≥ Empenho (ordem cronológica) | Temporal |
| `check_nfe_rules` | NFe posterior à data do contrato | Temporal |
| `check_aggregate_rules` | Σ(Liquidações) ≤ Valor Empenho | Financeiro |
| `check_nfe_aggregate_limit` | Σ(Liquidações parciais) ≤ Valor NFe | Financeiro |

---

#### 🟢 Fase Pagamento (TransactionComplete)

**📋 Análise Domínio-Negócio (Despesa Pública):**
- [ ] Há pagamentos registrados sem liquidação correspondente?
- [ ] Existem pagamentos que excedem o valor total liquidado?
- [ ] Existem contratos com pagamentos acima do valor total contratado?
- [ ] Há pagamentos com valor zerado ou negativo?
- [ ] Existem pagamentos realizados antes do reconhecimento da dívida (liquidação)?
- [ ] Há pagamentos com data futura registrada no sistema?
- [ ] Existem pagamentos anteriores à data do contrato ou empenho?

**💻 Análise Domínio-Código:**

| Regra | Descrição | Tipo |
|-------|-----------|------|
| `check_pagamento_requires_liquidacao` | Pagamento só existe se houver liquidação | Integridade |
| `check_pagamento_ids_unique` | IDs de pagamento únicos no agregado | Unicidade |
| `check_pagamento_not_exceeds_liquidacao` | Σ(Pagamentos) ≤ Σ(Liquidações) por Empenho | Financeiro |
| `check_total_pago_not_exceeds_contrato` | Σ(Pagamentos) ≤ Valor Contrato | Financeiro |
| `check_pagamento_valor_positivo` | Valor Pagamento > 0 | Integridade |
| `check_pagamento_date_after_liquidacao` | Data Pagamento ≥ min(Data Liquidação) | Temporal |
| `check_pagamento_date_not_future` | Data Pagamento ≤ Hoje | Temporal |
| `check_pagamento_date_after_contrato` | Data Pagamento ≥ Data Contrato | Temporal |
| `check_pagamento_date_after_empenho` | Data Pagamento ≥ min(Data Empenho) | Temporal |

# Suposições
insights pessoais:
Não fica claro como os dados são registrados. Exemplo: todos os registros são feitos processualmente obdecendo ordem de procedencia através de um sistema automatizado? se um contrato não possui entidades do meio do ciclo de vida, ou apresenta inconsistencias nelas, vale a pena fazer validações subsequentes? ou já invalidar o contrato inteiro a partir dai? ou então invalidar em etapas mais sensiveis, como pagamentos?
1. Contratos podem ser performados por muitos empenhos?
 - Sim!

2. **Cardinalidade Contrato → Empenho (1:N)**
    *   **Observação**: O banco de dados não restringe a criação de múltiplos empenhos para um mesmo contrato.
    *   **Invariante**: A normalização ocorre via **Fornecedor**: múltiplos empenhos são permitidos, desde que todos mantenham consistência com o fornecedor titular do contrato.
 
3. Há diferença de regras de modelagem e regras de negócio? a mesma obrigação de pagamento pode ser concluida por multiplos pagamentos que se somam ao valor do empenho?
 - resposta sim! há diferemça de regras de modelagem do banco e regras de negocio (só nao lem bro uma agora.. to-do acrescentar uma aqui!)

4- Tive duvidas em relação a cardinalidade 1-1 entre Nfe e  liquidação, pois no banco de dados não há restrição de 1-1, podendo ser 1-n
 - A resposta mais aceitavel que tive via IA e docs publicos é que é uma relação aceitavel ter 1-N, contanto que a soma dos valores das notas fiscais não ultrapasse o valor da liquidação.
 - : Em compras públicas é comum o fatiamento de pagamentos — uma única NFe pode ser liquidada parcialmente em etapas, especialmente em contratos de fornecimento contínuo, entregas parceladas etc

Quando iniciei o projeto foquei mais na validação de contratos através de fluxos exclusivitarios que vai filtrando contratos e exluindo invalidos de validações posteriores, algo como um circuitbreaker. Isso volta um pouco ao inicio dessa seção onde questionei sobre o mecanismo de registro dos dados: se passa-se por algo processual ou se seria possivel simplesmente emitir um raw sql na ponta do funil e inserir pagamentos do nada -sem referencias a entidades passadas que revelassem inconsistencias.
-- resposta  esse questionamento. Sim, é possivel incluir validações não sequenciais, fazendo consultas estratégicas que possam revelar contratos invalidos em um approach from the tail: puxando
a informação do fim pro final, ou de dados estratégicos com maior probabiblidade de revelar indicios de inconsistencias. 

De qualquer forma (escrevo isso enquanto desenvolvo) estou buscando modificar a implementação para ao invés de filtrar e remover de validações futuras, emitir subtrailing logs nas entidades que formem inconsistencias passadas e/ou que promovam skip em algumas validações posteriores que obviamente irão fa lhar a depender da inconsistencia. Confesso que não sei se será possivel pois -apenas pra me justificar- iniciei o teste no dia 02,  porque no momento do contato por email eu já estava participando de outros dois testes técnicos pra entregas pro dia 30/01 e 02/02
--ESsa seria uma feature interessante, mas acho que fugiria um pouco do escopo do teste. há coisas mais im portantes pra se executar

Adicionalmenta ao tópico de mecanismo de inserção de dados: seria interessante saber se a inserção pode ser feita aleatoriamente em qualquer etapa do processo de criação/inserção. Assim seria possivel avaliar uma abordagem diferente de análise, algo como um tailback approach que iria validar de trás pra frente (da parte mais sensivel, onde há pagamentos de fatos) com informações do início
-- Já respondi essa tantas vezes! mas estou deixando pra fins de documentação