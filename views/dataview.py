import streamlit as st
import plotly.express as px
import pandas as pd
import sys
import os
import time
from collections import defaultdict
from typing import Dict, List, Any

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from result import Result
from db_connection import get_db_connection
from models.contrato import Contrato
from models.entidade import Entidade
from models.fornecedor import Fornecedor
from models.empenho import Empenho
from models.liquidacao_nota_fiscal import LiquidacaoNotaFiscal
from models.nfe import Nfe
from models.pagamento import Pagamento

from clientside.transaction.empenho_transaction import EmpenhoTransaction
from clientside.transaction.transaction_liquidacao import LiquidacaoTransaction
from clientside.transaction.transaction_pagamento import PaymentTransaction
from clientside.domains.empenho import executar_empenho_rules as ValidaEmpenho
from clientside.domains.liquidação import Valida as ValidaLiquidacao
from clientside.domains.pagamento import Valida as ValidaPagamento

# ==========================================
# SUPER PERFORMANCE BATCH LOADERS (From/Based on etl_fullpipe.py)
# ==========================================

def batch_load_contratos(cursor, limit: int = 200, offset: int = 0) -> List[Contrato]:
    """Carrega um batch de contratos."""
    cursor.execute(f"SELECT * FROM contrato ORDER BY id_contrato LIMIT {limit} OFFSET {offset}")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    contratos = []
    for row in rows:
        res = Contrato.create(dict(zip(cols, row)))
        if res.is_ok:
            contratos.append(res.value)
    return contratos

def batch_load_related_data(cursor, contratos: List[Contrato]):
    """
    Carrega dados relacionados para um batch de contratos.
    Retorna dicts indexados para O(1) lookup.
    """
    if not contratos:
        return {}, {}, {}, {}, {}, {}
    
    contract_ids = [c.id_contrato for c in contratos]
    entidade_ids = list(set(c.id_entidade for c in contratos))
    fornecedor_ids = list(set(c.id_fornecedor for c in contratos))
    
    # ENTIDADES
    cursor.execute(f"SELECT * FROM entidade WHERE id_entidade = ANY(%s)", (entidade_ids,))
    entidades_map = {row[0]: Entidade.from_row(dict(zip([d[0] for d in cursor.description], row))).value 
                     for row in cursor.fetchall() if Entidade.from_row(dict(zip([d[0] for d in cursor.description], row))).is_ok}
    
    # FORNECEDORES
    cursor.execute(f"SELECT * FROM fornecedor WHERE id_fornecedor = ANY(%s)", (fornecedor_ids,))
    fornecedores_map = {row[0]: Fornecedor.from_row(dict(zip([d[0] for d in cursor.description], row))).value 
                        for row in cursor.fetchall() if Fornecedor.from_row(dict(zip([d[0] for d in cursor.description], row))).is_ok}
    
    # EMPENHOS
    cursor.execute(f"SELECT * FROM empenho WHERE id_contrato = ANY(%s)", (contract_ids,))
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    empenhos_por_contrato = defaultdict(list)
    all_empenho_ids = []
    for row in rows:
        res = Empenho.from_row(dict(zip(cols, row)))
        if res.is_ok:
            emp = res.value
            empenhos_por_contrato[emp.id_contrato].append(emp)
            all_empenho_ids.append(emp.id_empenho)
            
    # LIQUIDAÇÕES & NFEs & PAGAMENTOS (Lazy Logic but Batched)
    liquidacoes_por_empenho = defaultdict(list)
    all_chaves_danfe = []
    pagamentos_por_empenho = defaultdict(list)
    nfes_map = {}

    if all_empenho_ids:
        # LIQUIDAÇÕES
        cursor.execute(f"SELECT * FROM liquidacao_nota_fiscal WHERE id_empenho = ANY(%s)", (all_empenho_ids,))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        for row in rows:
            res = LiquidacaoNotaFiscal.from_row(dict(zip(cols, row)))
            if res.is_ok:
                liq = res.value
                liquidacoes_por_empenho[liq.id_empenho].append(liq)
                if liq.chave_danfe:
                    all_chaves_danfe.append(liq.chave_danfe)

        # PAGAMENTOS
        cursor.execute(f"SELECT * FROM pagamento WHERE id_empenho = ANY(%s)", (all_empenho_ids,))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        for row in rows:
            res = Pagamento.from_row(dict(zip(cols, row)))
            if res.is_ok:
                pag = res.value
                pagamentos_por_empenho[pag.id_empenho].append(pag)
                
    # NFES
    if all_chaves_danfe:
        cursor.execute(f"SELECT * FROM nfe WHERE chave_nfe = ANY(%s)", (all_chaves_danfe,))
        nfes_map = {row[1]: Nfe.from_row(dict(zip([d[0] for d in cursor.description], row))).value
                    for row in cursor.fetchall() if Nfe.from_row(dict(zip([d[0] for d in cursor.description], row))).is_ok}

    return (
        entidades_map,
        fornecedores_map,
        dict(empenhos_por_contrato),
        dict(liquidacoes_por_empenho),
        nfes_map,
        dict(pagamentos_por_empenho)
    )


# ==========================================
# UTIL
# ==========================================
def obj_to_dict(obj: Any) -> Any:
    if isinstance(obj, list):
        return [obj_to_dict(item) for item in obj]
    if hasattr(obj, "__dataclass_fields__") or hasattr(obj, "__dict__"):
        attrs = {}
        if hasattr(obj, "__dict__"):
            attrs = obj.__dict__
        elif hasattr(obj, "__dataclass_fields__"):
            from dataclasses import fields
            attrs = {f.name: getattr(obj, f.name) for f in fields(obj)}
        clean_attrs = {}
        for k, v in attrs.items():
            if k.startswith('_'): continue
            clean_attrs[k] = obj_to_dict(v)
        return clean_attrs
    return obj


# ==========================================
# PIPELINE EXECUTION
# ==========================================
@st.cache_data(ttl=600, show_spinner=False)
def load_and_process_data_optimized(batch_size: int = 200):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 0. Get Total Count
    cursor.execute("SELECT COUNT(*) FROM contrato")
    total_contratos = cursor.fetchone()[0]
    
    processed_results = []
    offset = 0
    
    progress_bar = st.progress(0, text="Iniciando processamento em batch...")
    
    while offset < total_contratos:
        remaining = total_contratos - offset
        current_batch_size = min(batch_size, remaining)
        
        # Update progress (arithmetic operations per user request)
        pct = min(offset / total_contratos, 1.0)
        progress_bar.progress(pct, text=f"Processando contratos {offset+1} a {offset+current_batch_size} de {total_contratos}...")

        # 1. Batch Load
        contratos = batch_load_contratos(cursor, limit=current_batch_size, offset=offset) # Modified helper needed
        (entidades, fornecedores, empenhos, liquidacoes, nfes, pagamentos) = batch_load_related_data(cursor, contratos)
        
        # 2. Batch Build (In-Memory!)
        tx_results = EmpenhoTransaction.build_from_batch(
            contratos, entidades, fornecedores, empenhos
        )
        
        for contrato, emp_res in zip(contratos, tx_results):
            status = "OK"
            error_msg = None
            stage = "Empenho"
            final_obj = None
            
            # Empenho Stage
            if emp_res.is_err:
                status = "ERRO"
                error_msg = emp_res.error
            else:
                val_emp = ValidaEmpenho(emp_res.value)
                if val_emp.is_err:
                    status = "ERRO"
                    error_msg = val_emp.error
                else:
                    final_obj = val_emp.value
                    stage = "Liquidação"
                    
                    # Liquidacao Stage (Batch Optimized)
                    liq_tx_res = LiquidacaoTransaction.build_from_batch(val_emp.value, liquidacoes, nfes)
                    
                    if liq_tx_res.is_err:
                        status = "ERRO"
                        error_msg = liq_tx_res.error
                    else:
                        val_liq = ValidaLiquidacao(liq_tx_res.value)
                        if val_liq.is_err:
                            status = "ERRO"
                            error_msg = val_liq.error
                        else:
                            final_obj = val_liq.value
                            stage = "Pagamento"
                            
                            # Pagamento Stage (Batch Optimized)
                            pay_tx_res = PaymentTransaction.build_from_batch(val_liq.value, pagamentos)
                            
                            if pay_tx_res.is_err:
                                status = "ERRO"
                                error_msg = pay_tx_res.error
                                final_obj = val_liq.value # Fallback
                            else:
                                val_pay = ValidaPagamento(pay_tx_res.value)
                                if val_pay.is_err:
                                    status = "ERRO"
                                    error_msg = val_pay.error
                                    final_obj = pay_tx_res.value # Fallback
                                else:
                                    status = "OK"
                                    final_obj = val_pay.value

            processed_results.append({
                "id": contrato.id_contrato,
                "status": status,
                "error": error_msg,
                "stage": stage,
                "object": final_obj,
                "valor": getattr(contrato, "valor_contrato", 0) or 0
            })
            
        offset += current_batch_size
    
    progress_bar.empty()
    cursor.close()
    conn.close()
    return processed_results


# ==========================================
# UI
# ==========================================
st.set_page_config(page_title="Inova - Auditoria", layout="wide")
st.title("📊 Inova Dataview - Auditoria Contínua")

if st.button("🔄 Recarregar Dados (Real-Time)"):
    st.cache_data.clear()
    st.rerun()

with st.spinner("🚀 Processando pipeline OTIMIZADO (Batch Processing)..."):
    data = load_and_process_data_optimized(batch_size=200)

total = len(data)
ok_list = [d for d in data if d["status"] == "OK"]
err_list = [d for d in data if d["status"] == "ERRO"]

col1, col2, col3 = st.columns(3)
col1.metric("Contratos Analisados", total)
col2.metric("✅ Contratos Íntegros", len(ok_list))
col3.metric("🚨 Contratos com Anomalias", len(err_list))

st.markdown("---")

tab_err, tab_ok = st.tabs(["🚨 Contratos com Erro (Auditoria)", "✅ Contratos OK (Estrutura)"])

with tab_err:
    st.header("Auditoria de Erros")
    if not err_list:
        st.success("Nenhuma anomalia detectada nesta amostra!")
    else:
        col_list, col_detail = st.columns([1, 2])
        with col_list:
            selected_err_id = st.selectbox(
                "Selecione Contrato para Auditar:",
                options=[d["id"] for d in err_list],
                format_func=lambda x: f"Contrato #{x}"
            )
        selected_err_item = next((d for d in err_list if d["id"] == selected_err_id), None)
        with col_detail:
            if selected_err_item:
                st.error(f"**Falha Detectada no Estágio: {selected_err_item['stage']}**")
                st.markdown(f"""
                <div style="padding:15px; border-left: 5px solid #ff4b4b; background-color: #f0f2f6;">
                    <h3>🛑 Resultado da Validação</h3>
                    <p style="font-size:16px; font-family:monospace;">{selected_err_item['error']}</p>
                </div>
                """, unsafe_allow_html=True)
                if selected_err_item["object"]:
                     with st.expander("Ver estado parcial do objeto (Debug)"):
                         st.json(obj_to_dict(selected_err_item["object"]))

with tab_ok:
    st.header("Estrutura Interna (Validada)")
    if not ok_list:
        st.warning("Nenhum contrato completo encontrado.")
    else:
        col_list_ok, col_detail_ok = st.columns([1, 2])
        with col_list_ok:
             selected_ok_id = st.selectbox(
                "Selecione Contrato Válido:",
                options=[d["id"] for d in ok_list],
                format_func=lambda x: f"Contrato #{x}"
            )
        selected_ok_item = next((d for d in ok_list if d["id"] == selected_ok_id), None)
        with col_detail_ok:
            if selected_ok_item:
                st.success(f"**Contrato #{selected_ok_item['id']} Integrity Check Passed**")
                st.markdown("Abaixo a visualização da **Estrutura em Memória** completa do objeto transacional:")
                st.json(obj_to_dict(selected_ok_item["object"]))

st.markdown("---")
st.caption("Sistema Inova - Módulo de Auditoria Automatizada | Powered by Python Agentic Framework")
