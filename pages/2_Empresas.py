# pages/2_Empresas.py
import streamlit as st
import pandas as pd
import unicodedata
import re
import io

from src.auth import is_authenticated, current_user
from src.utils_sf import (
    fetch_companies, FQN_MAIN, clear_companies_cache,
    update_company_row,
    STAGE_APRES,
    stage_prefix_for_company,
    list_presentations,
    upload_presentation,
    download_presentation,
    get_presigned_url,
    save_company_comment,
    fetch_company_comments,
    delete_company_row,
    create_company_row,
    fetch_comments_all,
)
from zoneinfo import ZoneInfo
from datetime import datetime
import datetime as _dt

st.set_page_config(page_title="Empresas • Catálogo", layout="wide")

if not is_authenticated():
    st.info("Faça login para acessar.")
    st.stop()

st.title("🏢 Empresas")

# ========= Helpers =========
def _deacc_lower(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower().strip()

def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))

def _split_tokens_cell(cell: str) -> list[str]:
    if cell is None:
        return ["Vazio"]
    raw = str(cell).strip()
    if raw == "":
        return ["Vazio"]
    toks = [t.strip() for t in raw.split(",")]
    out = []
    for t in toks:
        if t == "" or _deacc_lower(t) in {"none", "nan", "nat", "-"}:
            out.append("Vazio")
        else:
            out.append(t)
    return out or ["Vazio"]

def _options_from_col(df: pd.DataFrame, col: str) -> list[str]:
    if not col or col not in df.columns:
        return ["Todos", "Vazio"]
    vals = df[col].tolist()
    all_tokens = set()
    for v in vals:
        for tok in _split_tokens_cell(v):
            all_tokens.add(tok)
    opts = sorted(all_tokens)
    if "Vazio" not in opts:
        opts.append("Vazio")
    return ["Todos"] + opts

def _unique_token_opts(df: pd.DataFrame, col: str | None) -> list[str]:
    """
    Retorna tokens únicos (sem 'Todos'), garantindo 'Vazio' no fim.
    Usa a mesma lógica de _split_tokens_cell().
    """
    if not col or col not in df.columns:
        return ["Vazio"]
    toks = set()
    for v in df[col].tolist():
        for t in _split_tokens_cell(v):
            toks.add(t or "Vazio")
    opts = sorted(toks - {"Vazio"})
    opts.append("Vazio")
    return opts

def _truncate(text: str, n: int = 15) -> str:
    s = str(text or "")
    return s if len(s) <= n else s[:n] + "…"

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm = lambda x: _deacc_lower(x).replace(" ", "").replace("_", "")
    norm_map = {norm(c): c for c in df.columns}
    for cand in candidates:
        key = norm(cand)
        if key in norm_map:
            return norm_map[key]
    return None

def _to_date_safe(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None

_TRUE_SET = {"sim", "s", "true", "t", "1", "y", "yes"}
_FALSE_SET = {"nao", "não", "n", "false", "f", "0", "no"}

def _to_bool_safe(v: str | bool | int | None) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in _TRUE_SET:
        return True
    if s in _FALSE_SET:
        return False
    return False

def _bool_to_simnao(b: bool) -> str:
    return "Sim" if b else "Não"

# ========= Badges =========
_PALETTE = {
    "gray":   ("#f2f2f5", "#2f2f33"),
    "blue":   ("#e8f1ff", "#113a76"),
    "violet": ("#f0e8ff", "#402a77"),
    "indigo": ("#e9edff", "#283a8a"),
    "cyan":   ("#e6fbff", "#0b5461"),
    "pink":   ("#ffe8f3", "#7a274f"),
    "green":  ("#e9f8ee", "#1f6a3a"),
    "orange": ("#fff1e3", "#7a3e11"),
    "red":    ("#ffe8ea", "#7a1f28"),
    "yellow": ("#fff9e6", "#6b5a13"),
}

def _color_css(color: str) -> tuple[str, str]:
    return _PALETTE.get(color, _PALETTE["gray"])

def _segmento_color(v: str) -> str:
    t = _deacc_lower(v or "")
    if "dados" in t: return "blue"
    if "soluç" in t or "soluc" in t: return "violet"
    if "parcer" in t: return "indigo"
    return "gray"

def _atuacao_color(v: str) -> str:
    t = _deacc_lower(v or "")
    if "públic" in t or "public" in t: return "green"
    if "privad" in t: return "orange"
    if "academ" in t: return "cyan"
    return "gray"

def _situacao_color(v: str) -> str:
    t = _deacc_lower(v or "")
    if any(k in t for k in ["ativa", "regular"]): return "green"
    if any(k in t for k in ["inativa", "baixada", "suspensa", "irregular"]): return "red"
    if any(k in t for k in ["pendente", "analise", "análise"]): return "yellow"
    return "gray"

def _badge_html(label: str, color: str = "gray", max_chars: int = 14) -> str:
    bg, fg = _color_css(color)
    lab = str(label or "Vazio")
    if len(lab) > max_chars:
        lab = lab[:max_chars - 1] + "…"
    return (
        f"<span style="
        f"'flex:0 0 auto;display:inline-block;padding:3px 10px;border-radius:999px;"
        f"font-size:12px;border:1px solid rgba(0,0,0,0.08);background:{bg};color:{fg};"
        f"max-width:150px;text-overflow:ellipsis;overflow:hidden;white-space:nowrap;"
        f"margin-right:6px;margin-bottom:0;'>"
        f"{lab}</span>"
    )

def _badges_inline(groups: list[tuple[list[str], callable, int]]) -> None:  # type: ignore
    html = "<div style='display:flex;gap:8px;flex-wrap:nowrap;overflow:hidden;white-space:nowrap;'>"
    for tokens, color_fn, max_k in groups:
        toks = [t or "Vazio" for t in tokens]
        shown = toks[:max_k]
        extra = max(0, len(toks) - len(shown))
        for t in shown:
            html += _badge_html(t, color=color_fn(t))
        if extra > 0:
            html += _badge_html(f"+{extra}", color="gray", max_chars=4)
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)

# ========= Carrega base =========
try:
    df = fetch_companies()
except Exception as e:
    st.error(f"Falha ao carregar dados das empresas: {e}")
    st.stop()

# removemos UPDATED_AT se existir
df_view = df.drop(columns=["UPDATED_AT"], errors="ignore").copy()

# Colunas chave e auxiliares
COL_NOME = _find_col(df_view, ["nome da empresa", "nome_empresa", "nome"])
COL_SIT  = _find_col(df_view, ["situação", "situacao", "status"])
COL_ATU  = _find_col(df_view, ["atuação", "atuacao"])
COL_SEG  = _find_col(df_view, ["segmento"])
COL_CNPJ = _find_col(df_view, ["cnpj"])
COL_ID   = _find_col(df_view, ["id"])
COL_KEY  = COL_ID or COL_CNPJ or COL_NOME
if not COL_KEY:
    st.error("Não encontrei uma coluna-chave (ID/CNPJ/NOME) para atualizar os registros.")
    st.stop()

# ========= TABS =========
tab_catalogo, tab_nova, tab_export = st.tabs(["📚 Catálogo", "➕ Nova Empresa", "📥 Exportar"])

# ========= TAB 2: NOVA EMPRESA =========
with tab_nova:
    st.subheader("Criar nova empresa")
    st.caption("Todos os campos da tabela estão disponíveis abaixo. Preencha o que for necessário.")

    # Lista de todas as colunas da base (já sem UPDATED_AT)
    all_cols = list(df_view.columns)

    # Heurísticas de tipo por nome
    date_fields = {"DATA_DE_ASSINATURA", "INICIO_DA_RENOVACAO_DA_ASSINATURA", "VIGENCIA"}
    bool_fields = {"NDA_ASSINADO"}
    int_fields  = {"VALIDADE_EM_ANOS", "VALIDADE_EM_MESES", "VALIDADE_EM_DIAS"}
    long_text_like = {"DESCRICAO", "RESUMO", "DOCUMENTO", "OBS", "PONTOS_FORTES", "PONTOS_FRACOS"}

    # Mapa normalizado -> real
    norm = lambda x: _deacc_lower(x).replace(" ", "").replace("_", "")
    norm_map = {norm(c): c for c in all_cols}

    def _pretty_label(raw: str) -> str:
        up_map = {"CNPJ": "CNPJ", "NDA_ASSINADO": "NDA Assinado"}
        if raw in up_map: return up_map[raw]
        return raw.replace("_", " ").title()

    with st.form("form_nova_empresa"):
        values: dict[str, object] = {}

        opts_sit = _unique_token_opts(df_view, COL_SIT)
        opts_atu = _unique_token_opts(df_view, COL_ATU)
        # Grade 2 colunas
        cA, cB = st.columns(2)
        for i, col_real in enumerate(all_cols):
            label = _pretty_label(col_real)
            target = cA if (i % 2 == 0) else cB
            col_norm = norm(col_real)

            # Data
            if col_norm in {norm(x) for x in date_fields}:
                with target:
                    d = st.date_input(label, value=None, format="DD/MM/YYYY", key=f"new_{col_real}_date")
                if d:
                    values[col_real] = d.isoformat()

            # Booleano
            elif col_norm in {norm(x) for x in bool_fields}:
                with target:
                    checked = st.checkbox(label, value=False, key=f"new_{col_real}_bool")
                values[col_real] = _bool_to_simnao(checked)

            # Inteiro
            elif col_norm in {norm(x) for x in int_fields}:
                with target:
                    n = st.number_input(label, min_value=0, step=1, format="%d", key=f"new_{col_real}_int")
                values[col_real] = int(n)

            elif COL_SIT and col_norm == norm(COL_SIT):
                with target:
                    sel = st.selectbox(label, options=opts_sit, index=len(opts_sit)-1 if "Vazio" in opts_sit else 0,
                                    key=f"new_{col_real}_sel")
                if sel and sel != "Vazio":
                    values[col_real] = sel

            # 5) **ATUAÇÃO** como dropdown
            elif COL_ATU and col_norm == norm(COL_ATU):
                with target:
                    sel = st.selectbox(label, options=opts_atu, index=len(opts_atu)-1 if "Vazio" in opts_atu else 0,
                                    key=f"new_{col_real}_sel")
                if sel and sel != "Vazio":
                    values[col_real] = sel

            # Texto longo
            elif col_norm in {norm(x) for x in long_text_like}:
                with target:
                    t = st.text_area(label, value="", height=100, key=f"new_{col_real}_ta")
                if t.strip():
                    values[col_real] = t.strip()

            # Texto comum
            else:
                with target:
                    t = st.text_input(label, value="", key=f"new_{col_real}_ti")
                if t.strip():
                    values[col_real] = t.strip()

        st.caption("Dica: Campos não preenchidos não serão enviados (ficam NULL/DEFAULT no banco).")

        c1, c2 = st.columns([1, 3])
        with c1:
            submitted_new = st.form_submit_button("➕ Criar", type="primary", use_container_width=True)
        with c2:
            st.caption("Revise antes de criar — você pode editar depois pelo diálogo de detalhes.")

    if submitted_new:
        try:
            if not values:
                st.warning("Preencha pelo menos um campo.")
            else:
                create_company_row(FQN_MAIN, values)
                clear_companies_cache()
                st.success("Empresa criada com sucesso.")
                st.rerun()
        except Exception as e:
            st.error(f"Falha ao criar empresa: {e}")

# ========= TAB 3: EXPORT =========
with tab_export:
    st.subheader("Exportar planilha Excel")
    if st.button("Gerar arquivo (.xlsx)", type="primary"):
        try:
            # 1) Empresas
            df_empresas = df_view.copy()

            # 2) Comentários (todos)
            df_coment = fetch_comments_all()
            if not df_coment.empty and "CREATED_AT" in df_coment.columns:
                df_coment["CREATED_AT"] = pd.to_datetime(df_coment["CREATED_AT"], errors="coerce")
                df_coment["CREATED_AT_FMT"] = df_coment["CREATED_AT"].dt.strftime("%d/%m/%Y %H:%M")

            # 3) Merge
            key_col = COL_KEY
            left = df_empresas.copy()
            right = df_coment.copy()
            if key_col:
                left[key_col] = left[key_col].astype(str)
            if not right.empty and "EMPRESA_ID" in right.columns:
                right["EMPRESA_ID"] = right["EMPRESA_ID"].astype(str)
            df_join = (
                left.merge(
                    right,
                    left_on=key_col,
                    right_on="EMPRESA_ID",
                    how="left",
                    suffixes=("", "_COM")
                )
                if key_col else left
            )

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                df_empresas.to_excel(writer, index=False, sheet_name="empresas")
                (df_coment if not df_coment.empty else pd.DataFrame()).to_excel(
                    writer, index=False, sheet_name="comentarios"
                )
                df_join.to_excel(writer, index=False, sheet_name="empresas_com_comentarios")
            buf.seek(0)

            st.download_button(
                "⬇️ Download empresas_prospec.xlsx",
                data=buf,
                file_name="empresas_prospec.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.error(f"Falha ao gerar planilha: {e}")

# ========= TAB 1: CATÁLOGO =========
with tab_catalogo:
    # Filtros
    c1, c2, c3,c4 = st.columns([1.3, 1.3,1.3, 1.3 ])
    with c1:
        seg_opts = _options_from_col(df_view, COL_SEG)
        seg_sel = st.selectbox("Segmento:", options=seg_opts, index=0, key="seg_sel")
    with c2:
        atu_opts = _options_from_col(df_view, COL_ATU)
        atu_sel = st.selectbox("Atuação:", options=atu_opts, index=0, key="atu_sel")
    with c3:
        sit_opts = _options_from_col(df_view, COL_SIT)
        sit_sel = st.selectbox("Situação:", options=sit_opts, index=0, key="sit_sel")
    with c4:
        q = st.text_input("Pesquisar por Nome da Empresa ou CNPJ", placeholder="Ex.: ACME ou 12.345.678/0001-99", key="q_busca")

    filtered = df_view
    if COL_SEG and seg_sel and seg_sel != "Todos":
        filtered = filtered[filtered[COL_SEG].apply(lambda s: seg_sel in _split_tokens_cell(s))]
    if COL_ATU and atu_sel and atu_sel != "Todos":
        filtered = filtered[filtered[COL_ATU].apply(lambda s: atu_sel in _split_tokens_cell(s))]
    if COL_SIT and sit_sel and sit_sel != "Todos":
        filtered = filtered[filtered[COL_SIT].apply(lambda s: sit_sel in _split_tokens_cell(s))]

    q_norm = _deacc_lower(q)
    if q_norm:
        def _match_row(r):
            nome = _deacc_lower(r.get(COL_NOME, "")) if COL_NOME else ""
            cnpj = _only_digits(r.get(COL_CNPJ, "")) if COL_CNPJ else ""
            if re.fullmatch(r"\d+", q_norm):
                return _only_digits(q_norm) in cnpj if q_norm else False
            return q_norm in nome
        filtered = filtered[filtered.apply(_match_row, axis=1)]

    total = len(filtered)
    st.caption(f"**Total de Empresas**: {total}")

    if total == 0:
        st.info("Nenhuma empresa encontrada com os filtros atuais.")
    else:
        if COL_NOME:
            filtered = filtered.sort_values(by=COL_NOME, kind="stable")

        CARD_MIN_HEIGHT = 230
        N_COLS = 3
        cols = st.columns(N_COLS)

        for i, (_, row) in enumerate(filtered.iterrows()):
            with cols[i % N_COLS]:
                with st.container(border=True):
                    nome_full = row.get(COL_NOME) if COL_NOME else "(sem nome)"
                    nome_card = _truncate(nome_full, 15)
                    c_head1, c_head2 = st.columns([4, 1])
                    with c_head1:
                        st.markdown(f"### {nome_card}")

                    seg_tokens = _split_tokens_cell(row.get(COL_SEG)) if COL_SEG else []
                    atu_tokens = _split_tokens_cell(row.get(COL_ATU)) if COL_ATU else []
                    sit_token  = [str(row.get(COL_SIT) or "Vazio")] if COL_SIT else []

                    _badges_inline([
                        (seg_tokens, _segmento_color, 1),
                        (atu_tokens, _atuacao_color, 1),
                        (sit_token, _situacao_color, 1),
                    ])

                    st.markdown(f"<div style='min-height:{CARD_MIN_HEIGHT - 140}px'></div>", unsafe_allow_html=True)

                    st.button(
                        "🔎 Detalhes",
                        key=f"btn_det_{i}",
                        type="secondary",
                        use_container_width=True,
                        on_click=lambda r=row: _dialog_detalhes(r.to_dict()),
                    )

# ========= Modal de DETALHES =========
@st.dialog("Detalhes da empresa", width="large")
def _dialog_detalhes(row_dict: dict):
    GROUPS = {
        "Dados Gerais": [
            "NOME_DA_EMPRESA", "CLASSIFICAÇÃO", "SITUAÇÃO", "CNPJ",
            "SEGMENTO", "ATUAÇÃO", "SITE", "CONTATO", "STATUS_ATUAL",
        ],
        "Responsáveis": [
            "APROVACAO", "ANALISE_TECNICA", "RELACIONAMENTO", "AUTOMACAO",
        ],
        "Descrição da Empresa": [
            "DESCRICAO", "RESUMO", "DOCUMENTO",
        ],
        "Datas do Contrato": [
            "DATA_DE_ASSINATURA", "VALIDADE_EM_ANOS", "VALIDADE_EM_MESES",
            "VALIDADE_EM_DIAS", "INICIO_DA_RENOVACAO_DA_ASSINATURA",
            "VIGENCIA", "NDA_ASSINADO",
        ],
        "Dados de Mercado": [
            "OBS", "PONTOS_FORTES", "PONTOS_FRACOS", "CONCORRENTES", "PORTE",
        ],
    }

    def _resolve_col_once(field_key: str) -> str | None:
        return _find_col(df_view, [field_key, field_key.replace("_", " ")])

    nome_full = row_dict.get(COL_NOME) if COL_NOME else row_dict.get("NOME") or "Empresa"
    st.subheader(str(nome_full))

    company_prefix = stage_prefix_for_company(row_dict, COL_CNPJ, COL_NOME)
    main_tabs = st.tabs(["🗂️ Dados", "📎 Apresentações", "💬 Comentários"])

    # ==== TAB 1: Dados ====
    with main_tabs[0]:
        data_tabs = st.tabs(list(GROUPS.keys()))
        changed_updates: dict[str, str | None] = {}

        with st.form("form_edit_empresa"):
            for (tab, (group_title, fields)) in zip(data_tabs, GROUPS.items()):
                with tab:
                    if group_title == "Descrição da Empresa":
                        single = st.container()
                        cA, cB = single, single
                    else:
                        cA, cB = st.columns(2)

                    for idx, f in enumerate(fields):
                        col_real = _resolve_col_once(f)
                        label = f.replace("_", " ").title() if f not in {"CNPJ", "NDA_ASSINADO"} else ("CNPJ" if f=="CNPJ" else "NDA Assinado")
                        val = row_dict.get(col_real) if col_real else None
                        sval = "" if (val is None or (isinstance(val, float) and pd.isna(val))) else str(val)

                        target_col = cA if (idx % 2 == 0) else cB
                        f_norm = norm(f)
                        col_norm = norm(col_real) if col_real else None

                        def _first_token_or_vazio(v):
                            toks = _split_tokens_cell(v)
                            return toks[0] if toks else "Vazio"

                        if group_title == "Datas do Contrato":
                            if f in ("DATA_DE_ASSINATURA", "INICIO_DA_RENOVACAO_DA_ASSINATURA", "VIGENCIA"):
                                d_default = _to_date_safe(val)
                                with target_col:
                                    new_d = st.date_input(
                                        label,
                                        value=d_default if d_default else _dt.date.today(),
                                        format="DD/MM/YYYY"
                                    )
                                new_s = new_d.isoformat() if new_d else None
                                if col_real and (new_s != (d_default.isoformat() if d_default else "")):
                                    changed_updates[col_real] = new_s
                            elif f == "NDA_ASSINADO":
                                with target_col:
                                    checked = st.checkbox(label, value=_to_bool_safe(val))
                                new_s = _bool_to_simnao(checked)
                                if col_real and (new_s.strip() != sval.strip()):
                                    changed_updates[col_real] = new_s
                            else:
                                with target_col:
                                    if len(sval) > 120 or "\n" in sval:
                                        new_val = st.text_area(label, value=sval, height=100)
                                    else:
                                        new_val = st.text_input(label, value=sval)
                                if col_real and (new_val.strip() != sval.strip()):
                                    changed_updates[col_real] = new_val.strip() or None
                        else:
                            if COL_SIT and col_norm == norm(COL_SIT):
                                opts_sit = _unique_token_opts(df_view, COL_SIT)
                                current = _first_token_or_vazio(val)
                                if current and current not in opts_sit:
                                    opts_sit = [current] + [o for o in opts_sit if o != current]
                                with target_col:
                                    new_val = st.selectbox(label, options=opts_sit, index=opts_sit.index(current) if current in opts_sit else 0)
                                if col_real and (str(new_val).strip() != sval.strip()):
                                    changed_updates[col_real] = (None if new_val == "Vazio" else str(new_val).strip())

                            elif COL_ATU and col_norm == norm(COL_ATU):
                                opts_atu = _unique_token_opts(df_view, COL_ATU)
                                current = _first_token_or_vazio(val)
                                if current and current not in opts_atu:
                                    opts_atu = [current] + [o for o in opts_atu if o != current]
                                with target_col:
                                    new_val = st.selectbox(label, options=opts_atu, index=opts_atu.index(current) if current in opts_atu else 0)
                                if col_real and (str(new_val).strip() != sval.strip()):
                                    changed_updates[col_real] = (None if new_val == "Vazio" else str(new_val).strip())

                            else:
                                with target_col:
                                    if len(sval) > 120 or "\n" in sval:
                                        new_val = st.text_area(label, value=sval, height=100)
                                    else:
                                        new_val = st.text_input(label, value=sval)
                                if col_real and (new_val.strip() != sval.strip()):
                                    changed_updates[col_real] = new_val.strip() or None

            c1, c2 = st.columns([1, 3])
            with c1:
                submitted = st.form_submit_button("💾 Salvar alterações", use_container_width=True, type="primary")
            with c2:
                st.caption("Somente campos modificados são atualizados no banco.")

            if submitted:
                if not changed_updates:
                    st.info("Nenhuma alteração detectada.")
                else:
                    try:
                        key_val = row_dict.get(COL_KEY)
                        if key_val is None or (isinstance(key_val, float) and pd.isna(key_val)):
                            st.error("Não foi possível determinar a chave (ID/CNPJ/NOME) para atualizar.")
                        else:
                            update_company_row(FQN_MAIN, COL_KEY, key_val, changed_updates)
                            clear_companies_cache()
                            st.success("Alterações salvas com sucesso.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao salvar alterações: {e}")

    # ==== TAB 2: Apresentações ====
    with main_tabs[1]:
        up_files = st.file_uploader("Enviar arquivos", type=None, accept_multiple_files=True)
        if up_files:
            for uf in up_files:
                try:
                    upload_presentation(company_prefix, uf, uf.name)
                    st.success(f"Enviado: {uf.name}")
                except Exception as e:
                    st.error(f"Falha ao enviar {uf.name}: {e}")

        st.divider()
        try:
            files_df = list_presentations(company_prefix)
        except Exception as e:
            st.error(f"Falha ao listar arquivos: {e}")
            files_df = pd.DataFrame()

        if files_df.empty:
            st.info("Nenhum arquivo disponível.")
        else:
            files_df.columns = [c.lower() for c in files_df.columns]
            for _, fr in files_df.iterrows():
                full_name = str(fr.get('"name"', ""))
                base = full_name.split("/")[-1] if "/" in full_name else full_name
                try:
                    url = get_presigned_url(company_prefix, base, expires_seconds=3600)
                    if url:
                        st.link_button(f"🔗 {base}", url, use_container_width=True)
                    else:
                        data = download_presentation(company_prefix, base)
                        st.download_button(
                            f"⬇️ {base}",
                            data=data,
                            file_name=base,
                            mime="application/octet-stream",
                            use_container_width=True,
                        )
                except Exception as e:
                    st.error(f"Falha ao baixar {base}: {e}")

    # ==== TAB 3: Comentários ====
    with main_tabs[2]:
        user = None
        try:
            user = current_user()
        except Exception:
            pass

        key_val = row_dict.get(COL_KEY)

        st.markdown("**Registrar novo comentário**")
        with st.form("form_comentario"):
            msg = st.text_area("Escreva seu comentário", placeholder="Digite aqui...", height=120)
            submitted_c = st.form_submit_button("💬 Publicar", type="primary", use_container_width=True)

        if submitted_c:
            if not msg.strip():
                st.warning("Comentário vazio.")
            elif key_val is None or (isinstance(key_val, float) and pd.isna(key_val)):
                st.error("Não foi possível associar o comentário à empresa (chave ausente).")
            else:
                try:
                    username = (user or {}).get("username") or (user or {}).get("user") or "desconhecido"
                    display_name = (user or {}).get("name") or username
                    created_at = datetime.now(ZoneInfo("America/Sao_Paulo"))
                    save_company_comment(
                        empresa_id=str(key_val),
                        username=str(username),
                        name=str(display_name),
                        message=str(msg),
                        created_at=created_at,
                    )
                    st.success("Comentário publicado.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao salvar comentário: {e}")

        st.divider()
        st.markdown("**Comentários**")
        try:
            dfc = fetch_company_comments(str(key_val))
        except Exception as e:
            st.error(f"Falha ao carregar comentários: {e}")
            dfc = pd.DataFrame()

        if dfc.empty:
            st.info("Ainda não há comentários.")
        else:
            for _, r in dfc.iterrows():
                nm = str(r.get("NAME") or r.get("USERNAME") or "Usuário")
                msg = str(r.get("MESSAGE") or "")
                ts = r.get("CREATED_AT")
                try:
                    ts_fmt = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M")
                except Exception:
                    ts_fmt = "-"
                st.markdown(f"**{nm}** · {ts_fmt}")
                st.markdown(msg)
                st.markdown("---")
