# app.py
import streamlit as st
import pandas as pd
import duckdb
from uuid import uuid4
from datetime import date, datetime
import unicodedata


# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Atuação de Prospecção de Dados", layout="wide")
st.logo("logo_ibre.png")

# =========================
# MOCK DE USUÁRIOS
# =========================
USERS = {
    "spdo_visual": {"password": "123", "name": "SPDO Visual", "role": "visual"},
    "spdo_admin" :{"password": "123", "name": "SPDO Admin", "role": "admin"}, 
}

# =========================
# DUCKDB
# =========================
DB_PATH = "parcerias.db"
TABLE_MAIN = "PROSPECCAO"
TABLE_COMMENTS = "PROSPECCAO_COMENTARIOS"

# Colunas que esperamos na planilha
EXPECTED_COLS = [
    "Prioridade","Situação","CNPJ","Nome da Empresa","Segmento","Descrição","Resumo","Metodologia",
    "Cobertura","Site","Contatos","Data de Assinatura","Validade em Anos","Validade em Meses",
    "Validade em Dias","Início da Renovação da Assinatura","Vigência","Status","NDA Assinado",
    "Documento","Aprovação","Analise técnica","Relacionamento","Automação","OBS",
    "Pontos Fortes","Pontos Fracos","Concorrentes","Status Atual"
]

# Segmentos para filtro
SEGMENT_FILTERS = ["Todos", "Fornecedor de Soluções", "Fornecedor de Dados", "Potenciais Novos Negócios","Sem Segmento"]
SEGMENT_OPTIONS = [s for s in SEGMENT_FILTERS if s != "Todos"]
SEG_CANON_MAP = {
    "fornecedor de solucoes": "Fornecedor de Soluções",
    "fornecedor de soluções": "Fornecedor de Soluções",
    "fornecedor de dados": "Fornecedor de Dados",
    "potenciais novos negocios": "Potenciais Novos Negócios",
    "potenciais novos negócios": "Potenciais Novos Negócios",
    "sem segmento": "Sem Segmento",
}

def _deaccent_lower(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

SEG_ORDER = {seg: i for i, seg in enumerate(SEGMENT_OPTIONS)}

def normalize_segments(val) -> list[str]:
    """
    Converte o conteúdo da célula "Segmento" em lista de segmentos canônicos.
    - vazio/None/'-': ['Sem Segmento']
    - 'A, B' -> ['A', 'B'] (canônicos)
    - tokens inválidos são ignorados; se nada sobrar -> ['Sem Segmento']
    """
    if val is None:
        return ["Sem Segmento"]
    s = str(val).strip()
    if s == "" or s in {"-", "nan", "NaN"}:
        return ["Sem Segmento"]

    toks = [t.strip() for t in s.split(",")]
    out = []
    for t in toks:
        if not t:
            continue
        key = _deaccent_lower(t)
        canon = SEG_CANON_MAP.get(key)
        if canon:
            out.append(canon)
        else:
            # Se veio um nome fora do dicionário, tente casar literalmente com uma das opções (case-insensitive)
            for opt in SEGMENT_OPTIONS:
                if _deaccent_lower(opt) == key:
                    canon = opt
                    break
            if canon:
                out.append(canon)
            # Caso contrário, ignora silenciosamente
    out = sorted(set(out), key=lambda x: SEG_ORDER.get(x, 999))
    return out or ["Sem Segmento"]

def segments_to_str(segments: list[str]) -> str:
    # Persiste em texto "A, B" (ordem canônica)
    segs = sorted(set(segments), key=lambda x: SEG_ORDER.get(x, 999))
    return ", ".join(segs)
# =========================
# STATE & HOME
# =========================
def ensure_state():
    if "auth" not in st.session_state:
        st.session_state.auth = {"is_auth": False, "user": None}
    if "filter_segmento" not in st.session_state:
        st.session_state.filter_segmento = "Todos"
    if "upload_info" not in st.session_state:
        st.session_state.upload_info = None  # {'file_name':..., 'sheet':...}
    # estado da UI dos filtros por segmento: "select" (mostra botões) | "list" (mostra resultados + voltar)
    if "segment_view" not in st.session_state:
        st.session_state.segment_view = "select"

ensure_state()

def render_public_home():
    st.title("🏗️ Atuação de Prospecção de Dados — FGV IBRE")
    st.caption("Hub interno para prospecção de fornecedores de dados, parcerias e acompanhamentos de NDA.")
    st.markdown(
        """
        **Prospecção Dados** centraliza o ciclo de prospecção:
        - 📥 Importação de planilhas (.xlsx)
        - 🗂️ Filtro por **Segmento**
        - 🔎 Cards com **detalhes em modal**
        - 📝 **NDA / Datas** normalizadas (DD/MM/AAAA)
        - 💬 **Status Atual com comentários** (admin)
        - 🧠 Persistência no **DuckDB**
        """
    )
    st.divider()

# =========================
# HELPERS DE FORMATAÇÃO
# =========================
def _s(val):
    """String segura: vazio/None/NaN/NaT -> '-'."""
    if val is None:
        return "-"
    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "nat"}:
        return "-"
    return s

def _fmt_bool(val):
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"sim", "yes", "true", "1"}:
            return "✅ Sim"
        if v in {"não", "nao", "no", "false", "0"}:
            return "❌ Não"
    if isinstance(val, (bool, int)):
        return "✅ Sim" if bool(val) else "❌ Não"
    return _s(val)

def _fmt_date(val):
    """Normaliza datas variadas (inclui '2025-07-28 00:00:00') -> 'DD/MM/YYYY'; vazio -> '-'."""
    if val is None:
        return "-"
    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "nat", "-"}:
        return "-"
    try:
        if isinstance(val, (datetime, date)):
            return val.strftime("%d/%m/%Y")
        d = pd.to_datetime(s, errors="coerce", dayfirst=False)  # aceita 'YYYY-MM-DD HH:MM:SS'
        if pd.notna(d):
            return d.strftime("%d/%m/%Y")
    except Exception:
        pass
    return _s(val)

def _to_datetime(val):
    """Converte string/data em datetime (NaT se inválido). Aceita 'DD/MM/YYYY' e 'YYYY-MM-DD HH:MM:SS'."""
    if val is None:
        return pd.NaT
    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "nat", "-"}:
        return pd.NaT
    try:
        return pd.to_datetime(s, errors="coerce", dayfirst=True)
    except Exception:
        return pd.NaT

def _calc_status_like_excel(data_ass, inicio_renov, vigencia):
    """Lógica do Excel:
       DataAssinatura vazia -> EM NEGOCIAÇÃO
       InícioRenov > hoje   -> EM VIGÊNCIA
       InícioRenov < hoje e Vigência > hoje -> SOLICITAR RENOVAÇÃO
       Vigência < hoje      -> ATRASADO
       Senão -> '-'
    """
    today = pd.to_datetime(date.today())
    da = _to_datetime(data_ass)
    ir = _to_datetime(inicio_renov)
    vg = _to_datetime(vigencia)

    if pd.isna(da):
        return "EM NEGOCIAÇÃO"
    if pd.notna(ir) and ir > today:
        return "EM VIGÊNCIA"
    if (pd.notna(ir) and ir < today) and (pd.notna(vg) and vg > today):
        return "SOLICITAR RENOVAÇÃO"
    if pd.notna(vg) and vg < today:
        return "ATRASADO"
    return "-"

# =========================
# DUCKDB HELPERS
# =========================
def _connect():
    return duckdb.connect(DB_PATH)

def _ensure_tables():
    con = _connect()
    # Tabela principal
    cols = [
        'ID TEXT',
        *(f'"{c}" TEXT' for c in EXPECTED_COLS),
        'CREATED_AT TEXT',
        'UPDATED_AT TEXT'
    ]
    con.execute(f'CREATE TABLE IF NOT EXISTS {TABLE_MAIN} ({", ".join(cols)});')
    # Tabela de comentários
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_COMMENTS} (
            ID TEXT,
            EMPRESA_ID TEXT,
            USERNAME TEXT,
            NAME TEXT,
            MESSAGE TEXT,
            CREATED_AT TEXT
        );
    ''')
    con.close()

def _import_replace_df(df: pd.DataFrame):
    """Substitui todo o conteúdo da tabela principal pelo df informado."""
    _ensure_tables()
    df2 = df.copy()

    # Garante todas as colunas esperadas
    for c in EXPECTED_COLS:
        if c not in df2.columns:
            df2[c] = "-"

    # Normaliza datas como texto DD/MM/YYYY
    for dc in ["Data de Assinatura", "Início da Renovação da Assinatura", "Vigência"]:
        if dc in df2.columns:
            df2[dc] = df2[dc].apply(_fmt_date)

    # Calcula Status conforme lógica do Excel (sobrescreve o que veio)
    df2["Status"] = df2.apply(
        lambda r: _calc_status_like_excel(
            r.get("Data de Assinatura"), r.get("Início da Renovação da Assinatura"), r.get("Vigência")
        ), axis=1
    )
    
    if "Segmento" in df2.columns:
        df2["Segmento"] = df2["Segmento"].apply(lambda v: segments_to_str(normalize_segments(v)))
    else:
        df2["Segmento"] = segments_to_str(["Sem Segmento"])
    # Preenche vazios com '-'
    df2 = df2.applymap(lambda v: "-" if (v is None or str(v).strip() in {"", "nan", "NaN", "NaT"}) else str(v).strip())

    # Gera IDs e timestamps
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "ID" not in df2.columns:
        df2["ID"] = [uuid4().hex for _ in range(len(df2))]
    if "CREATED_AT" not in df2.columns:
        df2["CREATED_AT"] = now_str
    if "UPDATED_AT" not in df2.columns:
        df2["UPDATED_AT"] = now_str

    # Reordena colunas: ID + EXPECTED + created/updated
    cols_order = ["ID", *EXPECTED_COLS, "CREATED_AT", "UPDATED_AT"]
    df2 = df2.reindex(columns=cols_order)

    # Escreve (replace)
    con = _connect()
    con.execute(f"DELETE FROM {TABLE_MAIN};")
    con.register("df_up", df2)
    cols_q = ", ".join(f'"{c}"' for c in cols_order)
    con.execute(f'INSERT INTO {TABLE_MAIN} ({cols_q}) SELECT {cols_q} FROM df_up;')
    con.close()
    return len(df2)

def _fetch_df(segmento: str | None = None) -> pd.DataFrame:
    _ensure_tables()
    con = _connect()
    try:
        df = con.execute(f'SELECT * FROM {TABLE_MAIN} ORDER BY "Nome da Empresa";').df()
    finally:
        con.close()

    # cria uma coluna auxiliar com a lista de segmentos
    if "Segmento" not in df.columns:
        df["Segmento"] = "-"

    df["_segments"] = df["Segmento"].apply(normalize_segments)

    if segmento and segmento != "Todos":
        df = df[df["_segments"].apply(lambda lst: segmento in lst)]

    return df.drop(columns=["_segments"], errors="ignore")

def _update_record(rec_id: str, updates: dict):
    """Atualiza campos do registro (por ID)."""
    if not rec_id:
        raise ValueError("ID obrigatório.")
    if not updates:
        return
    _ensure_tables()
    updates = {k: v for k, v in updates.items() if k in EXPECTED_COLS or k in {"CREATED_AT","UPDATED_AT"}}
    updates["UPDATED_AT"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    set_clause = ", ".join([f'"{k}" = ?' for k in updates.keys()])
    params = list(updates.values()) + [rec_id]
    con = _connect()
    con.execute(f'UPDATE {TABLE_MAIN} SET {set_clause} WHERE ID = ?;', params)
    con.close()

def _insert_comment(empresa_id: str, username: str, name: str, message: str):
    if not message.strip():
        return
    _ensure_tables()
    con = _connect()
    con.execute(
        f'INSERT INTO {TABLE_COMMENTS} (ID, EMPRESA_ID, USERNAME, NAME, MESSAGE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?);',
        [uuid4().hex, empresa_id, username, name, message.strip(), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    )
    con.close()

def _fetch_comments(empresa_id: str) -> pd.DataFrame:
    _ensure_tables()
    con = _connect()
    try:
        df = con.execute(
            f'SELECT * FROM {TABLE_COMMENTS} WHERE EMPRESA_ID = ? ORDER BY CREATED_AT DESC;',
            [empresa_id]
        ).df()
    finally:
        con.close()
    return df

# =========================
# MODAL DE DETALHES (com abas + edição por role)
# =========================
def open_company_dialog(rec: dict, is_admin: bool, current_user: dict):
    titulo = f"Detalhes — {_s(rec.get('Nome da Empresa'))}"

    @st.dialog(titulo, width="large")
    def _dialog():
        st.caption(
            f"Segmento: **{_s(rec.get('Segmento'))}** • "
            f"CNPJ: **{_s(rec.get('CNPJ'))}** • "
            f"Prioridade: **{_s(rec.get('Prioridade'))}**"
        )
        st.divider()

        if is_admin:
            # ========================
            # FORM DE EDIÇÃO (SEM COMENTÁRIOS)
            # ========================
            with st.form(f"form_edit_{rec['ID']}"):
                tab_geral, tab_datas, tab_prod, tab_contatos, tab_obs, tab_status = st.tabs(
                    ["📌 Geral", "📅 Datas", "🧪 Produto/Cobertura",
                     "🔗 Contatos & Docs", "🧭 Observações & Mercado",
                     "🗒️ Status Atual & Comentários"]
                )

                with tab_geral:
                    col1, col2 = st.columns(2)
                    with col1:
                        prioridade = st.select_slider(
                            "Prioridade (0 = mais alta, 3 = menos)",
                            options=[0, 1, 2, 3],
                            value=int(rec.get("Prioridade")) if _s(rec.get("Prioridade")).isdigit() else 3,
                            format_func=lambda x: {0: "0 (mais alta)", 1: "1", 2: "2", 3: "3 (menos)"}.get(x, str(x)),
                        )
                        situacao = st.text_input("Situação", value=_s(rec.get("Situação")))
                        status = st.text_input("Status (calculado automaticamente ao salvar se datas mudarem)", value=_s(rec.get("Status")))
                        status_atual = st.text_area("Status Atual (resumo)", value=_s(rec.get("Status Atual")), height=80)
                        nda_ass = st.text_input("NDA Assinado", value=_s(rec.get("NDA Assinado")))
                        aprov = st.text_input("Aprovação", value=_s(rec.get("Aprovação")))
                    with col2:
                        nome = st.text_input("Nome da Empresa", value=_s(rec.get("Nome da Empresa")))
                        cnpj = st.text_input("CNPJ", value=_s(rec.get("CNPJ")))
                        seg_pre = normalize_segments(rec.get("Segmento"))
                        segmentos_ms = st.multiselect("Segmentos", options=SEGMENT_OPTIONS, default=seg_pre)
                        relac = st.text_input("Relacionamento", value=_s(rec.get("Relacionamento")))
                        auto = st.text_input("Automação", value=_s(rec.get("Automação")))
                        doc = st.text_input("Documento", value=_s(rec.get("Documento")))

                with tab_datas:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        data_ass = st.text_input("Data de Assinatura (DD/MM/AAAA)", value=_s(rec.get("Data de Assinatura")))
                    with col2:
                        inicio_renov = st.text_input("Início da Renovação da Assinatura (DD/MM/AAAA)", value=_s(rec.get("Início da Renovação da Assinatura")))
                    with col3:
                        vigencia = st.text_input("Vigência (DD/MM/AAAA)", value=_s(rec.get("Vigência")))
                    col4, col5, col6 = st.columns(3)
                    with col4:
                        val_anos = st.text_input("Validade em Anos", value=_s(rec.get("Validade em Anos")))
                    with col5:
                        val_meses = st.text_input("Validade em Meses", value=_s(rec.get("Validade em Meses")))
                    with col6:
                        val_dias = st.text_input("Validade em Dias", value=_s(rec.get("Validade em Dias")))

                with tab_prod:
                    col1, col2 = st.columns(2)
                    with col1:
                        metodologia = st.text_area("Metodologia", value=_s(rec.get("Metodologia")), height=120)
                        cobertura = st.text_area("Cobertura", value=_s(rec.get("Cobertura")), height=120)
                        resumo = st.text_area("Resumo", value=_s(rec.get("Resumo")), height=120)
                    with col2:
                        descricao = st.text_area("Descrição", value=_s(rec.get("Descrição")), height=180)
                        # OBS agora na aba Observações & Mercado

                with tab_contatos:
                    site = st.text_input("Site", value=_s(rec.get("Site")))
                    contatos = st.text_area("Contatos", value=_s(rec.get("Contatos")), height=120)
                    analise_tec = st.text_input("Analise técnica", value=_s(rec.get("Analise técnica")))

                with tab_obs:
                    obs = st.text_area("OBS", value=_s(rec.get("OBS")), height=120)
                    pts_fortes = st.text_area("Pontos Fortes", value=_s(rec.get("Pontos Fortes")), height=100)
                    pts_fracos = st.text_area("Pontos Fracos", value=_s(rec.get("Pontos Fracos")), height=100)
                    conc = st.text_area("Concorrentes", value=_s(rec.get("Concorrentes")), height=100)

                with tab_status:
                    st.markdown("**Comentários:**")
                    com_df = _fetch_comments(rec["ID"])
                    if com_df.empty:
                        st.caption("Sem comentários ainda.")
                    else:
                        for _, crow in com_df.iterrows():
                            ts = _s(crow.get("CREATED_AT"))
                            nm = _s(crow.get("NAME"))
                            msg = _s(crow.get("MESSAGE"))
                            st.markdown(f"🗨️ **{nm}** · _{ts}_")
                            st.markdown(f"> {msg}")
                            st.markdown("---")
                    # ⛔️ Nada de input/checkbox aqui dentro do form.

                save_btn = st.form_submit_button("💾 Salvar alterações", use_container_width=True)
                if save_btn:
                    data_ass_n = _fmt_date(data_ass)
                    inicio_renov_n = _fmt_date(inicio_renov)
                    vigencia_n = _fmt_date(vigencia)
                    status_calc = _calc_status_like_excel(data_ass_n, inicio_renov_n, vigencia_n)

                    updates = {
                        "Prioridade": str(prioridade),
                        "Situação": _s(situacao),
                        "CNPJ": _s(cnpj),
                        "Nome da Empresa": _s(nome),
                        "Segmento": segments_to_str(segmentos_ms),
                        "Descrição": _s(descricao),
                        "Resumo": _s(resumo),
                        "Metodologia": _s(metodologia),
                        "Cobertura": _s(cobertura),
                        "Site": _s(site),
                        "Contatos": _s(contatos),
                        "Data de Assinatura": data_ass_n,
                        "Validade em Anos": _s(val_anos),
                        "Validade em Meses": _s(val_meses),
                        "Validade em Dias": _s(val_dias),
                        "Início da Renovação da Assinatura": inicio_renov_n,
                        "Vigência": vigencia_n,
                        "Status": status_calc,
                        "NDA Assinado": _s(nda_ass),
                        "Documento": _s(doc),
                        "Aprovação": _s(aprov),
                        "Analise técnica": _s(analise_tec),
                        "Relacionamento": _s(relac),
                        "Automação": _s(auto),
                        "OBS": _s(obs),
                        "Pontos Fortes": _s(pts_fortes),
                        "Pontos Fracos": _s(pts_fracos),
                        "Concorrentes": _s(conc),
                        "Status Atual": _s(status_atual),
                    }
                    if not segmentos_ms:
                        st.error("Selecione pelo menos **um Segmento**.")
                        return
                    try:
                        _update_record(rec["ID"], updates)
                        st.success("Registro atualizado com sucesso!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao salvar: {e}")

            # ========================
            # INPUT DE COMENTÁRIO (FORA DO FORM) — ENTER PARA ENVIAR
            # ========================
            st.markdown("---")
            st.markdown("**Adicionar comentário (pressione Enter para enviar):**")

            # callback que insere e limpa
            def _submit_comment():
                key = f"novo_coment_{rec['ID']}"
                txt = st.session_state.get(key, "").strip()
                if txt:
                    _insert_comment(
                        empresa_id=rec["ID"],
                        username=current_user["username"],
                        name=current_user["name"],
                        message=txt
                    )
                    st.session_state[key] = ""  # limpa o campo
                    st.rerun()

            st.text_input(
                "Comentário",
                value="",
                key=f"novo_coment_{rec['ID']}",
                placeholder="Escreva seu comentário e pressione Enter",
                on_change=_submit_comment,
            )

        else:
            # VISUAL
            tab_geral, tab_datas, tab_prod, tab_contatos, tab_obs, tab_status = st.tabs(
                ["📌 Geral", "📅 Datas", "🧪 Produto/Cobertura",
                 "🔗 Contatos & Docs", "🧭 Observações & Mercado",
                 "🗒️ Status Atual & Comentários"]
            )
            with tab_geral:
                st.markdown(f"- **Prioridade:** {_s(rec.get('Prioridade'))}")
                st.markdown(f"- **Situação:** {_s(rec.get('Situação'))}")
                st.markdown(f"- **Status:** {_s(rec.get('Status'))}")
                st.markdown(f"- **Status Atual:** {_s(rec.get('Status Atual'))}")
                st.markdown(f"- **Vigência:** {_s(rec.get('Vigência'))}")
                st.markdown(f"- **NDA Assinado:** {_fmt_bool(rec.get('NDA Assinado'))}")
                st.markdown(f"- **Aprovação:** {_s(rec.get('Aprovação'))}")
                st.markdown(f"- **Analise técnica:** {_s(rec.get('Analise técnica'))}")
                st.markdown(f"- **Relacionamento:** {_s(rec.get('Relacionamento'))}")
                st.markdown(f"- **Automação:** {_s(rec.get('Automação'))}")
            with tab_datas:
                st.markdown(f"- **Data de Assinatura:** {_fmt_date(rec.get('Data de Assinatura'))}")
                st.markdown(f"- **Início da Renovação da Assinatura:** {_fmt_date(rec.get('Início da Renovação da Assinatura'))}")
                st.markdown(f"- **Validade (Anos/Meses/Dias):** {_s(rec.get('Validade em Anos'))} / {_s(rec.get('Validade em Meses'))} / {_s(rec.get('Validade em Dias'))}")
            with tab_prod:
                st.markdown(f"- **Metodologia:** {_s(rec.get('Metodologia'))}")
                st.markdown(f"- **Cobertura:** {_s(rec.get('Cobertura'))}")
                st.markdown(f"- **Descrição:** {_s(rec.get('Descrição'))}")
                st.markdown(f"- **Resumo:** {_s(rec.get('Resumo'))}")
            with tab_contatos:
                site = _s(rec.get('Site'))
                if site not in {"-", ""}:
                    st.markdown(f"- **Site:** [{site}]({site})")
                else:
                    st.markdown(f"- **Site:** -")
                st.markdown(f"- **Contatos:** {_s(rec.get('Contatos'))}")
                st.markdown(f"- **Documento:** {_s(rec.get('Documento'))}")
            with tab_obs:
                st.markdown(f"- **OBS:** {_s(rec.get('OBS'))}")
                st.markdown(f"- **Pontos Fortes:** {_s(rec.get('Pontos Fortes'))}")
                st.markdown(f"- **Pontos Fracos:** {_s(rec.get('Pontos Fracos'))}")
                st.markdown(f"- **Concorrentes:** {_s(rec.get('Concorrentes'))}")
            with tab_status:
                st.markdown(f"**Status Atual (resumo):** {_s(rec.get('Status Atual'))}")
                st.markdown("---")
                st.markdown("**Comentários:**")
                com_df = _fetch_comments(rec["ID"])
                if com_df.empty:
                    st.caption("Sem comentários.")
                else:
                    for _, crow in com_df.iterrows():
                        ts = _s(crow.get("CREATED_AT"))
                        nm = _s(crow.get("NAME"))
                        msg = _s(crow.get("MESSAGE"))
                        st.markdown(f"🗨️ **{nm}** · _{ts}_")
                        st.markdown(f"> {msg}")
                        st.markdown("---")

    _dialog()

# =========================
# SIDEBAR (LOGIN + UPLOAD)
# =========================
with st.sidebar:
    st.subheader("🔐 Acesso")
    if not st.session_state.auth["is_auth"]:
        with st.form("login_form"):
            username = st.text_input("Usuário", placeholder="ex: spdo_nome")
            password = st.text_input("Senha", type="password")
            ok = st.form_submit_button("Entrar", use_container_width=True)
            if ok:
                u = USERS.get(username)
                if u and password == u["password"]:
                    st.session_state.auth = {"is_auth": True, "user": {"username": username, **u}}
                    st.success(f"Bem-vindo, {u['name']}!")
                    st.rerun()
                else:
                    st.error("Usuário ou senha inválidos.")
    else:
        user = st.session_state.auth["user"]
        st.success(f"Logado como **{user['name']}** ({user['role']})")
        if st.button("Sair", use_container_width=True):
            st.session_state.clear()
            ensure_state()
            st.rerun()

    if st.session_state.auth["is_auth"]:
        st.markdown("---")
        st.markdown("### 📄 Importar Excel")
        uploaded = st.file_uploader("Selecione um .xlsx", type=["xlsx"], key="uploader_xlsx_sidebar")

        if uploaded:
            try:
                xls = pd.ExcelFile(uploaded, engine="openpyxl")
                sheet_names = xls.sheet_names
                chosen_sheet = "Dados" if "Dados" in sheet_names else sheet_names[0]

                df_view = pd.read_excel(xls, sheet_name=chosen_sheet, dtype=str)
                df_view.columns = df_view.columns.map(lambda c: str(c).strip())

                # Garante colunas esperadas
                for c in EXPECTED_COLS:
                    if c not in df_view.columns:
                        df_view[c] = "-"

                # Normalizações de datas (para exibição e status)
                for dc in ["Data de Assinatura", "Início da Renovação da Assinatura", "Vigência"]:
                    if dc in df_view.columns:
                        df_view[dc] = df_view[dc].apply(_fmt_date)

                # Calcula Status conforme a lógica do Excel
                df_view["Status"] = df_view.apply(
                    lambda r: _calc_status_like_excel(
                        r.get("Data de Assinatura"), r.get("Início da Renovação da Assinatura"), r.get("Vigência")
                    ), axis=1
                )

                # Vazio -> "-"
                df_view = df_view.applymap(lambda v: "-" if (v is None or str(v).strip() in {"", "nan", "NaN", "NaT"}) else str(v).strip())

                # Persiste no DuckDB (substitui tudo)
                n = _import_replace_df(df_view)

                st.session_state.upload_info = {"file_name": uploaded.name, "sheet": chosen_sheet, "rows": n}
                st.success(f"Importação concluída: {n} linha(s) para a aba '{chosen_sheet}'.")
                st.rerun()
            except Exception as e:
                st.error(f"Não foi possível ler o XLSX. Detalhes: {e}")

# Somente login
if not st.session_state.auth["is_auth"]:
    render_public_home()
    st.stop()

def _insert_record_main(record: dict) -> str:
    """Insere UMA empresa na tabela principal e retorna o ID."""
    _ensure_tables()

    # Monta linha com todas as colunas esperadas
    row = {c: _s(record.get(c)) for c in EXPECTED_COLS}

    # Normaliza datas (DD/MM/AAAA)
    for dc in ["Data de Assinatura", "Início da Renovação da Assinatura", "Vigência"]:
        row[dc] = _fmt_date(row.get(dc))

    # Calcula Status pela lógica estilo Excel
    row["Status"] = _calc_status_like_excel(
        row.get("Data de Assinatura"),
        row.get("Início da Renovação da Assinatura"),
        row.get("Vigência"),
    )

    # Campos obrigatórios adicionais
    rec_id = uuid4().hex
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Preenche vazios com '-'
    row = {
        k: ("-" if (v is None or str(v).strip() in {"", "nan", "NaN", "NaT"})
            else str(v).strip())
        for k, v in row.items()
    }

    # Monta SQL (sem f-string aninhada)
    cols_order = ["ID", *EXPECTED_COLS, "CREATED_AT", "UPDATED_AT"]
    cols_sql = ", ".join([f'"{c}"' for c in cols_order])  # <- monta fora
    placeholders = ", ".join(["?"] * len(cols_order))
    params = [rec_id, *[row[c] for c in EXPECTED_COLS], now_str, now_str]

    con = _connect()
    con.execute(f'INSERT INTO {TABLE_MAIN} ({cols_sql}) VALUES ({placeholders});', params)
    con.close()
    return rec_id


def open_create_dialog(default_segmento: str | None, current_user: dict):
    """Modal para criar nova empresa (admin)."""
    @st.dialog("✚ Nova empresa", width="large")
    def _dialog():
        st.caption("Preencha os campos e clique em **Salvar**.")
        with st.form("form_nova_empresa"):
            # Mesma estrutura de abas do detalhamento
            tab_geral, tab_datas, tab_prod, tab_contatos, tab_obs = st.tabs(
                ["📌 Geral", "📅 Datas", "🧪 Produto/Cobertura", "🔗 Contatos & Docs", "🧭 Observações & Mercado"]
            )

            with tab_geral:
                col1, col2 = st.columns(2)
                with col1:
                    nome = st.text_input("Nome da Empresa", value="")
                    cnpj = st.text_input("CNPJ", value="")
                    segmento_ms_default = [default_segmento] if default_segmento in SEGMENT_OPTIONS else []
                    segmentos_ms = st.multiselect("Segmentos", options=SEGMENT_OPTIONS, default=segmento_ms_default)
                    prioridade = st.select_slider("Prioridade (0 = sem prioridade, 3 = alta)", options=[0, 1, 2, 3], value=0)
                    situacao = st.text_input("Situação", value="-")
                    status_atual = st.text_area("Status Atual (resumo)", value="-", height=80)
                with col2:
                    nda_ass = st.text_input("NDA Assinado", value="-")
                    aprov = st.text_input("Aprovação", value="-")
                    relac = st.text_input("Relacionamento", value="-")
                    auto = st.text_input("Automação", value="-")
                    doc = st.text_input("Documento", value="-")


            with tab_datas:
                col1, col2, col3 = st.columns(3)
                with col1:
                    data_ass = st.text_input("Data de Assinatura (DD/MM/AAAA)", value="-")
                with col2:
                    inicio_renov = st.text_input("Início da Renovação da Assinatura (DD/MM/AAAA)", value="-")
                with col3:
                    vigencia = st.text_input("Vigência (DD/MM/AAAA)", value="-")
                col4, col5, col6 = st.columns(3)
                with col4:
                    val_anos = st.text_input("Validade em Anos", value="-")
                with col5:
                    val_meses = st.text_input("Validade em Meses", value="-")
                with col6:
                    val_dias = st.text_input("Validade em Dias", value="-")

            with tab_prod:
                col1, col2 = st.columns(2)
                with col1:
                    metodologia = st.text_area("Metodologia", value="-", height=100)
                    cobertura = st.text_area("Cobertura", value="-", height=100)
                    resumo = st.text_area("Resumo", value="-", height=100)
                with col2:
                    descricao = st.text_area("Descrição", value="-", height=160)
                    # OBS foi movido para a aba Observações & Mercado

            with tab_contatos:
                site = st.text_input("Site", value="-")
                contatos = st.text_area("Contatos", value="-", height=80)
                analise_tec = st.text_input("Analise técnica", value="-")
                # Pontos Fortes/Fracos/Concorrentes foram movidos para a aba Observações & Mercado

            # >>> Aba Observações & Mercado (agora com campos)
            with tab_obs:
                obs = st.text_area("OBS", value="-", height=100)
                pts_fortes = st.text_area("Pontos Fortes", value="-", height=80)
                pts_fracos = st.text_area("Pontos Fracos", value="-", height=80)
                conc = st.text_area("Concorrentes", value="-", height=80)

            save = st.form_submit_button("💾 Salvar empresa", type="primary", use_container_width=True)
            if not segmentos_ms:
                st.error("Selecione pelo menos **um Segmento**.")
                return
            if save:
                if not nome.strip():
                    st.error("O campo **Nome da Empresa** é obrigatório.")
                    return

                record = {
                    "Prioridade": str(prioridade),
                    "Situação": _s(situacao),
                    "CNPJ": _s(cnpj),
                    "Nome da Empresa": _s(nome),
                    "Segmento": segments_to_str(segmentos_ms),
                    "Descrição": _s(descricao),
                    "Resumo": _s(resumo),
                    "Metodologia": _s(metodologia),
                    "Cobertura": _s(cobertura),
                    "Site": _s(site),
                    "Contatos": _s(contatos),
                    "Data de Assinatura": _s(data_ass),
                    "Validade em Anos": _s(val_anos),
                    "Validade em Meses": _s(val_meses),
                    "Validade em Dias": _s(val_dias),
                    "Início da Renovação da Assinatura": _s(inicio_renov),
                    "Vigência": _s(vigencia),
                    "Status": "-",  # será recalculado no insert
                    "NDA Assinado": _s(nda_ass),
                    "Documento": _s(doc),
                    "Aprovação": _s(aprov),
                    "Analise técnica": _s(analise_tec),
                    "Relacionamento": _s(relac),
                    "Automação": _s(auto),
                    # Observações & Mercado
                    "OBS": _s(obs),
                    "Pontos Fortes": _s(pts_fortes),
                    "Pontos Fracos": _s(pts_fracos),
                    "Concorrentes": _s(conc),
                    # Status atual
                    "Status Atual": _s(status_atual),
                }

                try:
                    new_id = _insert_record_main(record)
                    st.success("Empresa criada com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao criar empresa: {e}")

    _dialog()

# =========================
# CONTEÚDO PRINCIPAL
# =========================
user = st.session_state.auth["user"]
is_admin = (user["role"] == "admin")

st.title("🏗️ Atuação de Prospecção de Dados")

# ====== Filtros por Segmento (com modo seleção/lista) ======
if st.session_state.segment_view == "select":
    st.subheader("Filtros por Segmento")
    bt_cols = st.columns(len(SEGMENT_FILTERS))
    for i, seg in enumerate(SEGMENT_FILTERS):
        with bt_cols[i]:
            if st.button(seg, use_container_width=True, key=f"seg-{seg}"):
                st.session_state.filter_segmento = seg
                st.session_state.segment_view = "list"
                st.rerun()
    st.caption("Escolha um segmento para visualizar os resultados.")
    st.stop()  # não mostra lista enquanto não escolher um filtro

# Modo LISTA (mostra resultados do filtro + botão Voltar)
st.subheader(f"Resultados — Segmento: {st.session_state.filter_segmento}")
c_voltar, c_novo = st.columns(2)
with c_voltar:
    if st.button("⬅️ Voltar aos filtros", use_container_width=True, key="btn-voltar-segmentos"):
        st.session_state.segment_view = "select"
        st.session_state.filter_segmento = "Todos"
        st.rerun()

with c_novo:
    if is_admin and st.button("✚ Criar empresa", use_container_width=True, key="btn-criar-empresa"):
        # Passa o segmento atual (exceto "Todos") como padrão
        default_seg = st.session_state.filter_segmento if st.session_state.filter_segmento != "Todos" else None
        open_create_dialog(default_segmento=default_seg, current_user=user)

st.divider()

# Carrega do DB conforme filtro atual
df_all = _fetch_df(st.session_state.filter_segmento)

if df_all.empty:
    st.info("Nenhum registro encontrado. Importe um Excel na barra lateral.")
else:
    st.caption(f"{len(df_all)} registro(s). Clique em um card para ver detalhes.")
    cols = st.columns(3)
    for i, (_, row) in enumerate(df_all.iterrows()):
        with cols[i % 3]:
            with st.container(border=True):
                nome = _s(row.get("Nome da Empresa"))
                seg  = segments_to_str(normalize_segments(row.get("Segmento")))
                stat = _s(row.get("Status"))
                vig  = _s(row.get("Vigência"))
                prio = _s(row.get("Prioridade"))
                st.markdown(f"### {nome}")
                st.caption(f"Segmento: **{seg}** • Status: **{stat}**")
                st.caption(f"Vigência: **{vig}** • Prioridade: **{prio}**")
                if st.button("Ver detalhes", key=f"btn-det-{row['ID']}", use_container_width=True):
                    open_company_dialog(row.to_dict(), is_admin=is_admin, current_user=user)
