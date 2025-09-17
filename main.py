# app.py
import streamlit as st
import pandas as pd
from uuid import uuid4
from datetime import date, datetime
import unicodedata
import hashlib

# Snowflake
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

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
# SNOWFLAKE (TABELAS ALVO)
# =========================
FQN_MAIN     = 'BASES_SPDO.DB_APP_PROSPEC_DATA.TB_EMPRESAS'
FQN_COMMENTS = 'BASES_SPDO.DB_APP_PROSPEC_DATA.TB_EMPRESAS_COMENTARIOS'

def get_session() -> Session:
    # 1) Streamlit in Snowflake
    try:
        return get_active_session()
    except Exception:
        pass
    # 2) Local / Server com .streamlit/secrets.toml
    return Session.builder.configs(st.secrets["snowflake"]).create()

sf_session = get_session()

def _sf(q: str):
    return sf_session.sql(q)

def _sf_escape(v: str) -> str:
    return str(v).replace("'", "''")

# =========================
# ESQUEMA LIMPO (MAIÚSCULO, SEM ACENTOS)
# =========================
EXPECTED_COLS = [
    "PRIORIDADE","SITUACAO","CNPJ","NOME_EMPRESA","SEGMENTO","DESCRICAO","RESUMO","METODOLOGIA",
    "COBERTURA","SITE","CONTATOS","DATA_ASSINATURA","VAL_ANOS","VAL_MESES","VAL_DIAS",
    "INICIO_RENOV","VIGENCIA","STATUS","NDA_ASSINADO","DOCUMENTO","APROVACAO","ANALISE_TECNICA",
    "RELACIONAMENTO","AUTOMACAO","OBS","PONTOS_FORTES","PONTOS_FRACOS","CONCORRENTES","STATUS_ATUAL"
]
DATE_COLS = ["DATA_ASSINATURA","INICIO_RENOV","VIGENCIA"]

# mapeia cabeçalhos antigos (planilha) -> nomes limpos da tabela
ORIGINAL_TO_CANON = {
    "Prioridade": "PRIORIDADE",
    "Situação": "SITUACAO",
    "CNPJ": "CNPJ",
    "Nome da Empresa": "NOME_EMPRESA",
    "Segmento": "SEGMENTO",
    "Descrição": "DESCRICAO",
    "Resumo": "RESUMO",
    "Metodologia": "METODOLOGIA",
    "Cobertura": "COBERTURA",
    "Site": "SITE",
    "Contatos": "CONTATOS",
    "Data de Assinatura": "DATA_ASSINATURA",
    "Validade em Anos": "VAL_ANOS",
    "Validade em Meses": "VAL_MESES",
    "Validade em Dias": "VAL_DIAS",
    "Início da Renovação da Assinatura": "INICIO_RENOV",
    "Vigência": "VIGENCIA",
    "Status": "STATUS",
    "NDA Assinado": "NDA_ASSINADO",
    "Documento": "DOCUMENTO",
    "Aprovação": "APROVACAO",
    "Analise técnica": "ANALISE_TECNICA",
    "Relacionamento": "RELACIONAMENTO",
    "Automação": "AUTOMACAO",
    "OBS": "OBS",
    "Pontos Fortes": "PONTOS_FORTES",
    "Pontos Fracos": "PONTOS_FRACOS",
    "Concorrentes": "CONCORRENTES",
    "Status Atual": "STATUS_ATUAL",
}

# Labels amigáveis (UI) para cada coluna limpa
LABEL = {
    "PRIORIDADE": "Prioridade",
    "SITUACAO": "Situação",
    "CNPJ": "CNPJ",
    "NOME_EMPRESA": "Nome da Empresa",
    "SEGMENTO": "Segmento",
    "DESCRICAO": "Descrição",
    "RESUMO": "Resumo",
    "METODOLOGIA": "Metodologia",
    "COBERTURA": "Cobertura",
    "SITE": "Site",
    "CONTATOS": "Contatos",
    "DATA_ASSINATURA": "Data de Assinatura",
    "VAL_ANOS": "Validade em Anos",
    "VAL_MESES": "Validade em Meses",
    "VAL_DIAS": "Validade em Dias",
    "INICIO_RENOV": "Início da Renovação da Assinatura",
    "VIGENCIA": "Vigência",
    "STATUS": "Status",
    "NDA_ASSINADO": "NDA Assinado",
    "DOCUMENTO": "Documento",
    "APROVACAO": "Aprovação",
    "ANALISE_TECNICA": "Analise técnica",
    "RELACIONAMENTO": "Relacionamento",
    "AUTOMACAO": "Automação",
    "OBS": "OBS",
    "PONTOS_FORTES": "Pontos Fortes",
    "PONTOS_FRACOS": "Pontos Fracos",
    "CONCORRENTES": "Concorrentes",
    "STATUS_ATUAL": "Status Atual",
}

# =========================
# SEGMENTAÇÃO (sem acentos)
# =========================
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
            for opt in SEGMENT_OPTIONS:
                if _deaccent_lower(opt) == key:
                    canon = opt
                    break
            if canon:
                out.append(canon)
    out = sorted(set(out), key=lambda x: SEG_ORDER.get(x, 999))
    return out or ["Sem Segmento"]

def segments_to_str(segments: list[str]) -> str:
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
        st.session_state.upload_info = None
    if "segment_view" not in st.session_state:
        st.session_state.segment_view = "select"
    # NOVO: controle do uploader e arquivos já processados
    if "upload_key" not in st.session_state:
        st.session_state.upload_key = 0
    if "processed_hashes" not in st.session_state:
        st.session_state.processed_hashes = set()

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
        - 🧠 Persistência no **Snowflake**
        """
    )
    st.divider()

# =========================
# HELPERS DE FORMATAÇÃO
# =========================
def _s(val):
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
    if val is None:
        return "-"
    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "nat", "-"}:
        return "-"
    try:
        if isinstance(val, (datetime, date)):
            return val.strftime("%d/%m/%Y")
        d = pd.to_datetime(s, errors="coerce", dayfirst=False)
        if pd.notna(d):
            return d.strftime("%d/%m/%Y")
    except Exception:
        pass
    return _s(val)

def _to_datetime(val):
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
# HELPERS (SNOWFLAKE)
# =========================
def import_to_sf_append(df: pd.DataFrame) -> int:
    """
    Sempre adiciona (APPEND) as linhas da planilha em {FQN_MAIN}.
    Não cria tabela, não trunca, não sobrescreve.
    """
    df2 = df.copy()

    # 1) headers da planilha -> nomes limpos (MAIÚSCULO)
    rename_map = {c: ORIGINAL_TO_CANON.get(c, c) for c in df2.columns}
    df2.rename(columns=rename_map, inplace=True)

    # 2) garante todas as colunas esperadas
    for c in EXPECTED_COLS:
        if c not in df2.columns:
            df2[c] = None if c in DATE_COLS else "-"

    # 3) datas -> datetime.date (None se inválido)
    for dc in DATE_COLS:
        s = pd.to_datetime(df2[dc].apply(_fmt_date), format="%d/%m/%Y", errors="coerce")
        df2[dc] = s.dt.date
        df2.loc[s.isna(), dc] = None

    # 4) STATUS calculado
    df2["STATUS"] = df2.apply(
        lambda r: _calc_status_like_excel(r.get("DATA_ASSINATURA"), r.get("INICIO_RENOV"), r.get("VIGENCIA")),
        axis=1
    )

    # 5) SEGMENTO canônico
    df2["SEGMENTO"] = df2["SEGMENTO"].apply(lambda v: segments_to_str(normalize_segments(v)))

    # 6) limpar apenas NÃO-data
    for c in [c for c in EXPECTED_COLS if c not in DATE_COLS]:
        df2[c] = df2[c].apply(lambda v: "-" if (v is None or str(v).strip() in {"", "nan", "NaN", "NaT"}) else str(v).strip())

    # 7) ID e timestamps (se não vierem do Excel)
    now_ts = pd.Timestamp.utcnow()
    if "ID" not in df2.columns:
        df2["ID"] = [uuid4().hex for _ in range(len(df2))]
    df2["CREATED_AT"] = now_ts
    df2["UPDATED_AT"] = now_ts

    # 8) ordena colunas como na tabela
    cols_order = ["ID", *EXPECTED_COLS, "CREATED_AT", "UPDATED_AT"]
    df2 = df2.reindex(columns=cols_order)

    # 9) APPEND (nada de TRUNCATE, nada de CSV_PARSER_FEATURES)
    res = sf_session.write_pandas(
        df2,
        table_name=FQN_MAIN.split('.')[-1],
        database=FQN_MAIN.split('.')[0],
        schema=FQN_MAIN.split('.')[1],
        overwrite=False,           # nunca sobrescreve
        auto_create_table=False,   # tabela já existe
        quote_identifiers=True
    )
    return len(df2)


def _fetch_df(segmento: str | None = None) -> pd.DataFrame:
    pdf = _sf(f'SELECT * FROM {FQN_MAIN}').to_pandas()
    if pdf.empty:
        return pdf
    if "SEGMENTO" not in pdf.columns:
        pdf["SEGMENTO"] = "-"

    pdf["_segments"] = pdf["SEGMENTO"].apply(normalize_segments)
    if segmento and segmento != "Todos":
        pdf = pdf[pdf["_segments"].apply(lambda lst: segmento in lst)]
    if "NOME_EMPRESA" in pdf.columns:
        pdf = pdf.sort_values("NOME_EMPRESA", kind="stable")
    return pdf.drop(columns=["_segments"], errors="ignore")

def _update_record(rec_id: str, updates: dict):
    """
    updates: dicionário com chaves dos nomes limpos em MAIÚSCULO.
    """
    if not rec_id:
        raise ValueError("ID obrigatório.")
    if not updates:
        return

    # filtra apenas colunas válidas
    updates = {k: v for k, v in updates.items() if k in EXPECTED_COLS or k in {"CREATED_AT","UPDATED_AT"}}
    updates["UPDATED_AT"] = pd.Timestamp.utcnow()

    set_parts = []
    for k, v in updates.items():
        if k in DATE_COLS:
            if v in (None, "-", "", "nan", "NaN", "NaT"):
                set_parts.append(f'{k} = NULL')
            else:
                try:
                    d = pd.to_datetime(str(v), dayfirst=True, errors="coerce")
                    set_parts.append(f"{k} = TO_DATE('{d.strftime('%Y-%m-%d')}')" if pd.notna(d) else f"{k} = NULL")
                except Exception:
                    set_parts.append(f'{k} = NULL')
        elif k in {"CREATED_AT", "UPDATED_AT"}:
            set_parts.append(f'{k} = CURRENT_TIMESTAMP()')
        else:
            set_parts.append(f"{k} = '{_sf_escape(v)}'")

    set_clause = ", ".join(set_parts)
    _sf(f"""UPDATE {FQN_MAIN}
            SET {set_clause}
            WHERE ID = '{_sf_escape(rec_id)}'""").collect()

def _insert_comment(empresa_id: str, username: str, name: str, message: str):
    if not str(message).strip():
        return
    _sf(f"""
        INSERT INTO {FQN_COMMENTS}
        ("ID","EMPRESA_ID","USERNAME","NAME","MESSAGE","CREATED_AT")
        VALUES (
          '{uuid4().hex}',
          '{_sf_escape(empresa_id)}',
          '{_sf_escape(username)}',
          '{_sf_escape(name)}',
          '{_sf_escape(message.strip())}',
          CURRENT_TIMESTAMP()
        )
    """).collect()

def _fetch_comments(empresa_id: str) -> pd.DataFrame:
    return _sf(f"""
        SELECT * FROM {FQN_COMMENTS}
        WHERE "EMPRESA_ID" = '{_sf_escape(empresa_id)}'
        ORDER BY "CREATED_AT" DESC
    """).to_pandas()

def _insert_record_main(record: dict) -> str:
    """
    record deve usar as CHAVES LIMPA (MAIÚSCULO) conforme EXPECTED_COLS.
    """
    row = {c: _s(record.get(c)) for c in EXPECTED_COLS}

    # Datas a partir de texto UI
    for dc in DATE_COLS:
        row[dc] = _fmt_date(row.get(dc))

    # STATUS
    row["STATUS"] = _calc_status_like_excel(row.get("DATA_ASSINATURA"), row.get("INICIO_RENOV"), row.get("VIGENCIA"))

    # SEGMENTO canônico
    row["SEGMENTO"] = segments_to_str(normalize_segments(row.get("SEGMENTO", "-")))

    # NÃO-data -> "-"
    for c in EXPECTED_COLS:
        if c not in DATE_COLS:
            v = row.get(c)
            row[c] = "-" if (v is None or str(v).strip() in {"", "nan", "NaN", "NaT"}) else str(v).strip()

    rec_id = uuid4().hex

    def _date_sql(v: str) -> str:
        if v in ("-", "", None):
            return "NULL"
        d = pd.to_datetime(v, dayfirst=True, errors="coerce")
        return f"TO_DATE('{d.strftime('%Y-%m-%d')}')" if pd.notna(d) else "NULL"

    cols_order = ["ID", *EXPECTED_COLS, "CREATED_AT", "UPDATED_AT"]
    col_list = ", ".join(cols_order)

    vals = []
    for c in cols_order:
        if c == "ID":
            vals.append(f"'{rec_id}'")
        elif c in {"CREATED_AT", "UPDATED_AT"}:
            vals.append("CURRENT_TIMESTAMP()")
        elif c in DATE_COLS:
            vals.append(_date_sql(row[c]))
        else:
            vals.append(f"'{_sf_escape(row.get(c, '-'))}'")

    values_sql = ", ".join(vals)
    _sf(f'INSERT INTO {FQN_MAIN} ({col_list}) VALUES ({values_sql})').collect()
    return rec_id

# =========================
# MODAL DE DETALHES (com abas + edição por role)
# =========================
def open_company_dialog(rec: dict, is_admin: bool, current_user: dict):
    titulo = f"Detalhes — {_s(rec.get('NOME_EMPRESA'))}"

    @st.dialog(titulo, width="large")
    def _dialog():
        st.caption(
            f"Segmento: **{_s(rec.get('SEGMENTO'))}** • "
            f"CNPJ: **{_s(rec.get('CNPJ'))}** • "
            f"Prioridade: **{_s(rec.get('PRIORIDADE'))}**"
        )
        st.divider()

        if is_admin:
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
                            value=int(rec.get("PRIORIDADE")) if _s(rec.get("PRIORIDADE")).isdigit() else 3,
                            format_func=lambda x: {0: "0 (mais alta)", 1: "1", 2: "2", 3: "3 (menos)"}.get(x, str(x)),
                        )
                        situacao = st.text_input(LABEL["SITUACAO"], value=_s(rec.get("SITUACAO")))
                        status = st.text_input(LABEL["STATUS"]+" (recalculado ao salvar se datas mudarem)", value=_s(rec.get("STATUS")))
                        status_atual = st.text_area(LABEL["STATUS_ATUAL"], value=_s(rec.get("STATUS_ATUAL")), height=80)
                        nda_ass = st.text_input(LABEL["NDA_ASSINADO"], value=_s(rec.get("NDA_ASSINADO")))
                        aprov = st.text_input(LABEL["APROVACAO"], value=_s(rec.get("APROVACAO")))
                    with col2:
                        nome = st.text_input(LABEL["NOME_EMPRESA"], value=_s(rec.get("NOME_EMPRESA")))
                        cnpj = st.text_input(LABEL["CNPJ"], value=_s(rec.get("CNPJ")))
                        seg_pre = normalize_segments(rec.get("SEGMENTO"))
                        segmentos_ms = st.multiselect(LABEL["SEGMENTO"], options=SEGMENT_OPTIONS, default=seg_pre)
                        relac = st.text_input(LABEL["RELACIONAMENTO"], value=_s(rec.get("RELACIONAMENTO")))
                        auto = st.text_input(LABEL["AUTOMACAO"], value=_s(rec.get("AUTOMACAO")))
                        doc = st.text_input(LABEL["DOCUMENTO"], value=_s(rec.get("DOCUMENTO")))

                with tab_datas:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        data_ass = st.text_input(f"{LABEL['DATA_ASSINATURA']} (DD/MM/AAAA)", value=_s(rec.get("DATA_ASSINATURA")))
                    with col2:
                        inicio_renov = st.text_input(f"{LABEL['INICIO_RENOV']} (DD/MM/AAAA)", value=_s(rec.get("INICIO_RENOV")))
                    with col3:
                        vigencia = st.text_input(f"{LABEL['VIGENCIA']} (DD/MM/AAAA)", value=_s(rec.get("VIGENCIA")))
                    col4, col5, col6 = st.columns(3)
                    with col4:
                        val_anos = st.text_input(LABEL["VAL_ANOS"], value=_s(rec.get("VAL_ANOS")))
                    with col5:
                        val_meses = st.text_input(LABEL["VAL_MESES"], value=_s(rec.get("VAL_MESES")))
                    with col6:
                        val_dias = st.text_input(LABEL["VAL_DIAS"], value=_s(rec.get("VAL_DIAS")))

                with tab_prod:
                    col1, col2 = st.columns(2)
                    with col1:
                        metodologia = st.text_area(LABEL["METODOLOGIA"], value=_s(rec.get("METODOLOGIA")), height=120)
                        cobertura = st.text_area(LABEL["COBERTURA"], value=_s(rec.get("COBERTURA")), height=120)
                        resumo = st.text_area(LABEL["RESUMO"], value=_s(rec.get("RESUMO")), height=120)
                    with col2:
                        descricao = st.text_area(LABEL["DESCRICAO"], value=_s(rec.get("DESCRICAO")), height=180)

                with tab_contatos:
                    site = st.text_input(LABEL["SITE"], value=_s(rec.get("SITE")))
                    contatos = st.text_area(LABEL["CONTATOS"], value=_s(rec.get("CONTATOS")), height=120)
                    analise_tec = st.text_input(LABEL["ANALISE_TECNICA"], value=_s(rec.get("ANALISE_TECNICA")))

                with tab_obs:
                    obs = st.text_area(LABEL["OBS"], value=_s(rec.get("OBS")), height=120)
                    pts_fortes = st.text_area(LABEL["PONTOS_FORTES"], value=_s(rec.get("PONTOS_FORTES")), height=100)
                    pts_fracos = st.text_area(LABEL["PONTOS_FRACOS"], value=_s(rec.get("PONTOS_FRACOS")), height=100)
                    conc = st.text_area(LABEL["CONCORRENTES"], value=_s(rec.get("CONCORRENTES")), height=100)

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

                save_btn = st.form_submit_button("💾 Salvar alterações", use_container_width=True)
                if save_btn:
                    data_ass_n = _fmt_date(data_ass)
                    inicio_renov_n = _fmt_date(inicio_renov)
                    vigencia_n = _fmt_date(vigencia)
                    status_calc = _calc_status_like_excel(data_ass_n, inicio_renov_n, vigencia_n)

                    updates = {
                        "PRIORIDADE": str(prioridade),
                        "SITUACAO": _s(situacao),
                        "CNPJ": _s(cnpj),
                        "NOME_EMPRESA": _s(nome),
                        "SEGMENTO": segments_to_str(segmentos_ms),
                        "DESCRICAO": _s(descricao),
                        "RESUMO": _s(resumo),
                        "METODOLOGIA": _s(metodologia),
                        "COBERTURA": _s(cobertura),
                        "SITE": _s(site),
                        "CONTATOS": _s(contatos),
                        "DATA_ASSINATURA": data_ass_n,
                        "VAL_ANOS": _s(val_anos),
                        "VAL_MESES": _s(val_meses),
                        "VAL_DIAS": _s(val_dias),
                        "INICIO_RENOV": inicio_renov_n,
                        "VIGENCIA": vigencia_n,
                        "STATUS": status_calc,
                        "NDA_ASSINADO": _s(nda_ass),
                        "DOCUMENTO": _s(doc),
                        "APROVACAO": _s(aprov),
                        "ANALISE_TECNICA": _s(analise_tec),
                        "RELACIONAMENTO": _s(relac),
                        "AUTOMACAO": _s(auto),
                        "OBS": _s(obs),
                        "PONTOS_FORTES": _s(pts_fortes),
                        "PONTOS_FRACOS": _s(pts_fracos),
                        "CONCORRENTES": _s(conc),
                        "STATUS_ATUAL": _s(status_atual),
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

            # Comentários fora do form (Enter envia)
            st.markdown("---")
            st.markdown("**Adicionar comentário (pressione Enter para enviar):**")

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
                    st.session_state[key] = ""
                    st.rerun()

            st.text_input(
                "Comentário",
                value="",
                key=f"novo_coment_{rec['ID']}",
                placeholder="Escreva seu comentário e pressione Enter",
                on_change=_submit_comment,
            )

        else:
            tab_geral, tab_datas, tab_prod, tab_contatos, tab_obs, tab_status = st.tabs(
                ["📌 Geral", "📅 Datas", "🧪 Produto/Cobertura",
                 "🔗 Contatos & Docs", "🧭 Observações & Mercado",
                 "🗒️ Status Atual & Comentários"]
            )
            with tab_geral:
                st.markdown(f"- **{LABEL['PRIORIDADE']}:** {_s(rec.get('PRIORIDADE'))}")
                st.markdown(f"- **{LABEL['SITUACAO']}:** {_s(rec.get('SITUACAO'))}")
                st.markdown(f"- **{LABEL['STATUS']}:** {_s(rec.get('STATUS'))}")
                st.markdown(f"- **{LABEL['STATUS_ATUAL']}:** {_s(rec.get('STATUS_ATUAL'))}")
                st.markdown(f"- **{LABEL['VIGENCIA']}:** {_s(rec.get('VIGENCIA'))}")
                st.markdown(f"- **{LABEL['NDA_ASSINADO']}:** {_fmt_bool(rec.get('NDA_ASSINADO'))}")
                st.markdown(f"- **{LABEL['APROVACAO']}:** {_s(rec.get('APROVACAO'))}")
                st.markdown(f"- **{LABEL['ANALISE_TECNICA']}:** {_s(rec.get('ANALISE_TECNICA'))}")
                st.markdown(f"- **{LABEL['RELACIONAMENTO']}:** {_s(rec.get('RELACIONAMENTO'))}")
                st.markdown(f"- **{LABEL['AUTOMACAO']}:** {_s(rec.get('AUTOMACAO'))}")
            with tab_datas:
                st.markdown(f"- **{LABEL['DATA_ASSINATURA']}:** {_fmt_date(rec.get('DATA_ASSINATURA'))}")
                st.markdown(f"- **{LABEL['INICIO_RENOV']}:** {_fmt_date(rec.get('INICIO_RENOV'))}")
                st.markdown(f"- **Validade (Anos/Meses/Dias):** {_s(rec.get('VAL_ANOS'))} / {_s(rec.get('VAL_MESES'))} / {_s(rec.get('VAL_DIAS'))}")
            with tab_prod:
                st.markdown(f"- **{LABEL['METODOLOGIA']}:** {_s(rec.get('METODOLOGIA'))}")
                st.markdown(f"- **{LABEL['COBERTURA']}:** {_s(rec.get('COBERTURA'))}")
                st.markdown(f"- **{LABEL['DESCRICAO']}:** {_s(rec.get('DESCRICAO'))}")
                st.markdown(f"- **{LABEL['RESUMO']}:** {_s(rec.get('RESUMO'))}")
            with tab_contatos:
                site = _s(rec.get('SITE'))
                if site not in {"-", ""}:
                    st.markdown(f"- **{LABEL['SITE']}:** [{site}]({site})")
                else:
                    st.markdown(f"- **{LABEL['SITE']}:** -")
                st.markdown(f"- **{LABEL['CONTATOS']}:** {_s(rec.get('CONTATOS'))}")
                st.markdown(f"- **{LABEL['DOCUMENTO']}:** {_s(rec.get('DOCUMENTO'))}")
            with tab_obs:
                st.markdown(f"- **{LABEL['OBS']}:** {_s(rec.get('OBS'))}")
                st.markdown(f"- **{LABEL['PONTOS_FORTES']}:** {_s(rec.get('PONTOS_FORTES'))}")
                st.markdown(f"- **{LABEL['PONTOS_FRACOS']}:** {_s(rec.get('PONTOS_FRACOS'))}")
                st.markdown(f"- **{LABEL['CONCORRENTES']}:** {_s(rec.get('CONCORRENTES'))}")
            with tab_status:
                st.markdown(f"**{LABEL['STATUS_ATUAL']}:** {_s(rec.get('STATUS_ATUAL'))}")
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

        # usar key dinâmica para resetar o componente após importação
        uploader_key = f"uploader_xlsx_sidebar_{st.session_state.upload_key}"
        uploaded = st.file_uploader("Selecione um .xlsx", type=["xlsx"], key=uploader_key)

        if uploaded is not None:
            # hash do conteúdo p/ idempotência
            digest = hashlib.sha256(uploaded.getvalue()).hexdigest()

            if digest in st.session_state.processed_hashes:
                st.info("Este arquivo já foi importado nesta sessão. Selecione outro arquivo.")
            else:
                try:
                    xls = pd.ExcelFile(uploaded, engine="openpyxl")
                    sheet_names = xls.sheet_names
                    chosen_sheet = "Dados" if "Dados" in sheet_names else sheet_names[0]

                    df_view = pd.read_excel(xls, sheet_name=chosen_sheet, dtype=str)
                    df_view.columns = df_view.columns.map(lambda c: str(c).strip())

                    # Mapeia colunas antigas -> limpas
                    rename_map = {c: ORIGINAL_TO_CANON.get(c, c) for c in df_view.columns}
                    df_view.rename(columns=rename_map, inplace=True)

                    # Garante colunas esperadas
                    for c in EXPECTED_COLS:
                        if c not in df_view.columns:
                            df_view[c] = "-"

                    # Datas -> date/None
                    for dc in DATE_COLS:
                        if dc in df_view.columns:
                            s = pd.to_datetime(df_view[dc].apply(_fmt_date), format="%d/%m/%Y", errors="coerce")
                            df_view[dc] = s.dt.date
                            df_view.loc[s.isna(), dc] = None

                    # Status
                    df_view["STATUS"] = df_view.apply(
                        lambda r: _calc_status_like_excel(
                            r.get("DATA_ASSINATURA"), r.get("INICIO_RENOV"), r.get("VIGENCIA")
                        ), axis=1
                    )

                    # Limpeza apenas nas NÃO-data
                    non_date_cols = [c for c in EXPECTED_COLS if c not in DATE_COLS]
                    for c in non_date_cols:
                        df_view[c] = df_view[c].apply(
                            lambda v: "-" if (v is None or str(v).strip() in {"", "nan", "NaN", "NaT"}) else str(v).strip()
                        )

                    # Segmento canônico
                    df_view["SEGMENTO"] = df_view["SEGMENTO"].apply(lambda v: segments_to_str(normalize_segments(v)))

                    # APPEND sempre
                    n = import_to_sf_append(df_view)

                    # marca como processado e reseta o uploader
                    st.session_state.processed_hashes.add(digest)
                    st.session_state.upload_key += 1   # força recriar o componente (limpa o arquivo)
                    st.session_state.upload_info = {"file_name": uploaded.name, "sheet": chosen_sheet, "rows": n}

                    st.success(f"Importação concluída: {n} linha(s) na aba '{chosen_sheet}'.")
                    st.rerun()

                except Exception as e:
                    st.error(f"Não foi possível ler o XLSX. Detalhes: {e}")

# Somente login
if not st.session_state.auth["is_auth"]:
    render_public_home()
    st.stop()

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
    st.stop()

# Modo LISTA (mostra resultados do filtro + botão Voltar)
st.subheader(f"Resultados — Segmento: {st.session_state.filter_segmento}")
c_voltar, c_novo = st.columns(2)
with c_voltar:
    if st.button("⬅️ Voltar aos filtros", use_container_width=True, key="btn-voltar-segmentos"):
        st.session_state.segment_view = "select"
        st.session_state.filter_segmento = "Todos"
        st.rerun()

def open_create_dialog(default_segmento: str | None, current_user: dict):
    @st.dialog("✚ Nova empresa", width="large")
    def _dialog():
        st.caption("Preencha os campos e clique em **Salvar**.")
        with st.form("form_nova_empresa"):
            tab_geral, tab_datas, tab_prod, tab_contatos, tab_obs = st.tabs(
                ["📌 Geral", "📅 Datas", "🧪 Produto/Cobertura", "🔗 Contatos & Docs", "🧭 Observações & Mercado"]
            )

            with tab_geral:
                col1, col2 = st.columns(2)
                with col1:
                    nome = st.text_input(LABEL["NOME_EMPRESA"], value="")
                    cnpj = st.text_input(LABEL["CNPJ"], value="")
                    segmento_ms_default = [default_segmento] if default_segmento in SEGMENT_OPTIONS else []
                    segmentos_ms = st.multiselect(LABEL["SEGMENTO"], options=SEGMENT_OPTIONS, default=segmento_ms_default)
                    prioridade = st.select_slider("Prioridade (0 = sem prioridade, 3 = alta)", options=[0, 1, 2, 3], value=0)
                    situacao = st.text_input(LABEL["SITUACAO"], value="-")
                    status_atual = st.text_area(LABEL["STATUS_ATUAL"], value="-", height=80)
                with col2:
                    nda_ass = st.text_input(LABEL["NDA_ASSINADO"], value="-")
                    aprov = st.text_input(LABEL["APROVACAO"], value="-")
                    relac = st.text_input(LABEL["RELACIONAMENTO"], value="-")
                    auto = st.text_input(LABEL["AUTOMACAO"], value="-")
                    doc = st.text_input(LABEL["DOCUMENTO"], value="-")

            with tab_datas:
                col1, col2, col3 = st.columns(3)
                with col1:
                    data_ass = st.text_input(f"{LABEL['DATA_ASSINATURA']} (DD/MM/AAAA)", value="-")
                with col2:
                    inicio_renov = st.text_input(f"{LABEL['INICIO_RENOV']} (DD/MM/AAAA)", value="-")
                with col3:
                    vigencia = st.text_input(f"{LABEL['VIGENCIA']} (DD/MM/AAAA)", value="-")
                col4, col5, col6 = st.columns(3)
                with col4:
                    val_anos = st.text_input(LABEL["VAL_ANOS"], value="-")
                with col5:
                    val_meses = st.text_input(LABEL["VAL_MESES"], value="-")
                with col6:
                    val_dias = st.text_input(LABEL["VAL_DIAS"], value="-")

            with tab_prod:
                col1, col2 = st.columns(2)
                with col1:
                    metodologia = st.text_area(LABEL["METODOLOGIA"], value="-", height=100)
                    cobertura = st.text_area(LABEL["COBERTURA"], value="-", height=100)
                    resumo = st.text_area(LABEL["RESUMO"], value="-", height=100)
                with col2:
                    descricao = st.text_area(LABEL["DESCRICAO"], value="-", height=160)

            with tab_contatos:
                site = st.text_input(LABEL["SITE"], value="-")
                contatos = st.text_area(LABEL["CONTATOS"], value="-", height=80)
                analise_tec = st.text_input(LABEL["ANALISE_TECNICA"], value="-")

            with tab_obs:
                obs = st.text_area(LABEL["OBS"], value="-", height=100)
                pts_fortes = st.text_area(LABEL["PONTOS_FORTES"], value="-", height=80)
                pts_fracos = st.text_area(LABEL["PONTOS_FRACOS"], value="-", height=80)
                conc = st.text_area(LABEL["CONCORRENTES"], value="-", height=80)

            save = st.form_submit_button("💾 Salvar empresa", type="primary", use_container_width=True)
            if not segmentos_ms:
                st.error("Selecione pelo menos **um Segmento**.")
                return
            if save:
                if not nome.strip():
                    st.error("O campo **Nome da Empresa** é obrigatório.")
                    return

                record = {
                    "PRIORIDADE": str(prioridade),
                    "SITUACAO": _s(situacao),
                    "CNPJ": _s(cnpj),
                    "NOME_EMPRESA": _s(nome),
                    "SEGMENTO": segments_to_str(segmentos_ms),
                    "DESCRICAO": _s(descricao),
                    "RESUMO": _s(resumo),
                    "METODOLOGIA": _s(metodologia),
                    "COBERTURA": _s(cobertura),
                    "SITE": _s(site),
                    "CONTATOS": _s(contatos),
                    "DATA_ASSINATURA": _s(data_ass),
                    "VAL_ANOS": _s(val_anos),
                    "VAL_MESES": _s(val_meses),
                    "VAL_DIAS": _s(val_dias),
                    "INICIO_RENOV": _s(inicio_renov),
                    "VIGENCIA": _s(vigencia),
                    "STATUS": "-",
                    "NDA_ASSINADO": _s(nda_ass),
                    "DOCUMENTO": _s(doc),
                    "APROVACAO": _s(aprov),
                    "ANALISE_TECNICA": _s(analise_tec),
                    "RELACIONAMENTO": _s(relac),
                    "AUTOMACAO": _s(auto),
                    "OBS": _s(obs),
                    "PONTOS_FORTES": _s(pts_fortes),
                    "PONTOS_FRACOS": _s(pts_fracos),
                    "CONCORRENTES": _s(conc),
                    "STATUS_ATUAL": _s(status_atual),
                }

                try:
                    _insert_record_main(record)
                    st.success("Empresa criada com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao criar empresa: {e}")
    _dialog()

with c_novo:
    if is_admin and st.button("✚ Criar empresa", use_container_width=True, key="btn-criar-empresa"):
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
                nome = _s(row.get("NOME_EMPRESA"))
                seg  = segments_to_str(normalize_segments(row.get("SEGMENTO")))
                stat = _s(row.get("STATUS"))
                vig  = _s(row.get("VIGENCIA"))
                prio = _s(row.get("PRIORIDADE"))
                st.markdown(f"### {nome}")
                st.caption(f"Segmento: **{seg}** • Status: **{stat}**")
                st.caption(f"Vigência: **{vig}** • Prioridade: **{prio}**")
                if st.button("Ver detalhes", key=f"btn-det-{row['ID']}", use_container_width=True):
                    open_company_dialog(row.to_dict(), is_admin=is_admin, current_user=user)
