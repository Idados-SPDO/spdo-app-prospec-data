# app.py
import streamlit as st
import pandas as pd
import duckdb
import re
from uuid import uuid4
from datetime import date, datetime

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Atuação de Prospecção de Dados", layout="wide")
st.logo("logo_ibre.png")

# =========================
# MOCK DE USUÁRIOS
# =========================
USERS = {
    "yago": {"password": "123", "name": "Yago Moraes", "role": "admin"},
    "ana": {"password": "123", "name": "Ana Silva", "role": "analista"},
    "joao": {"password": "123", "name": "João Souza", "role": "gestor"},
}

# =========================
# DUCKDB
# =========================
DB_PATH = "parcerias.db"
TABLE_NAME = "FORNECEDOR"

# Mapeamento: coluna do Excel -> coluna da tabela FORNECEDOR
EXCEL_TO_DB = {
    "Fornecedor": "FORNECEDOR",
    "Classificação do Fornecedor": "CLASSIFICACAO",
    "Categoria": "CATEGORIA",
    "Âmbito": "AMBITO",
    "Site": "SITE",
    "Descrição do Fornecedor": "DESC_FORNECEDOR",
    "O que o fornecedor oferece?": "OFERTA_FORNECEDOR",
    "Periodicidade": "PERIODICIDADE",
    "Como o fornecedor obtém os dados?": "DADOS_FORNECEDOR",
    "E-mails contato": "CONTATO",
    "Status": "STATUS",
    "Material de apresentação da empresa": "MATERIAL_APRESENTACAO",
}

STATUSES = [
    "Todos",
    "Contato não iniciado",
    "Contato iniciado e em andamento",
    "Contato iniciado mas sem ir a frente",
    "Assinatura NDA - Fornecedor",
    "Assinatura NDA - FGV IBRE",
    "Parceria concluída com sucesso.",
]

# Colunas extras (não vêm do Excel)
EXTRA_COLS = ["ID", "NDA_ASSINADO_EM"]
DB_COL_ORDER = ["ID", *EXCEL_TO_DB.values(), "NDA_ASSINADO_EM"]
DB_TO_LABEL = {
    **{v: k for k, v in EXCEL_TO_DB.items()},
    "ID": "ID",
    "NDA_ASSINADO_EM": "NDA assinado em",
}

# Classificação (mostramos "A — ..." no UI e salvamos só o código "A"/"B"/"C")
CLASSIFICACAO_OPTIONS = {
    "A": "Fornecedor de dados (compra ou parceria)",
    "B": "Não fornece os dados, mas pode oferecer alguma solução",
    "C": "Tem potencial para parceria de novos negócios",
}
CLASSIFICACAO_CHOICES = [f"{k} — {v}" for k, v in CLASSIFICACAO_OPTIONS.items()]

# Âmbito
AMBITO_OPTIONS = ["Nacional", "Global"]


# Setores para filtro
SETORES = [
    "Todos",
    "Alimentos/Bebidas",
    "Atacado/Varejo",
    "Construção/Infraestrutura",
    "Cosméticos",
    "Energia",
    "Saúde",
    "Mercado Financeiro",
    "Mão de Obra",
    "Mercado Imobiliário",
    "Outras Industrias",
]

# =========================
# Helpers de DB
# =========================
def _ensure_table():
    con = duckdb.connect(DB_PATH)
    # cria se não existir (com as colunas) 
    col_defs = ", ".join(f"{c} TEXT" for c in DB_COL_ORDER)
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({col_defs});")
    # garante colunas novas
    cols_df = con.execute(f"PRAGMA table_info('{TABLE_NAME}')").df()
    existing = set(cols_df["name"].tolist())
    for col in DB_COL_ORDER:
        if col not in existing:
            con.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} TEXT;")
    con.close()

def _persist_full_df(df: pd.DataFrame):
    """Regrava a tabela inteira com df nas colunas DB_COL_ORDER."""
    con = duckdb.connect(DB_PATH)
    con.execute(f"DELETE FROM {TABLE_NAME};")
    con.register("df_fix", df[DB_COL_ORDER].astype(str))
    cols = ", ".join(DB_COL_ORDER)
    con.execute(f"INSERT INTO {TABLE_NAME} ({cols}) SELECT {cols} FROM df_fix;")
    con.close()

def import_to_duckdb(df_db: pd.DataFrame) -> int:
    """Cria a tabela, injeta ID e NDA vazio e insere df_db."""
    _ensure_table()
    # Garante colunas extras
    df_db = df_db.copy()
    if "ID" not in df_db.columns:
        df_db["ID"] = [str(uuid4()) for _ in range(len(df_db))]
    if "NDA_ASSINADO_EM" not in df_db.columns:
        df_db["NDA_ASSINADO_EM"] = None
    df_db = df_db.reindex(columns=DB_COL_ORDER)

    con = duckdb.connect(DB_PATH)
    con.execute(f"DELETE FROM {TABLE_NAME};")
    con.register("df_up", df_db.astype(str))
    cols = ", ".join(DB_COL_ORDER)
    con.execute(f"INSERT INTO {TABLE_NAME} ({cols}) SELECT {cols} FROM df_up;")
    inserted = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()
    return inserted

def insert_fornecedor(rec: dict) -> None:
    """Insere um único fornecedor (gera ID se não vier)."""
    _ensure_table()
    payload = {c: rec.get(c, None) for c in DB_COL_ORDER}
    if not payload.get("ID"):
        payload["ID"] = str(uuid4())
    df = pd.DataFrame([payload]).reindex(columns=DB_COL_ORDER)
    con = duckdb.connect(DB_PATH)
    con.register("df_new", df.astype(str))
    cols = ", ".join(DB_COL_ORDER)
    con.execute(f"INSERT INTO {TABLE_NAME} ({cols}) SELECT {cols} FROM df_new;")
    con.close()

def update_fornecedor(row_id: str, updates: dict) -> None:
    """Atualiza colunas pelo ID."""
    if not row_id:
        raise ValueError("ID obrigatório para atualizar registro.")
    # filtra apenas colunas válidas e diferentes de ID
    valid_updates = {k: v for k, v in updates.items() if k in DB_COL_ORDER and k != "ID"}
    if not valid_updates:
        return
    set_clause = ", ".join([f"{k} = ?" for k in valid_updates.keys()])
    params = list(valid_updates.values()) + [row_id]
    con = duckdb.connect(DB_PATH)
    con.execute(f"UPDATE {TABLE_NAME} SET {set_clause} WHERE ID = ?;", params)
    con.close()

def load_fornecedores_df() -> pd.DataFrame:
    """Carrega todos os fornecedores; cria/ajusta colunas e ID se necessário."""
    _ensure_table()
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute(f"SELECT * FROM {TABLE_NAME}").df()
    except Exception:
        df = pd.DataFrame(columns=DB_COL_ORDER)
    con.close()

    # Garante todas as colunas no DF
    for c in DB_COL_ORDER:
        if c not in df.columns:
            df[c] = None
    df = df.reindex(columns=DB_COL_ORDER)

    # Se não há ID preenchido, gera e persiste
    if len(df) and (df["ID"].isna() | (df["ID"].astype(str).str.strip() == "")).any():
        df.loc[df["ID"].isna() | (df["ID"].astype(str).str.strip() == ""), "ID"] = [
            str(uuid4()) for _ in range((df["ID"].isna() | (df["ID"].astype(str).str.strip() == "")).sum())
        ]
        _persist_full_df(df)

    return df

# =========================
# Helpers de UI
# =========================
def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-_.]+", "", s)
    return s or "all"

def _classif_to_code(val: str | None) -> str | None:
    """Normaliza o valor para 'A'/'B'/'C' (se reconhecido)."""
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    # se já vier "A", "B", "C" (ou começando por isso)
    first = s[0].upper()
    if first in {"A", "B", "C"}:
        return first
    # tenta mapear por palavras-chave
    s_low = s.lower()
    if "fornecedor de dados" in s_low:
        return "A"
    if "não fornece" in s_low or "nao fornece" in s_low or "solução" in s_low or "solucao" in s_low:
        return "B"
    if "potencial" in s_low:
        return "C"
    return None  # deixa sem normalizar se não reconheceu

def _classif_choice_index_from_rec(val: str | None) -> int:
    """Descobre o índice do select a partir do valor salvo/antigo."""
    code = _classif_to_code(val) or "A"
    codes = list(CLASSIFICACAO_OPTIONS.keys())
    return codes.index(code)

def _classif_code_from_choice(choice: str) -> str:
    """Converte 'A — ...' para 'A'."""
    return choice.split("—", 1)[0].strip()

def _ambito_norm(val: str | None) -> str | None:
    """Normaliza para 'Nacional' ou 'Global' quando possível."""
    if not val:
        return None
    s = str(val).strip().lower()
    if s.startswith("nac"):
        return "Nacional"
    if s.startswith("glob") or s.startswith("intl") or s.startswith("internac"):
        return "Global"
    if s in {"nacional", "global"}:
        return s.capitalize()
    return None  # desconhecido: mantém None

def _normalize_url(u: str) -> str:
    if not u:
        return u
    u = u.strip()
    if not u:
        return u
    if not re.match(r"^https?://", u, flags=re.I):
        return "http://" + u
    return u

def _split_multi(s: str) -> list[str]:
    """Divide por ; , e quebras de linha (sem cortar frases comuns)."""
    if not s:
        return []
    parts = re.split(r"[;\n]+|,\s*(?=[^\s])", s)
    return [p.strip() for p in parts if p and p.strip() and p.strip().lower() != "none"]

def _norm(s):
    return "" if s is None else str(s).strip().casefold()

def _parse_date_str(s: str | None) -> date | None:
    if not s or str(s).strip() == "" or str(s).strip().lower() == "none":
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt).date()
        except Exception:
            pass
    return None  # formato desconhecido

# =========================
# DIALOGS
# =========================
def open_details_dialog(rec: dict):
    titulo = f"Detalhes — {rec.get('FORNECEDOR') or 'Fornecedor'}"

    @st.dialog(titulo, width="large")
    def _dialog():
        # CSS: aperta margens internas do dialog
        st.markdown("""
        <style>
        [data-testid="stDialog"] p{margin:0 !important;}
        [data-testid="stDialog"] ul{margin:0 !important;}
        [data-testid="stDialog"] li{margin:0 !important;}
        [data-testid="stDialog"] [data-testid="stMarkdown"]{margin:0 !important;}
        [data-testid="stDialog"] [data-testid="column"]{padding-top:0 !important;padding-bottom:0 !important;}
        [data-testid="stDialog"] .stButton{margin:0 !important;}
        </style>
        """, unsafe_allow_html=True)

        with st.form(f"form_editar_fornecedor_{rec.get('ID','novo')}", clear_on_submit=False):
            tab_geral, tab_contato, tab_textos = st.tabs(["📌 Geral", "📞 Contatos & NDA", "📝 Descrição & Oferta"])

            # =========================
            # 📌 GERAL
            # =========================
            with tab_geral:
                col1, col2 = st.columns(2)

                with col1:
                    nome = st.text_input(DB_TO_LABEL["FORNECEDOR"], value=rec.get("FORNECEDOR") or "")
                    classif = st.selectbox(
                        DB_TO_LABEL["CLASSIFICACAO"],
                        options=CLASSIFICACAO_CHOICES,
                        index=_classif_choice_index_from_rec(rec.get("CLASSIFICACAO"))
                    )
                    cat_options = [s for s in SETORES if s != "Todos"]
                    cat_value = rec.get("CATEGORIA") or cat_options[0]
                    cat_index = cat_options.index(cat_value) if cat_value in cat_options else 0
                    cat = st.selectbox(DB_TO_LABEL["CATEGORIA"], options=cat_options, index=cat_index)

                with col2:
                    amb_options = AMBITO_OPTIONS
                    amb_default = _ambito_norm(rec.get("AMBITO")) or AMBITO_OPTIONS[0]
                    amb_index = amb_options.index(amb_default) if amb_default in amb_options else 0
                    amb = st.selectbox(DB_TO_LABEL["AMBITO"], options=amb_options, index=amb_index)
                    site = st.text_area(DB_TO_LABEL["SITE"], value=rec.get("SITE") or "", placeholder="ex: site1.com; site2.com", height=90)

                    # Status como SELECTBOX com STATUSES (sem "Todos")
                    status_options = [s for s in STATUSES if s != "Todos"]
                    cur_status = (rec.get("STATUS") or "").strip()
                    status_idx = status_options.index(cur_status) if cur_status in status_options else 0
                    status = st.selectbox(
                        DB_TO_LABEL["STATUS"],
                        options=status_options,
                        index=status_idx,
                        key=f"det_status_{rec.get('ID','novo')}"
                    )

            # =========================
            # 📞 CONTATOS & NDA
            # =========================
            with tab_contato:
                colA, colB = st.columns(2)

                with colA:
                    cont  = st.text_area(DB_TO_LABEL["CONTATO"], value=rec.get("CONTATO") or "", placeholder="email1@dominio; email2@dominio", height=100)
                    peri  = st.text_input(DB_TO_LABEL["PERIODICIDADE"], value=rec.get("PERIODICIDADE") or "")
                    dados = st.text_area(DB_TO_LABEL["DADOS_FORNECEDOR"], value=rec.get("DADOS_FORNECEDOR") or "", height=100)

                with colB:
                    st.markdown("**NDA**")
                    colN1, colN2 = st.columns([1,2])
                    nda_assinado_bool = colN1.checkbox("Assinado?", value=bool(rec.get("NDA_ASSINADO_EM")))
                    nda_date_val = _parse_date_str(rec.get("NDA_ASSINADO_EM"))
                    nda_date = colN2.date_input(DB_TO_LABEL["NDA_ASSINADO_EM"], value=nda_date_val or date.today(), disabled=not nda_assinado_bool)
                    if not nda_assinado_bool:
                        nda_date = None

                    mat = st.text_area(DB_TO_LABEL["MATERIAL_APRESENTACAO"], value=rec.get("MATERIAL_APRESENTACAO") or "", placeholder="URL1; URL2", height=100)

            # =========================
            # 📝 DESCRIÇÃO & OFERTA
            # =========================
            with tab_textos:
                colX, colY = st.columns(2)
                with colX:
                    descf = st.text_area(DB_TO_LABEL["DESC_FORNECEDOR"], value=rec.get("DESC_FORNECEDOR") or "", height=180)
                with colY:
                    ofert = st.text_area(DB_TO_LABEL["OFERTA_FORNECEDOR"], value=rec.get("OFERTA_FORNECEDOR") or "", height=180)

            st.markdown("---")
            c1, c2, c3 = st.columns([1,1,1])
            salvar = c1.form_submit_button("Salvar alterações", type="primary", use_container_width=True)
            fechar = c2.form_submit_button("Fechar", use_container_width=True)
            abrir_links = c3.form_submit_button("Abrir apresentações", use_container_width=True)

            if abrir_links:
                # abre os links de apresentação (se houver)
                for j, item in enumerate(_split_multi(mat or "")):
                    st.link_button("Baixar apresentação", _normalize_url(item), key=f"lnk-{rec.get('ID','novo')}-{j}")

            if fechar:
                st.rerun()

            if salvar:
                if not nome.strip():
                    st.error("Campo 'Fornecedor' é obrigatório.")
                    return
                updates = {
                    "FORNECEDOR": nome.strip(),
                    "CLASSIFICACAO": _classif_code_from_choice(classif),
                    "CATEGORIA": cat,
                    "AMBITO": amb,
                    "SITE": site.strip() or None,
                    "DESC_FORNECEDOR": descf.strip() or None,
                    "OFERTA_FORNECEDOR": ofert.strip() or None,
                    "PERIODICIDADE": peri.strip() or None,
                    "DADOS_FORNECEDOR": dados.strip() or None,
                    "CONTATO": cont.strip() or None,
                    "STATUS": status.strip() or None,  # <- vem do selectbox
                    "MATERIAL_APRESENTACAO": (mat or "").strip() or None,
                    "NDA_ASSINADO_EM": nda_date.isoformat() if nda_date else None,
                }
                try:
                    update_fornecedor(rec.get("ID"), updates)
                    st.success("Fornecedor atualizado com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao atualizar: {e}")

    _dialog()


def open_create_dialog(default_categoria: str | None = None):
    @st.dialog("Novo fornecedor", width="large")
    def _dialog():
        st.caption("Preencha os campos abaixo e clique em **Salvar**.")
        colA, colB = st.columns(2)

        with colA:
            nome = st.text_input(DB_TO_LABEL["FORNECEDOR"], key="novo_FORNECEDOR")
            classif = st.selectbox(
                DB_TO_LABEL["CLASSIFICACAO"],
                options=CLASSIFICACAO_CHOICES,
                key="novo_CLASSIFICACAO"
            )
            cat = st.selectbox(
                DB_TO_LABEL["CATEGORIA"],
                [s for s in SETORES if s != "Todos"],
                index=( [s for s in SETORES if s != "Todos"].index(default_categoria) 
                        if default_categoria in SETORES and default_categoria != "Todos" else 0),
                key="novo_CATEGORIA",
            )
            amb = st.selectbox(
                DB_TO_LABEL["AMBITO"],
                options=AMBITO_OPTIONS,
                index=0,
                key="novo_AMBITO"
            )
            site = st.text_area(DB_TO_LABEL["SITE"], key="novo_SITE", placeholder="ex: site1.com; site2.com")
            status_options_new = [s for s in STATUSES if s != "Todos"]
            status = st.selectbox(DB_TO_LABEL["STATUS"], options=status_options_new, index=0, key="novo_STATUS_SEL")

        with colB:
            descf = st.text_area(DB_TO_LABEL["DESC_FORNECEDOR"], key="novo_DESC_FORNECEDOR")
            ofert = st.text_area(DB_TO_LABEL["OFERTA_FORNECEDOR"], key="novo_OFERTA_FORNECEDOR")
            peri  = st.text_input(DB_TO_LABEL["PERIODICIDADE"], key="novo_PERIODICIDADE")
            dados = st.text_area(DB_TO_LABEL["DADOS_FORNECEDOR"], key="novo_DADOS_FORNECEDOR")
            cont  = st.text_area(DB_TO_LABEL["CONTATO"], key="novo_CONTATO", placeholder="email1@dominio; email2@dominio")

            st.markdown("**NDA**")
            nda_assinado_bool = st.checkbox("NDA assinado?", key="novo_NDA_BOOL")
            if nda_assinado_bool:
                nda_date = st.date_input(DB_TO_LABEL["NDA_ASSINADO_EM"], key="novo_NDA_ASSINADO_EM", value=date.today())
            else:
                nda_date = None

            mat   = st.text_area(DB_TO_LABEL["MATERIAL_APRESENTACAO"], key="novo_MATERIAL_APRESENTACAO", placeholder="URL1; URL2")

        c1, c2 = st.columns([1,1])
        salvar = c1.button("Salvar", type="primary", use_container_width=True)
        cancelar = c2.button("Cancelar", use_container_width=True)

        if cancelar:
            st.rerun()

        if salvar:
            if not nome.strip():
                st.error("Campo 'Fornecedor' é obrigatório.")
                return
            rec = {
                "FORNECEDOR": nome.strip(),
                "CLASSIFICACAO": _classif_code_from_choice(classif),
                "CATEGORIA": cat,
                "AMBITO": amb,
                "SITE": site.strip() or None,
                "DESC_FORNECEDOR": descf.strip() or None,
                "OFERTA_FORNECEDOR": ofert.strip() or None,
                "PERIODICIDADE": peri.strip() or None,
                "DADOS_FORNECEDOR": dados.strip() or None,
                "CONTATO": cont.strip() or None,
                "STATUS": status.strip() or None,
                "MATERIAL_APRESENTACAO": mat.strip() or None,
                "NDA_ASSINADO_EM": nda_date.isoformat() if nda_date else None,
            }
            try:
                insert_fornecedor(rec)
                st.success("Fornecedor criado com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

    _dialog()

# =========================
# STATE & LOGIN
# =========================
def ensure_state():
    if "auth" not in st.session_state:
        st.session_state.auth = {"is_auth": False, "user": None}

    # estado da aba Setor
    if "view_setor" not in st.session_state:
        st.session_state.view_setor = "select"  # "select" | "list"
    if "filtro_setor_sel" not in st.session_state:
        st.session_state.filtro_setor_sel = "Todos"

    # estado da aba Status
    if "view_status" not in st.session_state:
        st.session_state.view_status = "select"  # "select" | "list"
    if "filtro_status_sel" not in st.session_state:
        st.session_state.filtro_status_sel = "Todos"

ensure_state()

with st.sidebar:
    st.subheader("🔐 Acesso")
    if not st.session_state.auth["is_auth"]:
        with st.form("login_form"):
            username = st.text_input("Usuário", placeholder="ex: yago")
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

    # -------- Importar Excel (na sidebar) --------
    if st.session_state.auth["is_auth"]:
        st.markdown("---")
        st.markdown("### 📄 Importar Excel")
        uploaded = st.file_uploader("Selecione um .xlsx (aba 'Dados')", type=["xlsx"], key="uploader_xlsx_sidebar")
        if uploaded:
            try:
                xls = pd.ExcelFile(uploaded, engine="openpyxl")
                if "Dados" not in xls.sheet_names:
                    st.error(f"Aba 'Dados' não encontrada. Abas: {', '.join(xls.sheet_names)}")
                else:
                    df_raw = pd.read_excel(xls, sheet_name="Dados", dtype=str)
                    df_raw.columns = df_raw.columns.map(lambda c: str(c).strip())

                    present = [c for c in df_raw.columns if c in EXCEL_TO_DB]
                    missing = [c for c in EXCEL_TO_DB if c not in df_raw.columns]

                    df_db = df_raw.rename(columns=EXCEL_TO_DB)[[EXCEL_TO_DB[c] for c in present]]
                    for miss in missing:
                        df_db[EXCEL_TO_DB[miss]] = None
                    # injeta colunas extras e reordena:
                    if "ID" not in df_db.columns:
                        df_db["ID"] = [str(uuid4()) for _ in range(len(df_db))]
                    df_db["NDA_ASSINADO_EM"] = None
                    df_db = df_db.reindex(columns=DB_COL_ORDER)

                    if "CLASSIFICACAO" in df_db.columns:
                        df_db["CLASSIFICACAO"] = df_db["CLASSIFICACAO"].apply(_classif_to_code)
                    if "AMBITO" in df_db.columns:
                        df_db["AMBITO"] = df_db["AMBITO"].apply(_ambito_norm)

                    with st.spinner("Importando para DuckDB..."):
                        n = import_to_duckdb(df_db)
                    st.success(f"Importação concluída! {n} registros gravados.")
                    if missing:
                        st.info("Colunas ausentes preenchidas como vazio: " + ", ".join(missing))
            except Exception as e:
                st.error(f"Não foi possível ler o XLSX. Detalhes: {e}")

# Somente login
if not st.session_state.auth["is_auth"]:
    st.stop()

# =========================
# CONTEÚDO PRINCIPAL
# =========================
# =========================
# CONTEÚDO PRINCIPAL
# =========================
df_all = load_fornecedores_df()

st.title("🏗️ Atuação de Prospecção de Dados")

# Função utilitária para renderizar os cards
def render_cards(df_view: pd.DataFrame, titulo_lista: str, key_prefix: str):
    st.markdown("---")
    if df_view.empty:
        if df_all.empty:
            st.info("Nenhum fornecedor encontrado. Importe um Excel pela barra lateral.")
        else:
            st.warning(f"Nenhum fornecedor para o filtro selecionado em: {titulo_lista}.")
        return

    st.caption(f"{len(df_view)} fornecedor(es) em “{titulo_lista}”.")
    cols = st.columns(3)
    for i in range(len(df_view)):
        with cols[i % 3]:
            with st.container(border=True):
                rec = df_view.iloc[i].to_dict()
                nome = rec.get("FORNECEDOR") or "—"
                status = rec.get("STATUS") or "—"
                categoria = rec.get("CATEGORIA") or "—"
                ambito = rec.get("AMBITO") or "—"
                nda_info = rec.get("NDA_ASSINADO_EM") or "—"

                st.markdown(f"### {nome}")
                st.caption(f"Status: **{status}**")
                st.caption(f"Categoria: **{categoria}** • Âmbito: **{ambito}**")
                st.caption(f"NDA: **{nda_info}**")

                rec_id = rec.get("ID") or f"{_slug(titulo_lista)}-{i}"
                # Prefixo de aba + id + índice => chave única global
                if st.button("Ver detalhes", key=f"{key_prefix}-det-{rec_id}-{i}", use_container_width=True):
                    open_details_dialog(rec)

# ======= Abas: Por Setor | Por Status =======
tab_setor, tab_status = st.tabs(["🔎 Filtro por Setor", "✅ Filtro por Status"])

with tab_setor:
    st.subheader("Filtrar por setor:")
    st.caption("Selecione um setor abaixo para ver os fornecedores.")

    # Botão CRIAR novo (logo abaixo do subtexto)
    st.button(
        "✚ Criar novo fornecedor",
        key="btn-create-setor",
        type="primary",                # deixa texto/ícone (➕) em branco
        use_container_width=True,
        on_click=lambda: open_create_dialog(None if st.session_state.filtro_setor_sel == "Todos" else st.session_state.filtro_setor_sel),
    )

    st.markdown("")  # pequeno respiro visual

    if st.session_state.view_setor == "select":
        # === Modo SELEÇÃO: mostrar botões de setores ===
        btn_cols = st.columns(3)
        for i, setor in enumerate(SETORES):
            with btn_cols[i % 3]:
                if st.button(setor, use_container_width=True, key=f"setor-{i}"):
                    st.session_state.filtro_setor_sel = setor
                    st.session_state.view_setor = "list"
                    st.rerun()
    else:
        # === Modo LISTA: mostrar resultado + botão Voltar ===
        setor_sel = st.session_state.filtro_setor_sel
        c1, = st.columns(1)
        if c1.button("⬅️ Voltar aos setores", key="voltar-setor", use_container_width=True):
            st.session_state.view_setor = "select"
            st.rerun()

        # Aplica filtro por setor
        if setor_sel == "Todos":
            df_view = df_all.copy()
        else:
            def _norm_local(s):
                return "" if s is None else str(s).strip().casefold()
            df_view = df_all[df_all["CATEGORIA"].apply(_norm_local) == _norm_local(setor_sel)].copy()

        render_cards(df_view, f"Setor: {setor_sel}", key_prefix=f"setor-{_slug(setor_sel)}")


with tab_status:
    st.subheader("Filtrar por status:")
    st.caption("Selecione um status abaixo para ver os fornecedores.")

    # Botão CRIAR novo (logo abaixo do subtexto)
    st.button(
        "✚ Criar novo fornecedor",
        key="btn-create-status",
        type="primary",               # texto/ícone branco
        use_container_width=True,
        on_click=lambda: open_create_dialog(None),
    )

    st.markdown("")

    if st.session_state.view_status == "select":
        # === Modo SELEÇÃO: mostrar botões de status ===
        btn_cols2 = st.columns(3)
        for i, stt in enumerate(STATUSES):
            with btn_cols2[i % 3]:
                if st.button(stt, use_container_width=True, key=f"status-{i}"):
                    st.session_state.filtro_status_sel = stt
                    st.session_state.view_status = "list"
                    st.rerun()
    else:
        # === Modo LISTA: mostrar resultado + botão Voltar ===
        status_sel = st.session_state.filtro_status_sel
        c2, = st.columns(1)
        if c2.button("⬅️ Voltar aos status", key="voltar-status", use_container_width=True):
            st.session_state.view_status = "select"
            st.rerun()

        # Aplica filtro por status — EXATO (sem normalização)
        if status_sel == "Todos":
            df_view_status = df_all.copy()
        else:
            df_view_status = df_all[
                (df_all["STATUS"].fillna("").apply(lambda x: x.strip()) == status_sel.strip())
            ].copy()

        render_cards(df_view_status, f"Status: {status_sel}", key_prefix=f"status-{_slug(status_sel)}")
