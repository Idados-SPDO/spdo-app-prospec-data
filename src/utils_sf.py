# utils_sf.py
import pandas as pd
import unicodedata
import re
from snowflake.snowpark import Session
import streamlit as st
import os
from io import BytesIO
from uuid import uuid4
from datetime import datetime
from zoneinfo import ZoneInfo


FQN_MAIN = "BASES_SPDO.DB_APP_PROSPEC_DATA.TB_EMPRESAS"
FQN_COMMENTS = "BASES_SPDO.DB_APP_PROSPEC_DATA.TB_EMPRESAS_COMENTARIOS"


def get_session() -> Session:
    return Session.builder.configs(st.secrets["snowflake"]).create()

def _sf(q: str):
    return get_session().sql(q)

def _sanitize_colname(name: str) -> str:
    s = str(name or "").strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^A-Z0-9]+", "_", s).strip("_")
    return s or "COLUNA"

def _dedupe_columns(cols: list[str]) -> list[str]:
    seen, out = {}, []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out

NUMERIC_COLS = {"CLASSIFICACAO", "VALIDADE_EM_ANOS", "VALIDADE_EM_DIAS", "VALIDADE_EM_MESES"}
DATE_COLS    = {"DATA_DA_ASSINATURA", "INICIO_RENOVACACAO_DATA_ASSINATURA", "VIGENCIA"}

def _to_number(v):
    try:
        if v is None: return None
        s = str(v).strip()
        if s == "" or s.lower() in {"nan", "nat", "none"}: return None
        s = s.replace(",", ".")
        f = float(s)
        return int(f) if f.is_integer() else f
    except Exception:
        return None

def _to_date_ddmmyyyy(v):
    try:
        if v is None: return None
        s = str(v).strip()
        if s == "" or s.lower() in {"nan", "nat", "none", "-"}: return None
        d = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return d.date() if pd.notna(d) else None
    except Exception:
        return None

def table_exists(fqn: str) -> bool:
    db, schema, table = fqn.split(".")
    q = f"""
      SELECT 1
      FROM {db}.INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
      LIMIT 1
    """
    return not _sf(q).to_pandas().empty

def ddl_from_excel_columns(fqn: str, cols: list[str]) -> str:
    parts = []
    for c in cols:
        if c in NUMERIC_COLS: coltype = "NUMBER"
        elif c in DATE_COLS:  coltype = "DATE"
        else:                 coltype = "VARCHAR"
        parts.append(f'"{c}" {coltype}')
    cols_sql = ",\n  ".join(parts)
    return f'CREATE TABLE {fqn} (\n  {cols_sql}\n)'

def create_table_and_load_from_excel(fqn: str, df_raw: pd.DataFrame) -> int:
    sanitized = _dedupe_columns([_sanitize_colname(c) for c in df_raw.columns])
    df = df_raw.copy()
    df.columns = sanitized

    _sf(ddl_from_excel_columns(fqn, sanitized)).collect()

    for c in df.columns:
        if c in NUMERIC_COLS:
            df[c] = df[c].apply(_to_number)
        elif c in DATE_COLS:
            df[c] = df[c].apply(_to_date_ddmmyyyy)
        else:
            df[c] = df[c].apply(lambda x: None if pd.isna(x) or str(x).strip().lower() in {"nan", "nat"} else str(x))

    sess = get_session()
    sess.write_pandas(
        df,
        table_name=fqn.split('.')[-1],
        database=fqn.split('.')[0],
        schema=fqn.split('.')[1],
        overwrite=False,
        auto_create_table=False,
        quote_identifiers=True
    )
    fetch_companies.clear()
    return len(df)

def append_excel_into_table(fqn: str, df_raw: pd.DataFrame) -> int:
    sanitized = _dedupe_columns([_sanitize_colname(c) for c in df_raw.columns])
    df2 = df_raw.copy()
    df2.columns = sanitized

    for c in df2.columns:
        if c in NUMERIC_COLS:
            df2[c] = df2[c].apply(_to_number)
        elif c in DATE_COLS:
            df2[c] = df2[c].apply(_to_date_ddmmyyyy)
        else:
            df2[c] = df2[c].apply(lambda x: None if pd.isna(x) or str(x).strip().lower() in {"nan", "nat"} else str(x))

    sess = get_session()
    sess.write_pandas(
        df2,
        table_name=fqn.split('.')[-1],
        database=fqn.split('.')[0],
        schema=fqn.split('.')[1],
        overwrite=False,
        auto_create_table=False,
        quote_identifiers=True
    )
    fetch_companies.clear()
    return len(df2)

@st.cache_data(ttl=300, show_spinner=False)
def fetch_companies(limit: int | None = None) -> pd.DataFrame:
    """Cache sem ORDER BY UPDATED_AT."""
    sql = f"SELECT * FROM {FQN_MAIN}"
    if limit and int(limit) > 0:
        sql += f" LIMIT {int(limit)}"
    return _sf(sql).to_pandas()

def clear_companies_cache():
    fetch_companies.clear()

STAGE_APRES = '@ST_APRESENTACOES'   # ex.: @"DB"."SCHEMA"."ST_APRESENTACOES"

def _quote_ident(ident: str) -> str:
    ident = str(ident or "").replace('"', '""')
    return f'"{ident}"'

def _sql_literal(val) -> str:
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    s = str(val).replace("'", "''")
    return f"'{s}'"

def update_company_row(fqn: str, key_col: str, key_val, updates: dict) -> None:
    """
    UPDATE ... SET ... WHERE key_col = key_val
    'updates' = {nome_coluna_exata: novo_valor}
    """
    if not updates:
        return
    set_clauses = [f"{_quote_ident(c)} = {_sql_literal(v)}" for c, v in updates.items()]
    where_clause = f"{_quote_ident(key_col)} = {_sql_literal(key_val)}"
    sql = f"UPDATE {fqn} SET {', '.join(set_clauses)} WHERE {where_clause}"
    _sf(sql).collect()
    fetch_companies.clear()

def slug(s: str) -> str:
    base = str(s or "").strip().lower()
    base = unicodedata.normalize("NFD", base)
    base = "".join(ch for ch in base if unicodedata.category(ch) != "Mn")
    base = re.sub(r"[^a-z0-9\-]+", "-", base)
    base = re.sub(r"-{2,}", "-", base).strip("-")
    return base or "empresa"

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))

def stage_prefix_for_company(row: dict, col_cnpj: str | None, col_nome: str | None) -> str:
    """Pasta da empresa no stage (preferindo CNPJ dígitos)."""
    cnpj_digits = only_digits(row.get(col_cnpj)) if col_cnpj else ""
    if cnpj_digits:
        return f"{cnpj_digits}/"
    nome = row.get(col_nome) if col_nome else None
    return f"{slug(nome)}/"

def list_presentations(prefix: str):
    """
    Retorna DataFrame de LIST no stage para o prefix informado.
    """
    sess = get_session()
    return sess.sql(f"LIST {STAGE_APRES}/{prefix}").to_pandas()

def upload_presentation(prefix: str, file_obj, filename: str) -> None:
    """
    Sobe um arquivo para {STAGE_APRES}/{prefix}{filename}
    """
    sess = get_session()
    target = f"{STAGE_APRES}/{prefix}{filename}"
    # Snowpark moderno
    try:
        sess.file.put_stream(file_obj, target, auto_compress=False, overwrite=True)
    except Exception:
        # Fallback: grava temporário e usa PUT file://
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(file_obj.read())
            tmp.flush()
            tmp_path = tmp.name
        _sf(f"PUT file://{tmp_path} {target} AUTO_COMPRESS=FALSE OVERWRITE=TRUE").collect()
        try:
            os.remove(tmp_path)
        except Exception:
            pass

# --- substitua sua download_presentation por esta versão ---
def download_presentation(prefix: str, filename: str) -> bytes:
    """
    Tenta baixar bytes do arquivo no stage:
    1) get_stream (Snowpark moderno)
    2) fallback: GET para uma pasta temporária e lê o primeiro arquivo salvo
    """
    sess = get_session()
    path = f"{STAGE_APRES}/{prefix}{filename}"

    # 1) Caminho preferido: streaming
    try:
        if hasattr(sess, "file") and hasattr(sess.file, "get_stream"):
            stream = sess.file.get_stream(path)
            return stream.read()
    except Exception:
        pass  # cai no fallback

    # 2) Fallback robusto com TemporaryDirectory
    import tempfile, pathlib, glob
    with tempfile.TemporaryDirectory() as tmpdir:
        _sf(f"GET {path} file://{tmpdir} OVERWRITE=TRUE").collect()
        # Procura qualquer arquivo salvo no diretório (às vezes o nome pode ter sufixo)
        files = glob.glob(str(pathlib.Path(tmpdir) / "**" / "*"), recursive=True)
        files = [f for f in files if pathlib.Path(f).is_file()]
        if not files:
            raise FileNotFoundError("GET executado, mas nenhum arquivo encontrado no diretório temporário.")
        # Prioriza o que termina com o nome esperado; se não houver, pega o primeiro
        target = next((f for f in files if pathlib.Path(f).name == filename), files[0])
        with open(target, "rb") as f:
            return f.read()

# --- nova função (opcional) para link direto de download ---
def get_presigned_url(prefix: str, filename: str, expires_seconds: int = 3600) -> str | None:
    """
    Tenta gerar uma URL pré-assinada. Requer permissões e, em geral, funciona para
    stages externos. Para stages internos, pode não estar habilitado.
    """
    try:
        q = f"select SYSTEM$GET_PRESIGNED_URL('{STAGE_APRES}', '{prefix}{filename}', {int(expires_seconds)}) as URL"
        df = _sf(q).to_pandas()
        if not df.empty:
            # coluna pode vir como 'URL' ou 'URL' minúsculo dependendo do driver
            for col in df.columns:
                if col.lower() == "url":
                    return df.iloc[0][col]
    except Exception:
        return None
    return None

    """
    Baixa e retorna bytes do arquivo no stage.
    """
    sess = get_session()
    path = f"{STAGE_APRES}/{prefix}{filename}"
    # Snowpark moderno
    try:
        stream = sess.file.get_stream(path)
        return stream.read()
    except Exception:
        # Fallback via GET para diretório temporário e leitura
        import tempfile, pathlib
        tmpdir = tempfile.mkdtemp()
        _sf(f"GET {path} file://{tmpdir} OVERWRITE=TRUE").collect()
        # GET salva com mesmo nome
        local = pathlib.Path(tmpdir) / filename
        with open(local, "rb") as f:
            data = f.read()
        try:
            os.remove(local)
        except Exception:
            pass
        return data
    
def _to_naive_br(dt: datetime) -> datetime:
    """
    Converte para horário de Brasília e remove tzinfo (naive),
    ideal para salvar em coluna TIMESTAMP_NTZ.
    """
    if isinstance(dt, datetime):
        if dt.tzinfo is not None:
            dt = dt.astimezone(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
        # se já for naive, deixa como está (assumindo que já está em BR)
        return dt
    # fallback: agora em BR, naive
    return datetime.now(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)

def save_company_comment(empresa_id: str, username: str, name: str, message: str, created_at: datetime) -> None:
    session = get_session()
    created_at_naive = _to_naive_br(created_at)

    rows = [(str(uuid4()), str(empresa_id), str(username), str(name), str(message), created_at_naive)]
    sdf = session.create_dataframe(
        rows,
        schema=["ID","EMPRESA_ID","USERNAME","NAME","MESSAGE","CREATED_AT"]
    )
    sdf.write.mode("append").save_as_table(FQN_COMMENTS)

def fetch_company_comments(empresa_id: str) -> pd.DataFrame:
    session = get_session()
    df = (
        session.table(FQN_COMMENTS)
        .filter(f"EMPRESA_ID = '{empresa_id}'")
        .sort("CREATED_AT", ascending=False)
    )
    return df.to_pandas()

def delete_company_row(table_fqn: str, key_col: str, key_value):
    """
    Exclui a empresa na tabela principal.
    OBS: Se quiser também excluir comentários dessa empresa, descomente as 2 linhas indicadas.
    """
    session = get_session()
    # Excluir comentários (opcional)
    # session.sql(f"DELETE FROM {FQN_COMMENTS} WHERE EMPRESA_ID = '{key_value}'").collect()
    session.sql(f"DELETE FROM {table_fqn} WHERE {key_col} = '{key_value}'").collect()



def create_company_row(table_fqn: str, values: dict) -> None:
    """
    Insere uma nova empresa. 'values' deve mapear {coluna: valor}.
    Gera um ID (UUID) se a coluna ID existir e não vier preenchida.
    """
    session = get_session()
    # Garante ID se a tabela tiver coluna ID e não enviaram
    cols_lower = {c.lower() for c in session.table(table_fqn).schema.names}
    if "id" in cols_lower:
        # encontra o nome real da coluna ID respeitando case
        id_real = [c for c in session.table(table_fqn).schema.names if c.lower() == "id"][0]
        values.setdefault(id_real, str(uuid4()))
    # Cria DF de 1 linha e faz append
    cols = list(values.keys())
    rows = [tuple(values[c] for c in cols)]
    sdf = session.create_dataframe(rows, schema=cols)
    sdf.write.mode("append").save_as_table(table_fqn)

def fetch_comments_all() -> pd.DataFrame:
    """Retorna todos os comentários."""
    session = get_session()
    return session.table(FQN_COMMENTS).to_pandas()