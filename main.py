import streamlit as st
from pathlib import Path
from src.auth import init_auth, is_authenticated, current_user, logout_user

st.set_page_config(page_title="Catálogo de Insumos", layout="wide", initial_sidebar_state="expanded")

# Logo (funciona tanto no header quanto fallback na sidebar)
logo_path = Path("assets") / "logo_ibre.png"
if logo_path.exists():
    try:
        st.logo(str(logo_path))
    except Exception:
        pass

# Auth bootstrap
init_auth()

# --- Definição de páginas ---
LOGIN_PAGE = [st.Page("pages/0_Login.py", title="🔐 Login")]
APP_PAGES  = [
    st.Page("pages/1_Home.py",     title="🏠 Início"),
    st.Page("pages/2_Empresas.py", title="🏢 Empresas"),
]

# --- Registra a navegação (apenas o que o usuário pode ver) ---
nav = st.navigation({"Navegação": APP_PAGES} if is_authenticated() else {"Acesso": LOGIN_PAGE})

# --- Sidebar global: só aparece quando autenticado ---
if is_authenticated():
    with st.sidebar:
        u = current_user()
        st.caption("Logado como")
        st.markdown(f"**{u['name']}** (`{u['username']}`)")
        if st.button("🚪 Sair", use_container_width=True, key="logout_sidebar"):
            logout_user()
            st.rerun()

# --- Roda a navegação ---
nav.run()
