import streamlit as st
from src.auth import init_auth, is_authenticated, login_user, current_user, logout_user

st.set_page_config(page_title="Login • Catálogo", layout="wide", initial_sidebar_state="collapsed")
init_auth()

# Esconde sidebar e ícone do header SOMENTE nesta página
st.markdown("""
<style>
[data-testid="stSidebar"] { display: none !important; }
button[kind="header"] { display: none !important; }
</style>
""", unsafe_allow_html=True)

st.title("🔐 Login")

if is_authenticated():
    u = current_user()
    st.success(f"Você já está logado como **{u['name']}** (`{u['username']}`).")
    st.page_link("pages/1_Home.py", label="Ir para Início", icon="🏠")
    if st.button("🚪 Sair", key="btn_logout_login_page"):
        logout_user()
        st.rerun()
else:
    with st.form("login_form"):
        username = st.text_input("Usuário", placeholder="ex: spdo_admin")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar", width='stretch')

    if submitted:
        u = (username or "").strip()
        p = (password or "").strip()
        if not u or not p:
            st.warning("Informe usuário e senha.")
        else:
            if login_user(u, p):
                st.success("Login realizado!")
                st.rerun()  # volta ao main; menu troca para Home/Empresas
            else:
                st.error("Usuário ou senha inválidos.")
