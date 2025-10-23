import streamlit as st
from src.auth import is_authenticated

st.set_page_config(page_title="Início • Catálogo", layout="wide")

if not is_authenticated():
    st.info("Faça login para acessar.")
    st.stop()

st.title("🏠 Início")
st.markdown("""
Bem-vindo ao **Catálogo / Hub de Prospecção**.

Use o menu à esquerda para navegar:
- **Empresas**: visualizar os registros da base, com cache de leitura para mais velocidade.
- (Futuro) Outras páginas/recursos.

Dica: use o botão **Sair** na barra lateral quando precisar encerrar a sessão.
""")
