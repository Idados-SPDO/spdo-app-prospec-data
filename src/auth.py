import streamlit as st

USERS = {
    "spdo_visual": {"password": "123", "name": "SPDO Visual", "role": "visual"},
    "spdo_admin":  {"password": "123", "name": "SPDO Admin",  "role": "admin"},
}

def init_auth():
    if "auth" not in st.session_state:
        st.session_state.auth = {"is_auth": False, "user": None}

def is_authenticated() -> bool:
    return bool(st.session_state.get("auth", {}).get("is_auth", False))

def current_user():
    return st.session_state.get("auth", {}).get("user")

def login_user(username: str, password: str) -> bool:
    u = USERS.get(username)
    if u and password == u["password"]:
        st.session_state.auth = {"is_auth": True, "user": {"username": username, **u}}
        return True
    return False

def logout_user():
    st.session_state.clear()
    st.session_state.auth = {"is_auth": False, "user": None}
