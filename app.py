# app.py (Raiz)
import streamlit as st
from src.core.manager import GeoValidaManager
from src.interface.dashboard import render_dashboard
from src.utils import DataLoader
import logging

# Configurar logger
logging.basicConfig(level=logging.WARNING)

# Inicializar manager apenas uma vez
@st.cache_resource
def get_manager():
    """Cria e inicializa o manager uma única vez"""
    manager = GeoValidaManager()
    manager.step_0_initialize_data()
    return manager

# Carregar dados do JSON para uso no dashboard
@st.cache_resource
def load_json_data():
    """Cache do DataLoader"""
    return DataLoader()

# Obter instâncias únicas
manager = get_manager()
data_loader = load_json_data()

# Renderizar dashboard
render_dashboard(manager)
