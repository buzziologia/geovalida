#!/usr/bin/env python3
# main.py
"""
Script mestre do GeoValida.
Orquestra todo o processo desde a inicialização de dados até a consolidação final.
"""
import sys
import logging
from src.config import setup_logging

# Importar ferramentas
from data.create_initialization_json_v2 import main as initialize_data
from src.run_consolidation import run_consolidation

# Configurar logger
logger = setup_logging()

def run_pipeline():
    """Executa o pipeline completo do GeoValida."""
    logger.info("*" * 80)
    logger.info("           GEOVALIDA - PIPELINE MASTER           ")
    logger.info("*" * 80)
    
    # 1. Inicializar Dados
    logger.info("\n>>> ETAPA 1: INICIALIZAÇÃO DE DADOS <<<")
    try:
        initialize_data()
        logger.info("✅ Inicialização de dados concluída.")
    except Exception as e:
        logger.error(f"❌ Falha na inicialização de dados: {e}")
        return False
        
    # 2. Executar Consolidação
    logger.info("\n>>> ETAPA 2: PROCESSAMENTO E CONSOLIDAÇÃO <<<")
    try:
        success = run_consolidation()
        if success:
            logger.info("✅ Consolidação concluída.")
        else:
            logger.error("❌ Falha na consolidação.")
            return False
    except Exception as e:
        logger.error(f"❌ Erro crítico na consolidação: {e}")
        return False
        
    logger.info("\n" + "*" * 80)
    logger.info("✅ PIPELINE COMPLETO FINALIZADO COM SUCESSO")
    logger.info("*" * 80)
    return True

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)