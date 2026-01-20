#!/usr/bin/env python3
"""
Script para criar o arquivo initialization.json a partir dos dados brutos (01_raw).
Simula o processo de carregamento inicial que o main.py faz, mas persiste o resultado em JSON.

Uso: python scripts/create_initialization.py
"""
import json
import pandas as pd
from pathlib import Path
import logging
import sys
from datetime import datetime

# Adicionar raiz do projeto ao path para importar src
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import FILES, DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("CreateInitialization")

def load_raw_data():
    """Carrega os dataframes brutos conforme configurado em src.config."""
    
    # 1. Carregar UTP Base
    utp_path = FILES['utp_base']
    logger.info(f"Carregando UTP Base de {utp_path}...")
    if not utp_path.exists():
        logger.error(f"Arquivo não encontrado: {utp_path}")
        return None, None

    if str(utp_path).endswith('.xlsx'):
        df_utp = pd.read_excel(utp_path, dtype=str)
    else:
        df_utp = pd.read_csv(utp_path, sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
    
    logger.info(f"  ✓ UTP: {len(df_utp)} linhas carregadas")

    # 2. Carregar SEDE + REGIC
    regic_path = FILES['sede_regic']
    logger.info(f"Carregando SEDE+REGIC de {regic_path}...")
    if not regic_path.exists():
        logger.error(f"Arquivo não encontrado: {regic_path}")
        return None, None
        
    if str(regic_path).endswith('.xlsx'):
        df_regic = pd.read_excel(regic_path, dtype=str)
    else:
        df_regic = pd.read_csv(regic_path, sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
        
    logger.info(f"  ✓ REGIC: {len(df_regic)} linhas carregadas")
    
    return df_utp, df_regic

def process_data(df_utp, df_regic):
    """Processa e combina os dados para o formato do JSON."""
    logger.info("Processando dados...")
    
    # Normalizar nomes de colunas
    df_utp.columns = df_utp.columns.str.lower().str.strip()
    df_regic.columns = df_regic.columns.str.lower().str.strip()
    
    logger.info(f"Colunas UTP disponíveis: {df_utp.columns.tolist()}")
    logger.info(f"Colunas REGIC disponíveis: {df_regic.columns.tolist()}")
    
    # Criar dicionário de municípios
    municipios = {}
    
    # Processar UTP
    # Mapear colunas variáveis
    # Prioridade para colunas conhecidas
    possible_cd = [c for c in df_utp.columns if 'cd_mun' in c or 'cod' in c]
    col_cd_mun = possible_cd[0] if possible_cd else 'cd_mun'
    
    possible_nm = [c for c in df_utp.columns if 'nm_mun' in c or 'nome' in c]
    col_nm_mun = possible_nm[0] if possible_nm else 'nm_mun'
    
    possible_utp = [c for c in df_utp.columns if 'utp' in c]
    col_utp = possible_utp[0] if possible_utp else 'utp_id'
    
    possible_rm = [c for c in df_utp.columns if 'metropolitana' in c or 'rm' in c]
    col_rm = possible_rm[0] if possible_rm else None

    logger.info(f"Colunas detecatadas: CD={col_cd_mun}, NM={col_nm_mun}, UTP={col_utp}, RM={col_rm}")

    for _, row in df_utp.iterrows():
        try:
            if col_cd_mun not in row:
                continue
                
            cd_mun = int(float(row[col_cd_mun]))
            
            rm_val = ''
            if col_rm and col_rm in row and pd.notna(row[col_rm]):
                rm_val = str(row[col_rm]).strip()
                
            mun_data = {
                'cd_mun': cd_mun,
                'nm_mun': str(row[col_nm_mun]).strip() if col_nm_mun in row else '',
                'utp_id': str(row[col_utp]).strip() if col_utp in row else '',
                'regiao_metropolitana': rm_val,
                'uf': str(row.get('uf', '')).strip(),
                'sede_utp': False,
                'regic': ''
            }
            municipios[cd_mun] = mun_data
        except ValueError:
            continue

    # Processar REGIC
    possible_cd_regic = [c for c in df_regic.columns if 'cd_mun' in c or 'cod' in c]
    col_cd_sede = possible_cd_regic[0] if possible_cd_regic else 'cd_mun'
    
    possible_regic = [c for c in df_regic.columns if 'regic' in c]
    col_regic = possible_regic[0] if possible_regic else 'regic'
    
    logger.info(f"Colunas REGIC detecatadas: CD={col_cd_sede}, REGIC={col_regic}")
    
    sedes_count = 0
    for _, row in df_regic.iterrows():
        try:
            if col_cd_sede not in row:
                continue
                
            cd_mun = int(float(row[col_cd_sede]))
            if cd_mun in municipios:
                municipios[cd_mun]['sede_utp'] = True
                municipios[cd_mun]['regic'] = str(row[col_regic]).strip() if col_regic in row else ''
                sedes_count += 1
        except ValueError:
            continue
            
    logger.info(f"  ✓ {len(municipios)} municípios processados")
    logger.info(f"  ✓ {sedes_count} sedes identificadas")
    
    return list(municipios.values())

def main():
    logger.info("=" * 80)
    logger.info("CRIAÇÃO DO INITIALIZATION.JSON")
    logger.info("=" * 80)
    
    df_utp, df_regic = load_raw_data()
    
    if df_utp is None or df_regic is None:
        logger.error("Falha ao carregar dados brutos.")
        return 1
        
    municipios_list = process_data(df_utp, df_regic)
    
    # Criar estrutura final
    data = {
        "metadata": {
            "created_at": datetime.now().isoformat(),
            "source_files": [str(FILES['utp_base'].name), str(FILES['sede_regic'].name)],
            "total_municipios": len(municipios_list)
        },
        "municipios": municipios_list,
        "utps": [] # Pode ser preenchido futuramente se necessário
    }
    
    # Salvar JSON
    output_path = DATA_DIR / "initialization.json"
    logger.info(f"Salvando em {output_path}...")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    logger.info("✅ Arquivo initialization.json criado com sucesso!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
