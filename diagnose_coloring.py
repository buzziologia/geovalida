#!/usr/bin/env python3
"""
Script avancado para diagnosticar problema de coloracao no dashboard.
Verifica especificamente UTP 677 e Fonte Boa (1301605).
"""

import json
from pathlib import Path
from collections import defaultdict

# Paths
DATA_DIR = Path(__file__).parent / "data"
INIT_JSON = DATA_DIR / "initialization.json"
CONSOLIDATION_JSON = DATA_DIR / "consolidation_result.json"
INITIAL_COLORING = DATA_DIR / "initial_coloring.json"
CONSOLIDATED_COLORING = DATA_DIR / "consolidated_coloring.json"

def load_json(path):
    if not path.exists():
        print(f"[X] Nao encontrado: {path.name}")
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def analyze_utp(utp_id, municipios, initial_colors, consolidated_colors, consolidation=None):
    """Analisa uma UTP especifica."""
    print(f"\n{'='*80}")
    print(f"ANALISE DA UTP {utp_id}")
    print(f"{'='*80}")
    
    # Municipios iniciais desta UTP
    muns_initial = [m for m in municipios if str(m['utp_id']) == utp_id]
    
    if not muns_initial:
        print(f"[X] UTP {utp_id} nao encontrada no estado inicial")
        return
    
    print(f"\nEstado INICIAL - {len(muns_initial)} municipios:")
    color_groups = defaultdict(list)
    
    for mun in muns_initial[:10]:  # Primeiros 10
        cd_mun = str(mun['cd_mun'])
        color_initial = initial_colors.get(cd_mun, initial_colors.get(int(cd_mun), None))
        color_groups[color_initial].append(f"{mun['nm_mun']} ({cd_mun})")
    
    for color, muns in color_groups.items():
        print(f"  Cor {color}: {', '.join(muns)}")
    
    # Estado pos-consolidacao
    if consolidation:
        utp_mapping = consolidation.get('utps_mapping', {})
        
        # Ver se esta UTP foi consolidada
        if utp_id in utp_mapping:
            new_utp = utp_mapping[utp_id]
            print(f"\n[!] UTP {utp_id} foi CONSOLIDADA para UTP {new_utp}")
            
            # Municipios da nova UTP
            muns_consolidated = []
            for m in municipios:
                old_utp = str(m['utp_id'])
                final_utp = utp_mapping.get(old_utp, old_utp)
                if final_utp == new_utp:
                    muns_consolidated.append(m)
            
            print(f"\nEstado POS-CONSOLIDACAO - {len(muns_consolidated)} municipios (UTP {new_utp}):")
            color_groups_consolidated = defaultdict(list)
            
            for mun in muns_consolidated[:10]:
                cd_mun = str(mun['cd_mun'])
                color_consolidated = consolidated_colors.get(cd_mun, consolidated_colors.get(int(cd_mun), None))
                color_groups_consolidated[color_consolidated].append(f"{mun['nm_mun']} ({cd_mun})")
            
            for color, muns in color_groups_consolidated.items():
                print(f"  Cor {color}: {', '.join(muns)}")
            
            # Diagnostico
            if len(color_groups_consolidated) > 1:
                print(f"\n[X] PROBLEMA: UTP consolidada tem {len(color_groups_consolidated)} cores diferentes!")
            else:
                print(f"\n[OK] UTP consolidada tem coloracao consistente")
        else:
            print(f"\n[INFO] UTP {utp_id} nao foi consolidada (permanece a mesma)")

def main():
    print("DIAGNOSTICO AVANCADO DE COLORACAO")
    print("="*80)
    
    # Carregar dados
    init_data = load_json(INIT_JSON)
    consolidation = load_json(CONSOLIDATION_JSON)
    initial_colors = load_json(INITIAL_COLORING)
    consolidated_colors = load_json(CONSOLIDATED_COLORING)
    
    if not all([init_data, initial_colors, consolidated_colors]):
        print("\n[X] Arquivos necessarios nao encontrados!")
        return
    
    municipios = init_data['municipios']
    
    print(f"\nArquivos carregados:")
    print(f"  - {len(municipios)} municipios")
    print(f"  - {len(initial_colors)} cores iniciais")
    print(f"  - {len(consolidated_colors)} cores consolidadas")
    
    # Analisar UTP 677
    analyze_utp("677", municipios, initial_colors, consolidated_colors, consolidation)
    
    # Analisar Fonte Boa
    print(f"\n{'='*80}")
    print(f"ANALISE DE FONTE BOA (1301605)")
    print(f"{'='*80}")
    
    fonte_boa = None
    for m in municipios:
        if str(m['cd_mun']) == "1301605":
            fonte_boa = m
            break
    
    if fonte_boa:
        cd_mun = "1301605"
        utp_initial = str(fonte_boa['utp_id'])
        color_initial = initial_colors.get(cd_mun, initial_colors.get(int(cd_mun), None))
        color_consolidated = consolidated_colors.get(cd_mun, consolidated_colors.get(int(cd_mun), None))
        
        print(f"Nome: {fonte_boa['nm_mun']}")
        print(f"UTP Inicial: {utp_initial}")
        print(f"Cor Inicial: {color_initial}")
        print(f"Cor Consolidada: {color_consolidated}")
        
        if consolidation:
            utp_mapping = consolidation.get('utps_mapping', {})
            new_utp = utp_mapping.get(utp_initial, utp_initial)
            print(f"UTP Pos-Consolidacao: {new_utp}")
            
            if color_initial == color_consolidated:
                print(f"\n[!] ATENCAO: Cores sao iguais, mas UTP mudou!")
                print(f"   Isso indica que o cache consolidated_coloring.json")
                print(f"   pode estar identico ao initial_coloring.json")
            else:
                print(f"\n[OK] Cores diferentes (esperado apos consolidacao)")

if __name__ == "__main__":
    main()
