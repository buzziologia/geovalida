#!/usr/bin/env python3
"""
Script para verificar a consistência da coloração de grafos.
Verifica se todos os municípios de uma mesma UTP têm a mesma cor.
"""

import json
import pandas as pd
from pathlib import Path
from collections import defaultdict

# Paths
DATA_DIR = Path(__file__).parent / "data"
INIT_JSON = DATA_DIR / "initialization.json"
CONSOLIDATION_JSON = DATA_DIR / "consolidation_result.json"
OLD_CACHE = DATA_DIR / "map_coloring_cache.json"

def load_json(path):
    """Carrega arquivo JSON."""
    if not path.exists():
        print(f"❌ Arquivo não encontrado: {path}")
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def check_coloring_consistency(municipios_list, coloring, mapping_name=""):
    """Verifica se todos os municípios de uma mesma UTP têm a mesma cor."""
    print(f"\n{'='*80}")
    print(f"Verificando consistência: {mapping_name}")
    print(f"{'='*80}")
    
    # Agrupar por UTP
    utp_colors = defaultdict(set)
    utp_municipalities = defaultdict(list)
    
    for mun in municipios_list:
        cd_mun = str(mun['cd_mun'])
        utp_id = str(mun['utp_id'])
        nm_mun = mun.get('nm_mun', 'N/A')
        
        # Obter cor do município
        color = coloring.get(cd_mun, coloring.get(int(cd_mun), None))
        
        if color is not None:
            utp_colors[utp_id].add(color)
            utp_municipalities[utp_id].append({
                'cd_mun': cd_mun,
                'nm_mun': nm_mun,
                'color': color
            })
    
    # Verificar inconsistências
    inconsistent_utps = []
    for utp_id, colors in utp_colors.items():
        if len(colors) > 1:
            inconsistent_utps.append({
                'utp_id': utp_id,
                'num_colors': len(colors),
                'colors': list(colors),
                'municipalities': utp_municipalities[utp_id]
            })
    
    # Relatório
    total_utps = len(utp_colors)
    if inconsistent_utps:
        print(f"[X] ERRO: {len(inconsistent_utps)} de {total_utps} UTPs com cores inconsistentes!")
        print(f"\nPrimeiras 10 UTPs com problemas:\n")
        
        for i, utp in enumerate(inconsistent_utps[:10], 1):
            print(f"\n{i}. UTP {utp['utp_id']} - {utp['num_colors']} cores diferentes:")
            
            # Agrupar municípios por cor
            by_color = defaultdict(list)
            for mun in utp['municipalities']:
                by_color[mun['color']].append(f"{mun['nm_mun']} ({mun['cd_mun']})")
            
            for color, muns in by_color.items():
                print(f"   Cor {color}: {', '.join(muns[:3])}", end='')
                if len(muns) > 3:
                    print(f" (+{len(muns)-3} outros)")
                else:
                    print()
    else:
        print(f"[OK] Todas as {total_utps} UTPs tem coloracao consistente!")
    
    return inconsistent_utps

def analyze_specific_case(cd_mun, municipios_list, coloring, consolidation_data=None):
    """Analisa um município específico."""
    print(f"\n{'='*80}")
    print(f"Análise do Município: {cd_mun}")
    print(f"{'='*80}")
    
    # Encontrar município
    mun = None
    for m in municipios_list:
        if str(m['cd_mun']) == str(cd_mun):
            mun = m
            break
    
    if not mun:
        print(f"❌ Município {cd_mun} não encontrado!")
        return
    
    print(f"Nome: {mun.get('nm_mun', 'N/A')}")
    print(f"UTP Inicial: {mun['utp_id']}")
    
    # Verificar se foi consolidado
    if consolidation_data:
        utp_mapping = consolidation_data.get('utps_mapping', {})
        old_utp = str(mun['utp_id'])
        new_utp = utp_mapping.get(old_utp, old_utp)
        if old_utp != new_utp:
            print(f"UTP Pós-Consolidação: {new_utp} (consolidado de {old_utp})")
        else:
            print(f"UTP Pós-Consolidação: {new_utp} (sem mudança)")
    
    # Verificar cor
    color = coloring.get(str(cd_mun), coloring.get(int(cd_mun), None))
    print(f"Cor atribuída: {color}")
    
    # Verificar outros municípios da mesma UTP
    utp_id = str(mun['utp_id'])
    same_utp = [m for m in municipios_list if str(m['utp_id']) == utp_id]
    
    print(f"\nOutros municipios da UTP {utp_id}:")
    for other in same_utp[:5]:
        other_cd = str(other['cd_mun'])
        other_color = coloring.get(other_cd, coloring.get(int(other_cd), 'N/A'))
        match = "[OK]" if other_color == color else "[X]"
        print(f"  {match} {other.get('nm_mun', 'N/A')} ({other_cd}): cor {other_color}")
    
    if len(same_utp) > 5:
        print(f"  ... e mais {len(same_utp) - 5} municípios")

def main():
    print("VERIFICACAO DE CONSISTENCIA DA COLORACAO DE GRAFOS")
    print("="*80)
    
    # 1. Carregar dados
    init_data = load_json(INIT_JSON)
    if not init_data:
        return
    
    municipios = init_data['municipios']
    print(f"OK - {len(municipios)} municipios carregados do initialization.json")
    
    # 2. Carregar cache de coloração
    coloring = load_json(OLD_CACHE)
    if not coloring:
        return
    
    print(f"OK - {len(coloring)} municipios com cores no cache")
    
    # 3. Verificar consistência INICIAL
    inconsistent = check_coloring_consistency(municipios, coloring, "ESTADO INICIAL")
    
    # 4. Verificar caso específico: Fonte Boa
    analyze_specific_case("1301605", municipios, coloring)
    
    # 5. Carregar consolidação e verificar novamente
    consolidation = load_json(CONSOLIDATION_JSON)
    if consolidation:
        print(f"\nOK - Consolidation result carregado")
        
        # Aplicar mapeamento
        utp_mapping = consolidation.get('utps_mapping', {})
        municipios_consolidated = []
        for mun in municipios:
            mun_copy = mun.copy()
            old_utp = str(mun_copy['utp_id'])
            if old_utp in utp_mapping:
                mun_copy['utp_id'] = utp_mapping[old_utp]
            municipios_consolidated.append(mun_copy)
        
        # Verificar consistência PÓS-CONSOLIDAÇÃO
        inconsistent_post = check_coloring_consistency(
            municipios_consolidated, 
            coloring, 
            "ESTADO POS-CONSOLIDACAO (usando cache do estado inicial)"
        )
        
        # Analisar Fonte Boa pós-consolidação
        analyze_specific_case("1301605", municipios_consolidated, coloring, consolidation)
        
        print(f"\n{'='*80}")
        print("CONCLUSAO")
        print(f"{'='*80}")
        print(f"""
O problema identificado:
- O cache atual contem cores baseadas nas UTPs INICIAIS
- Quando aplicamos consolidacao, os municipios mudam de UTP
- Mas as CORES nao sao recalculadas para refletir as novas UTPs!

SOLUCAO:
Precisamos gerar DOIS caches separados:
1. initial_coloring.json - coloracao baseada nas UTPs iniciais
2. consolidated_coloring.json - coloracao recalculada apos consolidacao

Para gerar os novos caches, execute:
    python main.py
""")

if __name__ == "__main__":
    main()
