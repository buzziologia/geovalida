# validate_sede_analyzer.py
"""
Script de validação simples para SedeAnalyzer.
"""

from src.pipeline.sede_analyzer import SedeAnalyzer
import sys

def main():
    print("=" * 60)
    print("VALIDACAO DO SEDEANALYZER")
    print("=" * 60)
    
    # Criar analisador
    print("\n1. Inicializando SedeAnalyzer...")
    analyzer = SedeAnalyzer()
    print("   OK - SedeAnalyzer criado com sucesso")
    
    # Executar análise completa
    print("\n2. Executando analise de dependencias...")
    try:
        summary = analyzer.analyze_sede_dependencies()
        
        if summary.get('success'):
            print("   OK - Analise concluida com sucesso!")
            print("\n" + "=" * 60)
            print("RESUMO DA ANALISE")
            print("=" * 60)
            print(f"   Total de Sedes: {summary['total_sedes']}")
            print(f"   Alertas de Dependência: {summary['total_alertas']}")
            print(f"   UTPs Analisadas: {summary['utps_analisadas']}")
            print(f"   População Total: {summary['populacao_total']:,}")
            print(f"   Sedes com Aeroporto: {summary['sedes_com_aeroporto']}")
            
            # Exportar tabela
            print("\n3. Exportando tabela comparativa...")
            df = analyzer.export_sede_comparison_table()
            print(f"   OK - Tabela exportada: {len(df)} linhas")
            
            # Mostrar amostra
            if len(df) > 0:
                print("\n" + "=" * 60)
                print("AMOSTRA DA TABELA (primeiras 5 sedes)")
                print("=" * 60)
                print(df.head().to_string())
            
            # Mostrar alertas
            if summary['total_alertas'] > 0:
                print("\n" + "=" * 60)
                print(f"ALERTAS DE DEPENDENCIA ({summary['total_alertas']} detectados)")
                print("=" * 60)
                
                df_alertas = df[df['Alerta'] != '']
                for idx, row in df_alertas.head(5).iterrows():
                    print(f"\n  ALERTA: {row['Sede']} ({row['UF']})")
                    print(f"     -> Principal Destino: {row['Principal Destino']}")
                    print(f"     -> Fluxo: {row['Fluxo (%)']}%")
                    print(f"     -> Tempo: {row['Tempo (h)']}h")
            
            print("\n" + "=" * 60)
            print("OK - VALIDACAO CONCLUIDA COM SUCESSO!")
            print("=" * 60)
            return 0
            
        else:
            print(f"   ERRO na analise: {summary.get('error', 'Erro desconhecido')}")
            return 1
            
    except Exception as e:
        print(f"\n   ERRO - Excecao durante analise: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())
