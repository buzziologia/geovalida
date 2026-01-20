#!/usr/bin/env python3
# src/run_consolidation.py
"""
Script para executar a consolidação de UTPs e gerar o cache
Execute com: python src/run_consolidation.py
"""
import sys
from pathlib import Path

# Adicionar raiz do projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.manager import GeoValidaManager
from src.interface.consolidation_manager import ConsolidationManager
from src.interface.consolidation_loader import ConsolidationLoader
import logging

# Configurar logging para debug
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ConsolidationRunner")


def run_consolidation():
    """Executa o pipeline de consolidação completo."""
    logger.info("=" * 80)
    logger.info("INICIANDO EXECUÇÃO DE CONSOLIDAÇÃO DE UTPS")
    logger.info("=" * 80)
    
    try:
        # 1. Inicializar manager
        logger.info("\n1️⃣ Inicializando manager...")
        manager = GeoValidaManager()
        
        # 2. Carregar dados (Etapa 0)
        logger.info("\n2️⃣ Etapa 0: Carregando dados...")
        try:
            if not manager.step_0_initialize_data():
                logger.error("❌ Falha ao carregar dados!")
                return False
            logger.info("✅ Dados carregados com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro durante carregamento de dados: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        # 3. Gerar mapa inicial (Etapa 1)
        logger.info("\n3️⃣ Etapa 1: Gerando mapa inicial...")
        try:
            manager.step_1_generate_initial_map()
            logger.info("✅ Mapa inicial gerado!")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao gerar mapa inicial: {e}")
        
        # 4. Analisar fluxos (Etapa 2)
        logger.info("\n4️⃣ Etapa 2: Analisando fluxos...")
        try:
            manager.step_2_analyze_flows()
            logger.info("✅ Fluxos analisados!")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao analisar fluxos: {e}")
        
        # 5. Consolidação Funcional (Etapa 5)
        logger.info("\n5️⃣ Etapa 5: Consolidação funcional...")
        try:
            changes_5 = manager.step_5_consolidate_functional()
            logger.info(f"✅ {changes_5} consolidações funcionais realizadas!")
        except Exception as e:
            logger.warning(f"⚠️ Erro na consolidação funcional: {e}")
            changes_5 = 0
        
        # 6. Limpeza Territorial (Etapa 7)
        logger.info("\n6️⃣ Etapa 7: Limpeza territorial...")
        try:
            changes_7 = manager.step_7_territorial_cleanup()
            logger.info(f"✅ {changes_7} limpezas territoriais realizadas!")
        except Exception as e:
            logger.warning(f"⚠️ Erro na limpeza territorial: {e}")
            changes_7 = 0
        
        total_changes = changes_5 + changes_7
        
        # 7. Obter consolidation_manager do consolidator
        logger.info(f"\n7️⃣ Salvando {total_changes} consolidações em cache...")
        
        # O consolidation_manager já está preenchido pelas chamadas ao consolidator
        consolidation_manager = manager.consolidator.consolidation_manager
        
        # 8. Atualizar cache do loader
        logger.info("\n8️⃣ Atualizando cache de consolidação...")
        consolidation_loader = ConsolidationLoader()
        consolidation_loader.update_from_log(consolidation_manager.log_data)
        logger.info("✅ Cache atualizado!")
        
        # 9. Executar análise de dependências e cachear
        logger.info("\n9️⃣ Executando análise de dependências...")
        try:
            from src.pipeline.sede_analyzer import SedeAnalyzer
            from pathlib import Path
            
            # Criar analisador com dados consolidados
            analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
            
            # Executar análise
            sede_summary = analyzer.analyze_sede_dependencies()
            
            if sede_summary.get('success'):
                # Exportar para JSON
                cache_path = Path(project_root) / "data" / "sede_analysis_cache.json"
                if analyzer.export_to_json(cache_path):
                    logger.info(f"✅ Análise de dependências salva em: {cache_path}")
                    logger.info(f"   📊 {sede_summary['total_sedes']} sedes, {sede_summary['total_alertas']} alertas")
                else:
                    logger.warning("⚠️ Falha ao salvar análise de dependências")
            else:
                logger.warning(f"⚠️ Análise de dependências falhou: {sede_summary.get('error', 'Erro desconhecido')}")
        
        except Exception as e:
            logger.warning(f"⚠️ Erro ao executar análise de dependências: {e}")
        
        # 10. Exibir resumo
        logger.info("\n" + "=" * 80)
        logger.info("RESUMO DA EXECUÇÃO")
        logger.info("=" * 80)
        
        summary = consolidation_loader.get_summary()
        logger.info(f"Status: {summary['status']}")
        logger.info(f"Total de consolidações: {summary['total_consolidations']}")
        logger.info(f"UTPs de origem: {summary['unique_sources']}")
        logger.info(f"UTPs de destino: {summary['unique_targets']}")
        logger.info(f"Último update: {summary['timestamp']}")
        
        logger.info("\n✅ CONSOLIDAÇÃO CONCLUÍDA COM SUCESSO!")
        logger.info(f"📁 Arquivos gerados:")
        logger.info(f"   - data/consolidation_log.json (Log detalhado)")
        logger.info(f"   - data/consolidation_result.json (Cache rápido)")
        logger.info("\n💡 Dica: Recarregue o Streamlit para ver os resultados!")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ ERRO DURANTE A EXECUÇÃO: {type(e).__name__}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = run_consolidation()
    sys.exit(0 if success else 1)
