"""
Sistema de Traslados - Pipeline Completo en Memoria
Versión 2.0 - Sin archivos intermedios

Flujo:
1. Extrae datos de SQL (ventas + stock)
2. Procesa y limpia datos en memoria
3. Ejecuta 3 fases de traslados (necesidades + curvas + drenaje)
4. Genera Excel de salida

Uso:
    python main.py --meses 2
    python main.py --meses 3 --debug
    python main.py --meses 2 --seleccion Referencias.xlsx
"""
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

# Imports del proyecto
from config import DatabaseConfig
from processors.ventas_processor import VentasProcessor
from processors.stock_processor import StockProcessor
from traslados.orchestrator import TrasladosOrchestrator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('traslados.log')
    ]
)
logger = logging.getLogger(__name__)


class TrasladosPipeline:
    """
    Pipeline completo de traslados en memoria
    
    Integra:
    - Extracción de SQL
    - ETL de ventas y stock
    - Cálculo de traslados (3 fases)
    - Generación de salida
    """
    
    def __init__(
        self,
        db_config: DatabaseConfig,
        bodega_principal: str = 'BODEGA PRINCIPAL',
        no_seed: bool = True,
        allow_seed_if_adu: bool = True,
        debug: bool = False,
        save_intermediates: bool = False
    ):
        """
        Inicializar pipeline
        
        Args:
            db_config: Configuración de base de datos
            bodega_principal: Nombre de la bodega principal
            no_seed: Bloquear siembra de referencias nuevas
            allow_seed_if_adu: Permitir siembra si SKU tiene ADU > 0
            debug: Modo debug (más logs)
            save_intermediates: Guardar Excel intermedios para auditoría
        """
        self.db_config = db_config
        self.bodega_principal = bodega_principal
        self.no_seed = no_seed
        self.allow_seed_if_adu = allow_seed_if_adu
        self.debug = debug
        self.save_intermediates = save_intermediates
        
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.debug("Modo DEBUG activado")
    
    def run(
        self,
        meses_ventas: int = 2,
        seleccion_path: Path = None,
        output_path: Path = Path("Traslados_final.xlsx"),
        dias_min: int = 7,
        dias_max: int = 14,
        safety_ratio: float = 0.2
    ) -> pd.DataFrame:
        """
        Ejecutar pipeline completo
        
        Args:
            meses_ventas: Meses de ventas a extraer
            seleccion_path: (Opcional) Excel con referencias a filtrar
            output_path: Path del archivo de salida
            dias_min: Días mínimos de cobertura objetivo
            dias_max: Días máximos de cobertura objetivo
            safety_ratio: Ratio de seguridad para drenaje (0.0-1.0)
        
        Returns:
            DataFrame con traslados generados
        """
        logger.info("="*80)
        logger.info("🚀 INICIANDO PIPELINE DE TRASLADOS v2.0")
        logger.info("="*80)
        logger.info(f"Parámetros:")