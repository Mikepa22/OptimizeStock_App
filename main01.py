"""
Sistema de Traslados - Pipeline Completo en Memoria
Version 2.0 - Sin archivos intermedios

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
from db.connection import DatabaseConnection
from db.queries import VentasQuery, StockQuery
from processors.ventas_processor import VentasProcessor
from processors.stock_processor import StockProcessor
from traslados.orchestrator import TrasladosOrchestrator

# Configurar logging sin emojis (compatibilidad Windows)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('traslados.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class TrasladosPipeline:
    """
    Pipeline completo de traslados en memoria
    
    Integra:
    - Extraccion de SQL
    - ETL de ventas y stock
    - Calculo de traslados (3 fases)
    - Generacion de salida
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
            db_config: Configuracion de base de datos
            bodega_principal: Nombre de la bodega principal
            no_seed: Bloquear siembra de referencias nuevas
            allow_seed_if_adu: Permitir siembra si SKU tiene ADU > 0
            debug: Modo debug (mas logs)
            save_intermediates: Guardar Excel intermedios para auditoria
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
    ) -> tuple:
        """
        Ejecutar pipeline completo
        
        Args:
            meses_ventas: Meses de ventas a extraer
            seleccion_path: (Opcional) Excel con referencias a filtrar
            output_path: Path del archivo de salida
            dias_min: Dias minimos de cobertura objetivo
            dias_max: Dias maximos de cobertura objetivo
            safety_ratio: Ratio de seguridad para drenaje (0.0-1.0)
        
        Returns:
            Tupla (df_traslados, df_stock_final)
        """
        logger.info("="*80)
        logger.info("INICIANDO PIPELINE DE TRASLADOS v2.0")
        logger.info("="*80)
        logger.info(f"Parametros:")
        logger.info(f"  - Meses de ventas: {meses_ventas}")
        logger.info(f"  - Dias cobertura: {dias_min}-{dias_max}")
        logger.info(f"  - Safety ratio: {safety_ratio}")
        logger.info(f"  - Bodega principal: {self.bodega_principal}")
        logger.info(f"  - No seed: {self.no_seed}")
        logger.info(f"  - Allow seed if ADU: {self.allow_seed_if_adu}")
        
        try:
            # PASO 1: Extraer datos de SQL
            logger.info("\nPASO 1/4: Extrayendo datos de SQL Server...")
            df_ventas_raw, df_stock_raw = self._extract_from_sql(meses_ventas)
            logger.info(f"  > Ventas extraidas: {len(df_ventas_raw):,} registros")
            logger.info(f"  > Stock extraido: {len(df_stock_raw):,} registros")
            
            # PASO 2: Procesar y limpiar datos
            logger.info("\nPASO 2/4: Procesando y limpiando datos...")
            df_ventas, df_stock = self._process_data(
                df_ventas_raw, 
                df_stock_raw,
                seleccion_path
            )
            logger.info(f"  > Ventas procesadas: {len(df_ventas):,} registros")
            logger.info(f"  > Stock procesado: {len(df_stock):,} registros")
            logger.info(f"  > Inventario total: {df_stock['Existencia'].sum():,} unidades")
            
            if self.save_intermediates:
                df_ventas.to_excel("_intermediate_ventas.xlsx", index=False)
                df_stock.to_excel("_intermediate_stock.xlsx", index=False)
                logger.debug("  > Guardados: _intermediate_ventas.xlsx, _intermediate_stock.xlsx")
            
            # PASO 3: Calcular TRASLADOS (3 fases)
            logger.info("\nPASO 3/4: Calculando TRASLADOS...")
            df_traslados, df_stock_final = self._calculate_transfers(
                df_ventas=df_ventas,
                df_stock=df_stock,
                dias_min=dias_min,
                dias_max=dias_max,
                safety_ratio=safety_ratio
            )
            
            if df_traslados.empty:
                logger.warning("  ! No se generaron traslados")
                return df_traslados, df_stock_final
            
            logger.info(f"  > Traslados generados: {len(df_traslados):,} lineas")
            logger.info(f"  > Total unidades: {df_traslados['Unidades a trasladar'].sum():,}")
            
            # Resumen por fase
            resumen_fases = df_traslados.groupby('Fase')['Unidades a trasladar'].agg(['count', 'sum'])
            logger.info("\n  Resumen por fase:")
            for fase, row in resumen_fases.iterrows():
                logger.info(f"    {fase:30s}: {int(row['count']):4d} lineas, {int(row['sum']):6,} unidades")
            
            # PASO 4: Guardar resultado
            logger.info(f"\nPASO 4/4: Guardando resultado en {output_path}...")
            self._save_output(df_traslados, df_stock_final, output_path)
            logger.info(f"  > Archivo generado exitosamente")
            
            # Resumen final
            logger.info("\n" + "="*80)
            logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
            logger.info("="*80)
            logger.info(f"Archivo de salida: {output_path.absolute()}")
            logger.info(f"Total traslados: {len(df_traslados):,} lineas")
            logger.info(f"Total unidades: {df_traslados['Unidades a trasladar'].sum():,}")
            
            return df_traslados, df_stock_final
            
        except Exception as e:
            logger.error(f"\nERROR EN PIPELINE: {e}", exc_info=True)
            raise
    
    def _extract_from_sql(
        self, 
        meses: int
    ) -> tuple:
        """
        Extrae datos crudos de SQL Server
        
        Args:
            meses: Numero de meses a extraer
        
        Returns:
            Tupla (df_ventas_raw, df_stock_raw)
        """
        logger.info("  > Conectando a SQL Server...")
        
        with DatabaseConnection(self.db_config.connection_string()) as db_conn:
            # Extraer ventas
            logger.info(f"  > Extrayendo ventas de ultimos {meses} meses...")
            query_ventas = VentasQuery.get_ventas_ultimos_n_meses(meses)
            df_ventas_raw = db_conn.execute_query(query_ventas)
            
            # Extraer stock
            logger.info("  > Extrayendo stock actual...")
            query_stock = StockQuery.get_stock_actual()
            df_stock_raw = db_conn.execute_query(query_stock)
        
        return df_ventas_raw, df_stock_raw
    
    def _process_data(
        self,
        df_ventas_raw: pd.DataFrame,
        df_stock_raw: pd.DataFrame,
        seleccion_path: Path = None
    ) -> tuple:
        """
        Procesa y limpia datos usando los processors
        
        Args:
            df_ventas_raw: DataFrame crudo de ventas
            df_stock_raw: DataFrame crudo de stock
            seleccion_path: (Opcional) Excel con referencias a filtrar
        
        Returns:
            Tupla (df_ventas_clean, df_stock_clean)
        """
        # Procesar ventas
        logger.info("  > Procesando ventas...")
        ventas_processor = VentasProcessor(debug=self.debug)
        df_ventas = ventas_processor.process(df_ventas_raw)
        
        # Procesar stock (requiere ventas procesadas)
        logger.info("  > Procesando stock...")
        stock_processor = StockProcessor(debug=self.debug)
        df_stock = stock_processor.process(df_stock_raw, df_ventas)
        
        # Aplicar filtro de seleccion si existe
        if seleccion_path and seleccion_path.exists():
            logger.info(f"  > Aplicando filtro de seleccion: {seleccion_path}")
            df_seleccion = pd.read_excel(seleccion_path)
            
            # Filtrar ventas
            if 'Referencia' in df_seleccion.columns:
                refs_seleccion = df_seleccion['Referencia'].dropna().unique()
                df_ventas = df_ventas[df_ventas['Referencia'].isin(refs_seleccion)].copy()
                df_stock = df_stock[df_stock['Referencia'].isin(refs_seleccion)].copy()
                logger.info(f"    - Filtradas {len(refs_seleccion):,} referencias")
        
        return df_ventas, df_stock
    
    def _calculate_transfers(
        self,
        df_ventas: pd.DataFrame,
        df_stock: pd.DataFrame,
        dias_min: int,
        dias_max: int,
        safety_ratio: float
    ) -> tuple:
        """
        Calcula traslados usando el orchestrator
        
        Args:
            df_ventas: Ventas procesadas
            df_stock: Stock procesado
            dias_min: Dias minimos de cobertura
            dias_max: Dias maximos de cobertura
            safety_ratio: Ratio de seguridad para drenaje
        
        Returns:
            Tupla (df_traslados, df_stock_final)
        """
        # Crear orchestrator
        logger.info("  > Inicializando motor de traslados...")
        orchestrator = TrasladosOrchestrator(
            df_ventas=df_ventas,
            df_stock=df_stock,
            bodega_principal=self.bodega_principal,
            no_seed=self.no_seed,
            allow_seed_if_adu=self.allow_seed_if_adu,
            dias_min=dias_min,
            dias_max=dias_max,
            debug=self.debug
        )
        
        # Ejecutar las 3 fases
        logger.info("\n  > Ejecutando Fase 1: Necesidades base...")
        df_fase1 = orchestrator.run_fase1_necesidades_base()
        if not df_fase1.empty:
            logger.info(f"    > Fase 1: {len(df_fase1):,} lineas, {df_fase1['Unidades a trasladar'].sum():,} unidades")
        else:
            logger.info(f"    - Fase 1: Sin traslados necesarios")
        
        logger.info("\n  > Ejecutando Fase 2: Completar curvas...")
        df_fase2 = orchestrator.run_fase2_completar_curvas()
        if not df_fase2.empty:
            logger.info(f"    > Fase 2: {len(df_fase2):,} lineas, {df_fase2['Unidades a trasladar'].sum():,} unidades")
        else:
            logger.info(f"    - Fase 2: Curvas ya completas")
        
        logger.info(f"\n  > Ejecutando Fase 3: Drenar bodega (safety={safety_ratio})...")
        df_fase3 = orchestrator.run_fase3_drenar_bodega(safety_ratio=safety_ratio)
        if not df_fase3.empty:
            logger.info(f"    > Fase 3: {len(df_fase3):,} lineas, {df_fase3['Unidades a trasladar'].sum():,} unidades")
        else:
            logger.info(f"    - Fase 3: Bodega ya optimizada")
        
        # Consolidar resultado
        df_traslados = orchestrator.get_all_transfers()
        df_stock_final = orchestrator.df_stock.copy()
        
        return df_traslados, df_stock_final
    
    def _save_output(
        self, 
        df_traslados: pd.DataFrame,
        df_stock_final: pd.DataFrame,
        output_path: Path
    ):
        """
        Guarda el resultado en Excel con formato
        
        Args:
            df_traslados: DataFrame con traslados sugeridos
            df_stock_final: DataFrame con stock final despues de traslados
            output_path: Path del archivo de salida
        """
        # Ordenar traslados por fase y cantidad
        df_traslados = df_traslados.sort_values(
            by=['Fase', 'Tienda destino', 'Unidades a trasladar'],
            ascending=[True, True, False]
        )
        
        # Ordenar stock por tienda y referencia
        df_stock_final = df_stock_final.sort_values(
            by=['Tienda', 'Referencia', 'Talla'],
            ascending=[True, True, True]
        )
        
        # Guardar archivo principal con 2 hojas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Hoja 1: Traslados sugeridos
            df_traslados.to_excel(writer, sheet_name='Traslados', index=False)
            
            # Hoja 2: Stock final despues de traslados
            df_stock_final.to_excel(writer, sheet_name='Stock Final', index=False)
        
        logger.info(f"  > Archivo principal guardado (2 hojas: Traslados + Stock Final)")
        
        # Generar resumen adicional (opcional)
        resumen_path = output_path.parent / f"{output_path.stem}_resumen.xlsx"
        
        with pd.ExcelWriter(resumen_path, engine='openpyxl') as writer:
            # Hoja 1: Resumen por tienda destino
            resumen_tienda = df_traslados.groupby('Tienda destino').agg({
                'Unidades a trasladar': 'sum',
                'Referencia': 'nunique'
            }).reset_index()
            resumen_tienda.columns = ['Tienda', 'Total Unidades', 'Referencias Unicas']
            resumen_tienda = resumen_tienda.sort_values('Total Unidades', ascending=False)
            resumen_tienda.to_excel(writer, sheet_name='Por Tienda', index=False)
            
            # Hoja 2: Resumen por fase
            resumen_fase = df_traslados.groupby('Fase').agg({
                'Unidades a trasladar': 'sum',
                'Tienda destino': 'nunique'
            }).reset_index()
            resumen_fase.columns = ['Fase', 'Total Unidades', 'Tiendas Destino']
            resumen_fase.to_excel(writer, sheet_name='Por Fase', index=False)
            
            # Hoja 3: Top SKUs transferidos
            top_items = df_traslados.groupby(['Referencia', 'Talla']).agg({
                'Unidades a trasladar': 'sum'
            }).reset_index().sort_values('Unidades a trasladar', ascending=False).head(50)
            top_items.to_excel(writer, sheet_name='Top 50 Items', index=False)
            
            # Hoja 4: Stock por tienda (resumen)
            stock_resumen = df_stock_final.groupby('Tienda').agg({
                'Existencia': 'sum',
                'Referencia': 'nunique'
            }).reset_index()
            stock_resumen.columns = ['Tienda', 'Total Unidades', 'Referencias Unicas']
            stock_resumen = stock_resumen.sort_values('Total Unidades', ascending=False)
            stock_resumen.to_excel(writer, sheet_name='Stock por Tienda', index=False)
        
        logger.info(f"  > Resumen adicional: {resumen_path}")


def main():
    """
    Funcion principal del pipeline
    """
    parser = argparse.ArgumentParser(
        description="Sistema de Traslados - Pipeline Completo en Memoria",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python main.py --meses 2
  python main.py --meses 3 --debug
  python main.py --meses 2 --seleccion Referencias_Agotadas.xlsx
  python main.py --meses 2 --safety-ratio 0.1  # Drenar mas agresivamente
        """
    )
    
    # Configuracion SQL
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Archivo .env con credenciales SQL (default: .env)"
    )
    
    parser.add_argument(
        "--meses",
        type=int,
        default=2,
        help="Meses de ventas a procesar (default: 2)"
    )
    
    # Filtro opcional
    parser.add_argument(
        "--seleccion",
        type=Path,
        help="(Opcional) Excel con referencias a filtrar"
    )
    
    # Salida
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("Traslados_final.xlsx"),
        help="Archivo de salida (default: Traslados_final.xlsx)"
    )
    
    # Parametros del algoritmo
    parser.add_argument(
        "--dias-min",
        type=int,
        default=7,
        help="Dias minimos de cobertura objetivo (default: 7)"
    )
    
    parser.add_argument(
        "--dias-max",
        type=int,
        default=14,
        help="Dias maximos de cobertura objetivo (default: 14)"
    )
    
    parser.add_argument(
        "--safety-ratio",
        type=float,
        default=0.2,
        help="Ratio de seguridad para drenaje de bodega (default: 0.2)"
    )
    
    # Politica de siembra
    parser.add_argument(
        "--allow-seed",
        action="store_true",
        help="Permitir siembra de referencias nuevas"
    )
    
    # Opciones de debugging
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Activar modo debug"
    )
    
    parser.add_argument(
        "--save-intermediates",
        action="store_true",
        help="Guardar Excel intermedios (para auditoria)"
    )
    
    args = parser.parse_args()
    
    # Cargar configuracion de BD
    if not args.env_file.exists():
        logger.warning(f"Archivo {args.env_file} no encontrado, usando variables de entorno")
    
    db_config = DatabaseConfig.from_env(args.env_file)
    
    # Crear y ejecutar pipeline
    pipeline = TrasladosPipeline(
        db_config=db_config,
        bodega_principal='BODEGA PRINCIPAL',
        no_seed=not args.allow_seed,  # Invertir logica
        allow_seed_if_adu=True,  # Siempre permitir si hay ADU
        debug=args.debug,
        save_intermediates=args.save_intermediates
    )
    
    # Ejecutar
    start_time = datetime.now()
    
    df_traslados, df_stock_final = pipeline.run(
        meses_ventas=args.meses,
        seleccion_path=args.seleccion,
        output_path=args.out,
        dias_min=args.dias_min,
        dias_max=args.dias_max,
        safety_ratio=args.safety_ratio
    )
    
    elapsed = datetime.now() - start_time
    logger.info(f"\nTiempo total: {elapsed.total_seconds():.1f} segundos")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())