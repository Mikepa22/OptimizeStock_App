"""
Script principal para procesar stock/inventario desde SQL Server
Reemplaza PreproStock.py (que leía Excel)

IMPORTANTE: Requiere DataFrame de ventas procesadas para filtrar referencias
"""
import argparse
import logging
from pathlib import Path
from typing import Optional
import sys

import pandas as pd

from config.database import DatabaseConfig
from db.connection import DatabaseConnection
from db.queries import StockQuery, VentasQuery
from processors.stock_processor import StockProcessor
from processors.ventas_processor import VentasProcessor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def cargar_ventas_procesadas(args, db_conn: Optional[DatabaseConnection] = None) -> pd.DataFrame:
    """
    Carga DataFrame de ventas procesadas.
    Puede cargarlo desde archivo Excel o procesarlo desde SQL.
    
    Args:
        args: Argumentos del CLI
        db_conn: Conexión a BD (opcional, para procesar desde SQL)
    
    Returns:
        DataFrame de ventas procesadas
    """
    # Opción 1: Desde archivo Excel ya procesado
    if args.ventas_procesadas:
        ventas_path = Path(args.ventas_procesadas)
        if not ventas_path.exists():
            logger.error(f"Archivo de ventas no encontrado: {ventas_path}")
            sys.exit(1)
        
        logger.info(f"Cargando ventas procesadas desde {ventas_path}")
        ventas_df = pd.read_excel(
            ventas_path, 
            sheet_name=args.ventas_sheet,
            engine="openpyxl"
        )
        if 'Referencia' in ventas_df.columns:
            ventas_df['Referencia'] = ventas_df['Referencia'].astype('string').str.strip()
            
        logger.info(f"Cargadas {len(ventas_df):,} filas de ventas")
        return ventas_df
    
    # Opción 2: Procesar desde SQL internamente
    elif args.procesar_ventas_sql:
        if db_conn is None:
            logger.error("Se requiere conexión a BD para procesar ventas desde SQL")
            sys.exit(1)
        
        logger.info("Procesando ventas desde SQL (modo integrado)")
        
        # Ejecutar query de ventas
        query = VentasQuery.get_ventas_ultimos_n_meses(args.meses_ventas)
        ventas_raw = db_conn.execute_query(query)
        logger.info(f"Cargadas {len(ventas_raw):,} filas crudas de ventas")
        
        # Procesar ventas
        ventas_processor = VentasProcessor(debug=args.debug)
        ventas_df = ventas_processor.process(ventas_raw)
        logger.info(f"Procesadas {len(ventas_df):,} filas de ventas")
        
        return ventas_df
    
    else:
        logger.error("Debe proporcionar --ventas-procesadas o --procesar-ventas-sql")
        sys.exit(1)


def exportar_xlsx(df: pd.DataFrame, out_path: Path) -> None:
    """
    Exporta DataFrame a Excel
    
    Args:
        df: DataFrame a exportar
        out_path: Ruta del archivo de salida
    """
    logger.info(f"Exportando a {out_path}")
    df.to_excel(out_path, index=False, engine='xlsxwriter')
    logger.info(f"✓ Exportado: {len(df):,} filas")


def main():
    parser = argparse.ArgumentParser(
        description="Procesar stock desde SQL Server (reemplaza PreproStock.py)"
    )
    
    # === FUENTE DE DATOS: STOCK ===
    stock_group = parser.add_mutually_exclusive_group(required=True)
    stock_group.add_argument(
        "--sql",
        action="store_true",
        help="Leer stock desde SQL Server (vista MP_T400)"
    )
    stock_group.add_argument(
        "--excel",
        dest="stock_excel_path",
        help="[Legacy] Leer stock desde Excel"
    )
    
    # === FUENTE DE DATOS: VENTAS (REQUERIDO) ===
    ventas_group = parser.add_mutually_exclusive_group(required=True)
    ventas_group.add_argument(
        "--ventas-procesadas",
        type=Path,
        help="Excel de ventas YA procesadas (salida de main_ventas.py)"
    )
    ventas_group.add_argument(
        "--procesar-ventas-sql",
        action="store_true",
        help="Procesar ventas desde SQL internamente (requiere --sql en stock)"
    )
    
    # === CONFIGURACIÓN SQL ===
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Archivo .env con credenciales SQL (default: .env)"
    )
    parser.add_argument(
        "--meses-ventas",
        type=int,
        default=2,
        help="Meses de ventas a cargar si --procesar-ventas-sql (default: 2)"
    )
    
    # === CONFIGURACIÓN EXCEL (LEGACY) ===
    parser.add_argument(
        "--stock-sheet",
        default="Sheet1",
        help="[Excel Stock] Hoja a leer (default: Sheet1)"
    )
    parser.add_argument(
        "--ventas-sheet",
        default="Datos",
        help="Hoja de ventas procesadas (default: Datos)"
    )
    
    # === FILTRO OPCIONAL ===
    parser.add_argument(
        "--seleccion",
        type=Path,
        help="(Opcional) Excel con referencias a filtrar"
    )
    parser.add_argument(
        "--seleccion-sheet",
        default=None,
        help="Hoja del archivo de selección"
    )
    
    # === SALIDA ===
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("Stock_procesado.xlsx"),
        help="Archivo de salida (default: Stock_procesado.xlsx)"
    )
    
    # === OPCIONES ===
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Activar modo debug con logging detallado"
    )
    
    args = parser.parse_args()
    
    # Configurar nivel de log
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Modo DEBUG activado")
    
    try:
        # === CONFIGURAR CONEXIÓN SQL (SI ES NECESARIO) ===
        db_conn = None
        if args.sql or args.procesar_ventas_sql:
            logger.info("=== CONFIGURANDO CONEXIÓN SQL ===")
            
            if not args.env_file.exists():
                logger.warning(f"Archivo {args.env_file} no encontrado, "
                             "usando variables de entorno")
            
            db_config = DatabaseConfig.from_env(args.env_file)
            logger.info(f"Conexión: {db_config}")
            
            db_conn = DatabaseConnection(db_config.connection_string())
        
        # === CARGAR VENTAS PROCESADAS ===
        logger.info("=== CARGANDO VENTAS PROCESADAS ===")
        ventas_df = cargar_ventas_procesadas(args, db_conn)
        
        # === CARGAR STOCK ===
        if args.sql:
            logger.info("=== CARGANDO STOCK DESDE SQL SERVER ===")
            
            query = StockQuery.get_stock_actual()
            stock_raw = db_conn.execute_query(query)
            logger.info(f"Cargadas {len(stock_raw):,} filas de stock")
            
        else:  # Excel (legacy)
            logger.info("=== CARGANDO STOCK DESDE EXCEL (modo legacy) ===")
            
            stock_path = Path(args.stock_excel_path)
            if not stock_path.exists():
                logger.error(f"Archivo no encontrado: {stock_path}")
                sys.exit(1)
            
            logger.info(f"Leyendo {stock_path}")
            stock_raw = pd.read_excel(
                stock_path, 
                sheet_name=args.stock_sheet,
                engine="openpyxl"
            )
            logger.info(f"Cargadas {len(stock_raw):,} filas desde Excel")
        
        # Cerrar conexión si se usó
        if db_conn:
            db_conn.close()
        
        # === PROCESAMIENTO ===
        logger.info("=== PROCESANDO STOCK ===")
        
        processor = StockProcessor(debug=args.debug)
        stock_processed = processor.process(stock_raw, ventas_df)
        
        # === FILTRO OPCIONAL POR SELECCIÓN ===
        if args.seleccion:
            logger.info("=== APLICANDO FILTRO DE SELECCIÓN ===")
            
            if not args.seleccion.exists():
                logger.error(f"Archivo de selección no encontrado: {args.seleccion}")
                sys.exit(1)
            
            selection_df = pd.read_excel(
                args.seleccion, 
                sheet_name=args.seleccion_sheet or 0,
                engine="openpyxl"
            )
            
            stock_processed = processor.filter_by_selection(
                stock_processed, 
                selection_df
            )
        
        # === EXPORTACIÓN ===
        logger.info("=== EXPORTANDO RESULTADO ===")
        
        out_path = args.out.resolve()
        exportar_xlsx(stock_processed, out_path)
        
        # === RESUMEN FINAL ===
        logger.info("=== RESUMEN ===")
        logger.info(f"Filas procesadas: {len(stock_processed):,}")
        logger.info(f"Columnas: {len(stock_processed.columns)}")
        logger.info(f"Referencias únicas: {stock_processed['Referencia'].nunique():,}")
        logger.info(f"SKUs únicos: {stock_processed['SKU'].nunique():,}")
        logger.info(f"Tiendas: {stock_processed['Tienda'].nunique()}")
        logger.info(f"Existencia total: {stock_processed['Existencia'].sum():,} unidades")
        logger.info(f"\n✓ Archivo generado: {out_path}")
        
        # Verificación crítica: asegurar que no hay padding
        if args.debug:
            logger.debug("\n=== VERIFICACIÓN DE LIMPIEZA ===")
            for col in ['Referencia', 'Talla', 'Tienda']:
                if col in stock_processed.columns:
                    sample = stock_processed[col].iloc[0] if len(stock_processed) > 0 else None
                    has_spaces = (sample.startswith(' ') or sample.endswith(' ')) if sample else False
                    status = "❌ TIENE ESPACIOS" if has_spaces else "✓ limpio"
                    logger.debug(f"{col}: '{sample}' → {status}")
        
    except Exception as e:
        logger.error(f"ERROR: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
