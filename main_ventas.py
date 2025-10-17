"""
Script principal para procesar ventas desde SQL Server
Reemplaza PreproVenta.py (que leía Excel)
"""
import argparse
import logging
from pathlib import Path
from typing import Optional
import sys

import pandas as pd

from config.database import DatabaseConfig
from db.connection import DatabaseConnection
from db.queries import VentasQuery
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


def exportar_xlsx(df: pd.DataFrame, 
                  out_path: Path,
                  add_resumen: bool = True,
                  fecha_col: str = "Fecha",
                  valor_col: str = "Valor neto") -> None:
    """
    Exporta DataFrame a Excel con formato
    
    Args:
        df: DataFrame a exportar
        out_path: Ruta del archivo de salida
        add_resumen: Si True, agrega hoja "Resumen" con métricas
        fecha_col: Columna de fecha para formato
        valor_col: Columna de valor para sumar
    """
    logger.info(f"Exportando a {out_path}")
    
    with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
        # Hoja principal
        df.to_excel(writer, index=False, sheet_name="Datos")
        
        # Formato
        wb = writer.book
        ws = writer.sheets["Datos"]
        fmt_date = wb.add_format({"num_format": "yyyy-mm-dd"})
        fmt_int = wb.add_format({"num_format": "0"})
        
        cols = {c: i for i, c in enumerate(df.columns)}
        if fecha_col in cols:
            ws.set_column(cols[fecha_col], cols[fecha_col], None, fmt_date)
        if valor_col in cols:
            ws.set_column(cols[valor_col], cols[valor_col], None, fmt_int)
        
        # Hoja resumen
        if add_resumen:
            resumen = pd.DataFrame({
                "Métrica": ["Filas", f"Suma {valor_col}"],
                "Valor": [
                    len(df), 
                    df[valor_col].fillna(0).sum() if valor_col in df.columns else 0
                ]
            })
            resumen.to_excel(writer, index=False, sheet_name="Resumen")
    
    logger.info(f"✓ Exportado: {len(df):,} filas")


def main():
    parser = argparse.ArgumentParser(
        description="Procesar ventas desde SQL Server (reemplaza PreproVenta.py)"
    )
    
    # Fuente de datos
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--sql",
        action="store_true",
        help="Leer desde SQL Server (vista MP_VENTAS_CODE)"
    )
    source_group.add_argument(
        "--excel",
        dest="excel_path",
        help="[Legacy] Leer desde Excel (compatibilidad con flujo antiguo)"
    )
    
    # Configuración SQL
    parser.add_argument(
        "--meses",
        type=int,
        default=2,
        help="Meses de datos a cargar (default: 2)"
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Archivo .env con credenciales SQL (default: .env)"
    )
    
    # Configuración Excel (legacy)
    parser.add_argument(
        "--sheet",
        default="Sheet1",
        help="[Excel] Hoja a leer (default: Sheet1)"
    )
    
    # Filtro opcional
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
    
    # Salida
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("Ventas_procesadas_fmt.xlsx"),
        help="Archivo de salida (default: Ventas_procesadas_fmt.xlsx)"
    )
    parser.add_argument(
        "--no-resumen",
        action="store_true",
        help="No generar hoja de resumen"
    )
    
    # Opciones
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
        # === CARGA DE DATOS ===
        if args.sql:
            logger.info("=== CARGANDO DESDE SQL SERVER ===")
            
            # Configurar conexión
            if not args.env_file.exists():
                logger.warning(f"Archivo {args.env_file} no encontrado, usando variables de entorno")
            
            db_config = DatabaseConfig.from_env(args.env_file)
            logger.info(f"Conexión: {db_config}")
            
            # Conectar y ejecutar query
            db_conn = DatabaseConnection(db_config.connection_string())
            query = VentasQuery.get_ventas_ultimos_n_meses(args.meses)
            
            df_raw = db_conn.execute_query(query)
            db_conn.close()
            
            logger.info(f"Cargadas {len(df_raw):,} filas desde SQL")
            
        else:  # Excel (legacy)
            logger.info("=== CARGANDO DESDE EXCEL (modo legacy) ===")
            
            excel_path = Path(args.excel_path)
            if not excel_path.exists():
                logger.error(f"Archivo no encontrado: {excel_path}")
                sys.exit(1)
            
            logger.info(f"Leyendo {excel_path}")
            df_raw = pd.read_excel(excel_path, sheet_name=args.sheet, engine="openpyxl")
            logger.info(f"Cargadas {len(df_raw):,} filas desde Excel")
        
        # === PROCESAMIENTO ===
        logger.info("=== PROCESANDO DATOS ===")
        
        processor = VentasProcessor(debug=args.debug)
        df_processed = processor.process(df_raw)
        
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
            
            df_processed = processor.filter_by_selection(df_processed, selection_df)
        
        # === EXPORTACIÓN ===
        logger.info("=== EXPORTANDO RESULTADO ===")
        
        out_path = args.out.resolve()
        exportar_xlsx(
            df_processed, 
            out_path,
            add_resumen=not args.no_resumen
        )
        
        # === RESUMEN FINAL ===
        logger.info("=== RESUMEN ===")
        logger.info(f"Filas procesadas: {len(df_processed):,}")
        logger.info(f"Columnas: {len(df_processed.columns)}")
        logger.info(f"Rango de fechas: {df_processed['Fecha'].min()} a {df_processed['Fecha'].max()}")
        logger.info(f"Referencias únicas: {df_processed['Referencia'].nunique():,}")
        logger.info(f"SKUs únicos: {df_processed['SKU'].nunique():,}")
        logger.info(f"Valor total: ${df_processed['Valor neto'].sum():,.0f}")
        logger.info(f"\n✓ Archivo generado: {out_path}")
        
        # Verificación crítica: asegurar que no hay padding
        if args.debug:
            logger.debug("\n=== VERIFICACIÓN DE LIMPIEZA ===")
            for col in ['Bodega', 'Referencia', 'Talla']:
                if col in df_processed.columns:
                    sample = df_processed[col].iloc[0] if len(df_processed) > 0 else None
                    has_spaces = sample.startswith(' ') or sample.endswith(' ') if sample else False
                    status = "❌ TIENE ESPACIOS" if has_spaces else "✓ limpio"
                    logger.debug(f"{col}: '{sample}' → {status}")
        
    except Exception as e:
        logger.error(f"ERROR: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
