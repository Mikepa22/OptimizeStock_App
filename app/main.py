"""
FastAPI Web Application para Generador de Traslados Cielito
"""
import sys
import asyncio
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime
import tempfile
import shutil

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
import pandas as pd

# Agregar el directorio raíz al path para importar los módulos
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DatabaseConfig
from processors import VentasProcessor, StockProcessor
from db import DatabaseConnection, VentasQuery, StockQuery
from traslados.orchestrator import TrasladosOrchestrator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title="Generador de Traslados Cielito",
    description="Sistema automatizado de generación de traslados de inventario",
    version="2.0"
)

# Configurar directorios
BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"

# Crear directorio de salida si no existe
OUTPUT_DIR.mkdir(exist_ok=True)

# Montar archivos estáticos
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Configurar templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Estado de ejecución (en producción usar Redis o similar)
execution_state = {
    "running": False,
    "progress": 0,
    "stage": "",
    "error": None,
    "output_files": []
}


class TrasladosRequest(BaseModel):
    """Modelo de request para generar traslados"""
    meses: int = 2
    debug: bool = False
    save_intermediates: bool = False
    dias_min: int = 7
    dias_max: int = 14
    safety_ratio: float = 0.3
    allow_seed: bool = False


def update_progress(progress: int, stage: str):
    """Actualiza el estado de progreso"""
    execution_state["progress"] = progress
    execution_state["stage"] = stage
    logger.info(f"Progreso: {progress}% - {stage}")


async def run_traslados_pipeline(params: TrasladosRequest) -> dict:
    """
    Ejecuta el pipeline completo de traslados
    """
    try:
        execution_state["running"] = True
        execution_state["error"] = None
        execution_state["output_files"] = []
        
        # ETAPA 1: Configuración (0-10%)
        update_progress(5, "Cargando configuración de base de datos...")
        db_config = DatabaseConfig.from_env()
        
        # ETAPA 2: Extracción de datos (10-40%)
        update_progress(10, f"Extrayendo ventas de últimos {params.meses} meses...")
        
        with DatabaseConnection(db_config.connection_string()) as db_conn:
            query_ventas = VentasQuery.get_ventas_ultimos_n_meses(params.meses)
            df_ventas_raw = db_conn.execute_query(query_ventas)
            
            update_progress(25, f"Ventas extraídas: {len(df_ventas_raw):,} registros")
            
            update_progress(30, "Extrayendo stock actual...")
            query_stock = StockQuery.get_stock_actual()
            df_stock_raw = db_conn.execute_query(query_stock)
            
            update_progress(40, f"Stock extraído: {len(df_stock_raw):,} registros")
        
        # ETAPA 3: Procesamiento de datos (40-60%)
        update_progress(45, "Procesando ventas...")
        ventas_processor = VentasProcessor(debug=params.debug)
        df_ventas = ventas_processor.process(df_ventas_raw)
        
        update_progress(52, "Procesando stock...")
        stock_processor = StockProcessor(debug=params.debug)
        df_stock = stock_processor.process(df_stock_raw, df_ventas)
        
        update_progress(60, f"Datos procesados: {len(df_ventas):,} ventas, {len(df_stock):,} stock")
        
        # Guardar intermedios si se solicita
        if params.save_intermediates:
            update_progress(62, "Guardando archivos intermedios...")
            df_ventas.to_excel(OUTPUT_DIR / 'Ventas_procesadas_intermediate.xlsx', index=False)
            df_stock.to_excel(OUTPUT_DIR / 'Stock_procesado_intermediate.xlsx', index=False)
        
        # ETAPA 4: Cálculo de traslados (60-90%)
        update_progress(65, "Inicializando motor de traslados...")
        
        orchestrator = TrasladosOrchestrator(
            df_ventas=df_ventas,
            df_stock=df_stock,
            bodega_principal='BODEGA PRINCIPAL',
            tiendas_path=Path('data/TIENDAS.csv'),
            tiempos_path=Path('data/TIEMPO.csv'),
            no_seed=not params.allow_seed,
            allow_seed_if_adu=True,
            debug=params.debug
        )
        
        update_progress(70, "Ejecutando Fase 1: Necesidades base...")
        update_progress(80, "Ejecutando Fase 2: Completar curvas...")
        update_progress(85, "Ejecutando Fase 3: Drenar bodega...")
        
        df_traslados, df_stock_final = orchestrator.run_all(
            enable_curvas=True,
            enable_drenaje=True,
            safety_ratio=params.safety_ratio
        )
        
        update_progress(90, f"Traslados calculados: {len(df_traslados):,} líneas")
        
        # ETAPA 5: Generación de archivos (90-100%)
        update_progress(92, "Generando archivo principal...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"Traslados_final_{timestamp}.xlsx"
        resumen_file = OUTPUT_DIR / f"Traslados_final_resumen_{timestamp}.xlsx"
        
        # Guardar archivo principal
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_traslados.to_excel(writer, sheet_name='Traslados', index=False)
            df_stock_final.to_excel(writer, sheet_name='Stock Final', index=False)
        
        update_progress(96, "Generando resumen...")
        
        # Guardar resumen
        with pd.ExcelWriter(resumen_file, engine='openpyxl') as writer:
            # Por Tienda
            resumen_tienda = df_traslados.groupby('Tienda destino').agg({
                'Unidades a trasladar': 'sum',
                'Referencia': 'nunique'
            }).reset_index()
            resumen_tienda.columns = ['Tienda', 'Total Unidades', 'Referencias Unicas']
            resumen_tienda = resumen_tienda.sort_values('Total Unidades', ascending=False)
            resumen_tienda.to_excel(writer, sheet_name='Por Tienda', index=False)
            
            # Por Fase
            resumen_fase = df_traslados.groupby('Fase').agg({
                'Unidades a trasladar': 'sum',
                'Tienda destino': 'nunique',
                'Referencia': 'nunique'
            }).reset_index()
            resumen_fase.columns = ['Fase', 'Total Unidades', 'Tiendas', 'Referencias']
            resumen_fase.to_excel(writer, sheet_name='Por Fase', index=False)
            
            # Top 50
            top_refs = df_traslados.groupby(['Referencia', 'Talla']).agg({
                'Unidades a trasladar': 'sum',
                'Tienda destino': 'nunique'
            }).reset_index()
            top_refs.columns = ['Referencia', 'Talla', 'Total Unidades', 'Num Tiendas']
            top_refs = top_refs.sort_values('Total Unidades', ascending=False).head(50)
            top_refs.to_excel(writer, sheet_name='Top 50 Referencias', index=False)
        
        execution_state["output_files"] = [
            str(output_file.name),
            str(resumen_file.name)
        ]
        
        update_progress(100, "¡Proceso completado exitosamente!")
        
        return {
            "success": True,
            "message": "Traslados generados exitosamente",
            "files": execution_state["output_files"],
            "stats": {
                "total_traslados": len(df_traslados),
                "total_unidades": int(df_traslados['Unidades a trasladar'].sum()),
                "referencias_unicas": int(df_traslados['Referencia'].nunique()),
                "tiendas_origen": int(df_traslados['Tienda origen'].nunique()),
                "tiendas_destino": int(df_traslados['Tienda destino'].nunique())
            }
        }
        
    except Exception as e:
        logger.error(f"Error en pipeline: {e}", exc_info=True)
        execution_state["error"] = str(e)
        update_progress(0, f"Error: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        execution_state["running"] = False


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Página principal"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/generate")
async def generate_traslados(params: TrasladosRequest, background_tasks: BackgroundTasks):
    """
    Endpoint para generar traslados
    """
    if execution_state["running"]:
        raise HTTPException(status_code=409, detail="Ya hay una ejecución en progreso")
    
    # Ejecutar en background
    background_tasks.add_task(run_traslados_pipeline, params)
    
    return {
        "message": "Proceso iniciado",
        "status": "running"
    }


@app.get("/api/status")
async def get_status():
    """
    Obtiene el estado actual de la ejecución
    """
    return {
        "running": execution_state["running"],
        "progress": execution_state["progress"],
        "stage": execution_state["stage"],
        "error": execution_state["error"],
        "output_files": execution_state["output_files"]
    }


@app.get("/api/download/{filename}")
async def download_file(filename: str):
    """
    Descarga un archivo generado
    """
    file_path = OUTPUT_DIR / filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.delete("/api/reset")
async def reset_state():
    """
    Resetea el estado de la aplicación
    """
    execution_state["running"] = False
    execution_state["progress"] = 0
    execution_state["stage"] = ""
    execution_state["error"] = None
    execution_state["output_files"] = []
    
    return {"message": "Estado reseteado"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)