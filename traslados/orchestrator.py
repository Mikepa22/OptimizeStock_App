"""
Orquestador completo del sistema de traslados

Ejecuta las 3 fases en orden:
1. Necesidades base (garantizar mínimos)
2. Completar curvas (balancear tallas)
3. Drenar bodega (minimizar inventario muerto)
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Tuple

from .engine_core import TrasladosEngineCore
from .curve_completer import CurveCompleter
from .bodega_drainer import BodegaDrainer
from .data_loader import prepare_auxiliary_data
from .adu_calculator import calculate_adu_from_ventas, enrich_stock_with_adu

logger = logging.getLogger(__name__)


class TrasladosOrchestrator:
    """
    Orquestador completo del sistema de traslados
    
    Coordina las 3 fases del algoritmo y gestiona el flujo de datos entre ellas.
    """
    
    def __init__(self,
                 df_ventas: pd.DataFrame,
                 df_stock: pd.DataFrame,
                 bodega_principal: Optional[str] = None,
                 tiendas_path: Optional[Path] = None,
                 tiempos_path: Optional[Path] = None,
                 no_seed: bool = True,
                 allow_seed_if_adu: bool = False,
                 debug: bool = False):
        """
        Args:
            df_ventas: DataFrame de ventas procesadas
            df_stock: DataFrame de stock procesado
            bodega_principal: Nombre de bodega principal (opcional)
            tiendas_path: Path a Clasificacion_Tiendas.csv (opcional)
            tiempos_path: Path a Tiempos de entrega.csv (opcional)
            no_seed: No permitir siembra de referencias nuevas
            allow_seed_if_adu: Permitir siembra si SKU tiene ADU > 0
            debug: Modo debug
        """
        self.df_ventas = df_ventas
        self.df_stock_original = df_stock.copy()
        self.bodega_principal = bodega_principal
        self.no_seed = no_seed
        self.allow_seed_if_adu = allow_seed_if_adu
        self.debug = debug
        
        # Cargar datos auxiliares
        logger.info("Cargando datos auxiliares...")
        self.tiendas_map, self.tiendas_df, self.tiempos_df = prepare_auxiliary_data(
            tiendas_path=tiendas_path,
            tiempos_path=tiempos_path
        )
        
        # Calcular ADU
        logger.info("Calculando velocidad de venta (ADU)...")
        self.adu_df = calculate_adu_from_ventas(df_ventas)
        
        # Enriquecer stock con ADU
        logger.info("Enriqueciendo stock con ADU...")
        self.df_stock = enrich_stock_with_adu(df_stock, self.adu_df)
        
        # Detectar bodega principal si no se especificó
        if not self.bodega_principal:
            self.bodega_principal = self._detect_bodega_principal()
        
        # Almacenar resultados
        self.traslados_fase1 = []
        self.traslados_fase2 = []
        self.traslados_fase3 = []
    
    def _detect_bodega_principal(self) -> Optional[str]:
        """
        Detecta bodega principal automáticamente
        
        Criterios:
        - Nombre contiene 'BODEGA', 'CEDI', 'PRINCIPAL'
        - Mayor stock total
        """
        candidatos = self.df_stock[
            self.df_stock['Tienda'].str.contains(
                'BODEGA|CEDI|PRINCIPAL',
                case=False,
                regex=True,
                na=False
            )
        ]
        
        if candidatos.empty:
            logger.warning("No se detectó bodega principal automáticamente")
            return None
        
        # Seleccionar la con mayor stock
        stock_por_tienda = candidatos.groupby('Tienda')['Existencia'].sum()
        bodega = stock_por_tienda.idxmax()
        
        logger.info(f"Bodega principal detectada: {bodega}")
        return bodega
    
    def run_fase1_necesidades_base(self) -> pd.DataFrame:
        """
        FASE 1: Satisfacer necesidades base
        
        Garantiza que todas las tiendas tengan el mínimo requerido por SKU.
        """
        logger.info("\n" + "=" * 70)
        logger.info("FASE 1: NECESIDADES BASE")
        logger.info("=" * 70)
        
        engine = TrasladosEngineCore(
            stock_df=self.df_stock,
            adu_df=self.adu_df,
            tiempos_df=self.tiempos_df,
            bodega_principal=self.bodega_principal,
            no_seed=self.no_seed,
            allow_seed_if_adu=self.allow_seed_if_adu,
            debug=self.debug
        )
        
        # Ejecutar solo fase 1
        needs = engine.identify_base_needs()
        engine.process_base_needs(needs)
        
        # Actualizar stock con los cambios
        self.df_stock = engine.stock_df
        self.traslados_fase1 = engine.transfers
        
        logger.info(f"OK Fase 1 completada: {len(self.traslados_fase1)} traslados")
        
        return pd.DataFrame(self.traslados_fase1)
    
    def run_fase2_completar_curvas(self) -> pd.DataFrame:
        """
        FASE 2: Completar curvas de tallas
        
        Completa tallas faltantes en tiendas que ya manejan la referencia.
        """
        logger.info("\n" + "=" * 70)
        logger.info("FASE 2: COMPLETAR CURVAS")
        logger.info("=" * 70)
        
        if not self.bodega_principal:
            logger.warning("Sin bodega principal - omitiendo fase 2")
            return pd.DataFrame()
        
        completer = CurveCompleter(
            stock_df=self.df_stock,
            adu_df=self.adu_df,
            bodega_principal=self.bodega_principal,
            debug=self.debug
        )
        
        self.df_stock, self.traslados_fase2 = completer.complete_curves()
        
        logger.info(f"OK Fase 2 completada: {len(self.traslados_fase2)} traslados")
        
        return pd.DataFrame(self.traslados_fase2)
    
    def run_fase3_drenar_bodega(self, safety_ratio: float = 0.0) -> pd.DataFrame:
        """
        FASE 3: Drenar residual de bodega
        
        Envía stock residual a tiendas que lo venden, minimizando inventario muerto.
        
        Args:
            safety_ratio: % de stock a conservar en bodega (0.0-1.0)
                         0.0 = drenar todo
                         0.2 = conservar 20%
        """
        logger.info("\n" + "=" * 70)
        logger.info("FASE 3: DRENAR BODEGA")
        logger.info("=" * 70)
        
        if not self.bodega_principal:
            logger.warning("Sin bodega principal - omitiendo fase 3")
            return pd.DataFrame()
        
        drainer = BodegaDrainer(
            stock_df=self.df_stock,
            adu_df=self.adu_df,
            bodega_principal=self.bodega_principal,
            no_seed=self.no_seed,
            allow_seed_if_adu=self.allow_seed_if_adu,
            debug=self.debug
        )
        
        self.df_stock, self.traslados_fase3 = drainer.drain(safety_ratio=safety_ratio)
        
        logger.info(f"OK Fase 3 completada: {len(self.traslados_fase3)} traslados")
        
        return pd.DataFrame(self.traslados_fase3)
    
    def run_all(self,
                enable_curvas: bool = True,
                enable_drenaje: bool = True,
                safety_ratio: float = 0.0) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Ejecuta todas las fases del algoritmo
        
        Args:
            enable_curvas: Ejecutar fase 2 (completar curvas)
            enable_drenaje: Ejecutar fase 3 (drenar bodega)
            safety_ratio: % stock a conservar en bodega (fase 3)
        
        Returns:
            Tuple (traslados_df, stock_final_df)
        """
        logger.info("INICIANDO SISTEMA DE TRASLADOS COMPLETO")
        logger.info(f"Bodega principal: {self.bodega_principal}")
        logger.info(f"Política siembra: no_seed={self.no_seed}, allow_adu={self.allow_seed_if_adu}")
        
        # FASE 1: Necesidades base (siempre)
        self.run_fase1_necesidades_base()
        
        # FASE 2: Curvas (opcional)
        if enable_curvas and self.bodega_principal:
            self.run_fase2_completar_curvas()
        else:
            logger.info("\nFASE 2: OMITIDA (deshabilitada o sin bodega)")
        
        # FASE 3: Drenaje (opcional)
        if enable_drenaje and self.bodega_principal:
            self.run_fase3_drenar_bodega(safety_ratio=safety_ratio)
        else:
            logger.info("\nFASE 3: OMITIDA (deshabilitada o sin bodega)")
        
        # Consolidar todos los traslados
        all_transfers = (
            self.traslados_fase1 +
            self.traslados_fase2 +
            self.traslados_fase3
        )
        
        df_traslados = pd.DataFrame(all_transfers)
        
        # Agregar columna de fase
        if not df_traslados.empty:
            fases = (
                ['Fase 1: Base'] * len(self.traslados_fase1) +
                ['Fase 2: Curvas'] * len(self.traslados_fase2) +
                ['Fase 3: Drenaje'] * len(self.traslados_fase3)
            )
            df_traslados['Fase'] = fases
        
        # Reordenar columnas
        if not df_traslados.empty:
            cols_order = [
                'Fase',
                'Tienda origen',
                'Tienda destino',
                'Referencia',
                'Talla',
                'Unidades a trasladar',
                'Stock tienda origen antes traslado',
                'Stock tienda origen despues traslado',
                'Stock tienda destino antes traslado',
                'Stock tienda destino despues del traslado'
            ]
            df_traslados = df_traslados[cols_order]
        
        # Generar resumen
        self._print_summary(df_traslados)
        
        return df_traslados, self.df_stock
    
    def _print_summary(self, df_traslados: pd.DataFrame):
        """Imprime resumen ejecutivo"""
        logger.info("\n" + "=" * 70)
        logger.info("RESUMEN EJECUTIVO")
        logger.info("=" * 70)
        
        if df_traslados.empty:
            logger.info("⚠️  No se generaron traslados")
            return
        
        logger.info(f"Total traslados: {len(df_traslados):,}")
        logger.info(f"  Fase 1 (Base):    {len(self.traslados_fase1):,}")
        logger.info(f"  Fase 2 (Curvas):  {len(self.traslados_fase2):,}")
        logger.info(f"  Fase 3 (Drenaje): {len(self.traslados_fase3):,}")
        logger.info("")
        
        total_unidades = df_traslados['Unidades a trasladar'].sum()
        logger.info(f"Unidades totales movidas: {total_unidades:,}")
        logger.info(f"Referencias únicas: {df_traslados['Referencia'].nunique():,}")
        logger.info(f"Tiendas origen: {df_traslados['Tienda origen'].nunique()}")
        logger.info(f"Tiendas destino: {df_traslados['Tienda destino'].nunique()}")
        
        if self.bodega_principal:
            bodega_final = self.df_stock[
                self.df_stock['Tienda'] == self.bodega_principal
            ]['Existencia'].sum()
            logger.info(f"\nStock final en bodega: {int(bodega_final):,} unidades")
    
    def export_results(self, output_path: Path):
        """
        Exporta resultados a Excel
        
        Args:
            output_path: Ruta del archivo de salida
        """
        logger.info(f"\nExportando resultados a {output_path}...")
        
        # Consolidar traslados
        all_transfers = (
            self.traslados_fase1 +
            self.traslados_fase2 +
            self.traslados_fase3
        )
        
        df_traslados = pd.DataFrame(all_transfers)
        
        if not df_traslados.empty:
            # Agregar fase
            fases = (
                ['Fase 1: Base'] * len(self.traslados_fase1) +
                ['Fase 2: Curvas'] * len(self.traslados_fase2) +
                ['Fase 3: Drenaje'] * len(self.traslados_fase3)
            )
            df_traslados['Fase'] = fases
        
        # Stock final
        df_stock_final = self.df_stock.groupby(
            ['Tienda', 'SKU', 'Referencia', 'Talla'],
            as_index=False
        )['Existencia'].sum()
        
        # Exportar
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # Hoja 1: Traslados
            if not df_traslados.empty:
                df_traslados.to_excel(writer, sheet_name='Traslados', index=False)
            else:
                pd.DataFrame({'Mensaje': ['No se generaron traslados']}).to_excel(
                    writer, sheet_name='Traslados', index=False
                )
            
            # Hoja 2: Stock final
            df_stock_final.to_excel(writer, sheet_name='Stock_Final', index=False)
            
            # Hoja 3: Resumen
            resumen_data = {
                'Métrica': [
                    'Total traslados',
                    'Fase 1: Necesidades base',
                    'Fase 2: Completar curvas',
                    'Fase 3: Drenar bodega',
                    'Unidades totales movidas',
                    'Referencias únicas',
                    'Tiendas origen',
                    'Tiendas destino'
                ],
                'Valor': [
                    len(df_traslados) if not df_traslados.empty else 0,
                    len(self.traslados_fase1),
                    len(self.traslados_fase2),
                    len(self.traslados_fase3),
                    int(df_traslados['Unidades a trasladar'].sum()) if not df_traslados.empty else 0,
                    df_traslados['Referencia'].nunique() if not df_traslados.empty else 0,
                    df_traslados['Tienda origen'].nunique() if not df_traslados.empty else 0,
                    df_traslados['Tienda destino'].nunique() if not df_traslados.empty else 0
                ]
            }
            pd.DataFrame(resumen_data).to_excel(writer, sheet_name='Resumen', index=False)
        
        logger.info(f"✓ Resultados exportados: {output_path}")