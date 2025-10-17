"""
Motor principal de traslados - Emparejamiento de excesos y faltantes

Flujo:
1. Identificar necesidades BASE (tiendas sin stock mínimo)
2. Emparejar con orígenes disponibles
3. Ejecutar traslados respetando cobertura
4. (Opcional) Completar curvas desde bodega
5. (Opcional) Drenar residual de bodega
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import logging

from config.settings import (
    MIN_POR_SKU_TIENDA,
    MIN_POR_SKU_ECOM,
    MAX_STOCK_PER_SKU,
    ORIGIN_MIN_COV_DAYS,
    DEST_TARGET_COV_DAYS,
    ORIGIN_MIN_COV_ECOM,
    DEST_TARGET_COV_ECOM,
    COV_BUFFER_DAYS
)

logger = logging.getLogger(__name__)


class TrasladosEngineCore:
    """
    Motor principal de emparejamiento y ejecución de traslados
    """
    
    def __init__(self, 
                 stock_df: pd.DataFrame,
                 adu_df: pd.DataFrame,
                 tiempos_df: Optional[pd.DataFrame] = None,
                 bodega_principal: Optional[str] = None,
                 no_seed: bool = True,
                 allow_seed_if_adu: bool = False,
                 debug: bool = False):
        """
        Args:
            stock_df: Stock con columnas: Tienda, SKU, Referencia, Talla, 
                      Existencia, ADU, Cobertura_dias, IsEcom
            adu_df: DataFrame con ADU por Tienda/SKU (para validar siembra)
            tiempos_df: DataFrame de tiempos de entrega (opcional)
            bodega_principal: Nombre de bodega principal (puede quedar en 0)
            no_seed: Si True, no permitir siembra de referencias nuevas
            allow_seed_if_adu: Si True, permite siembra si SKU tiene ADU > 0
            debug: Modo debug con logs detallados
        """
        self.stock_df = stock_df.copy()
        self.adu_df = adu_df
        self.tiempos_df = tiempos_df
        self.bodega_principal = bodega_principal
        self.no_seed = no_seed
        self.allow_seed_if_adu = allow_seed_if_adu
        self.debug = debug
        
        # Asignar MinObjetivo por tipo de tienda
        self.stock_df['MinObjetivo'] = np.where(
            self.stock_df['IsEcom'], 
            MIN_POR_SKU_ECOM, 
            MIN_POR_SKU_TIENDA
        )
        
        # Bodega principal no necesita stock mínimo
        if bodega_principal:
            self.stock_df.loc[
                self.stock_df['Tienda'] == bodega_principal, 
                'MinObjetivo'
            ] = 0
        
        self.transfers = []
        
        # Índice para búsquedas rápidas
        self._build_indexes()
    
    def can_seed_to_store(self, tienda: str, referencia: str, sku: str) -> bool:
        """
        Valida si se puede sembrar (introducir) un SKU en una tienda
        
        Política de siembra:
        1. Si la tienda YA tiene la referencia (cualquier talla) → ✓ Permitir
        2. Si no_seed=True (default) → ✗ Bloquear
        3. Si allow_seed_if_adu=True Y el SKU tiene ADU>0 en esa tienda → ✓ Permitir
        
        Args:
            tienda: Tienda destino
            referencia: Referencia del producto
            sku: SKU específico (ref + talla)
        
        Returns:
            True si se permite el traslado/siembra
        """
        # Verificar si la tienda YA tiene esta referencia (cualquier talla)
        has_ref_now = (
            self.stock_df[
                (self.stock_df['Tienda'] == tienda) &
                (self.stock_df['Referencia'] == referencia) &
                (self.stock_df['Existencia'] > 0)
            ].shape[0] > 0
        )
        
        # Si ya tiene la referencia, siempre permitir
        if has_ref_now:
            return True
        
        # Si no tiene la referencia, evaluar política de siembra
        
        # Política: NO sembrar (default seguro)
        if self.no_seed:
            # Excepción: permitir si el SKU específico tiene ventas históricas
            if self.allow_seed_if_adu:
                adu_value = self._get_adu_for_sku(tienda, sku)
                if adu_value > 0:
                    if self.debug:
                        logger.debug(f"[seed-permitido] {tienda} / {sku}: ADU={adu_value:.2f}")
                    return True
            
            # Bloqueado: referencia nueva sin ventas
            if self.debug:
                logger.debug(f"[seed-bloqueado] {tienda} REF {referencia} SKU {sku}: "
                           f"no tiene ref y ADU<=0")
            return False
        
        # Si no_seed=False, siempre permitir siembra
        return True
    
    def _get_adu_for_sku(self, tienda: str, sku: str) -> float:
        """Obtiene ADU de un SKU en una tienda desde adu_df"""
        if self.adu_df is None or self.adu_df.empty:
            return 0.0
        
        match = self.adu_df[
            (self.adu_df['Tienda'] == tienda) &
            (self.adu_df['SKU'].astype(str).str.strip().str.upper() == 
             str(sku).strip().upper())
        ]
        
        if match.empty:
            return 0.0
        
        return float(match['ADU'].iloc[0])
    
    def _build_indexes(self) -> None:
        """Construir índices para búsquedas rápidas"""
        # Índice: (Tienda, SKU) -> fila(s) en stock_df
        self.idx_tienda_sku = {}
        for idx, row in self.stock_df.iterrows():
            key = (row['Tienda'], row['SKU'])
            if key not in self.idx_tienda_sku:
                self.idx_tienda_sku[key] = []
            self.idx_tienda_sku[key].append(idx)
    
    def get_stock(self, tienda: str, sku: str) -> int:
        """Obtener stock actual de un SKU en una tienda"""
        key = (tienda, sku)
        if key not in self.idx_tienda_sku:
            return 0
        
        indices = self.idx_tienda_sku[key]
        return int(self.stock_df.loc[indices, 'Existencia'].sum())
    
    def get_cobertura(self, tienda: str, sku: str) -> float:
        """Obtener cobertura en días de un SKU en una tienda"""
        key = (tienda, sku)
        if key not in self.idx_tienda_sku:
            return np.inf
        
        indices = self.idx_tienda_sku[key]
        row = self.stock_df.loc[indices[0]]
        
        adu = float(row['ADU']) if pd.notna(row['ADU']) else 0.0
        stock = self.get_stock(tienda, sku)
        
        if adu > 0:
            return stock / adu
        return np.inf
    
    def allowed_to_send(self, tienda: str, sku: str) -> int:
        """
        Calcula unidades disponibles para enviar desde una tienda/SKU
        
        Reglas:
        - Bodega principal: Puede dar TODO
        - Otras tiendas: Deben guardar max(MinObjetivo, CoberturaDías * ADU)
        """
        key = (tienda, sku)
        if key not in self.idx_tienda_sku:
            return 0
        
        indices = self.idx_tienda_sku[key]
        row = self.stock_df.loc[indices[0]]
        
        stock_actual = self.get_stock(tienda, sku)
        
        # Bodega principal puede dar todo
        if self.bodega_principal and tienda == self.bodega_principal:
            return stock_actual
        
        # Otras tiendas: guardar mínimo + cobertura
        min_objetivo = int(row['MinObjetivo'])
        adu = float(row['ADU']) if pd.notna(row['ADU']) else 0.0
        is_ecom = bool(row['IsEcom'])
        
        # Cobertura mínima en días
        min_cov_days = ORIGIN_MIN_COV_ECOM if is_ecom else ORIGIN_MIN_COV_DAYS
        
        # Guardar: max(MinObjetivo, CoberturaDías * ADU)
        if adu > 0:
            guardar_por_cobertura = int(np.ceil(min_cov_days * adu))
            guardar = max(min_objetivo, guardar_por_cobertura)
        else:
            guardar = min_objetivo
        
        disponible = max(0, stock_actual - guardar)
        
        return disponible
    
    def calculate_target_units(self, tienda: str, sku: str) -> int:
        """
        Calcula objetivo de unidades para un SKU en tienda destino
        
        target = max(MinObjetivo, CoberturaDías * ADU)
        cap con MAX_STOCK_PER_SKU
        """
        key = (tienda, sku)
        
        # Si no existe el SKU en la tienda, buscar info de ADU
        if key not in self.idx_tienda_sku:
            # No podemos calcular target sin info
            return MIN_POR_SKU_TIENDA
        
        indices = self.idx_tienda_sku[key]
        row = self.stock_df.loc[indices[0]]
        
        min_objetivo = int(row['MinObjetivo'])
        adu = float(row['ADU']) if pd.notna(row['ADU']) else 0.0
        is_ecom = bool(row['IsEcom'])
        
        target_cov_days = DEST_TARGET_COV_ECOM if is_ecom else DEST_TARGET_COV_DAYS
        
        if adu > 0:
            target_por_cobertura = int(np.ceil(target_cov_days * adu))
            target = max(min_objetivo, target_por_cobertura)
        else:
            target = min_objetivo
        
        # Cap con máximo
        target = min(target, MAX_STOCK_PER_SKU)
        
        return target
    
    def identify_base_needs(self) -> pd.DataFrame:
        """
        Identifica necesidades BASE: tiendas con stock < MinObjetivo
        
        Returns:
            DataFrame con columnas: Tienda, SKU, Referencia, Talla, 
                                   Necesita, IsEcom, ADU
        """
        logger.info("Identificando necesidades base...")
        
        # Filtrar: stock < MinObjetivo y NO es bodega principal
        mask = (self.stock_df['Existencia'] < self.stock_df['MinObjetivo'])
        
        if self.bodega_principal:
            mask &= (self.stock_df['Tienda'] != self.bodega_principal)
        
        needs = self.stock_df[mask].copy()
        
        if needs.empty:
            logger.info("No hay necesidades base")
            return pd.DataFrame(columns=[
                'Tienda', 'SKU', 'Referencia', 'Talla', 
                'Necesita', 'IsEcom', 'ADU'
            ])
        
        # Calcular unidades necesarias
        needs['Necesita'] = (
            needs['MinObjetivo'] - needs['Existencia']
        ).astype(int)
        
        # Cap con capacidad máxima
        needs['CapMax'] = MAX_STOCK_PER_SKU - needs['Existencia']
        needs['Necesita'] = needs[['Necesita', 'CapMax']].min(axis=1).clip(lower=0).astype(int)
        
        # Filtrar necesidades > 0
        needs = needs[needs['Necesita'] > 0].copy()
        
        # Ordenar por urgencia: ADU descendente (alta rotación primero)
        needs = needs.sort_values('ADU', ascending=False)
        
        logger.info(f"Necesidades base: {len(needs):,} registros")
        logger.info(f"  Unidades totales necesarias: {needs['Necesita'].sum():,}")
        
        return needs[['Tienda', 'SKU', 'Referencia', 'Talla', 'Necesita', 'IsEcom', 'ADU']]
    
    def rank_origins_for_sku(self, 
                            sku: str, 
                            dest_tienda: str,
                            dest_referencia: str) -> List[str]:
        """
        Rankea tiendas origen disponibles para un SKU
        
        Criterios (en orden):
        1. Misma región
        2. Prioridad logística (de tiempos_df)
        3. Mayor cobertura en origen
        4. Menor tiempo de entrega (de tiempos_df)
        
        Args:
            sku: SKU a trasladar
            dest_tienda: Tienda destino
            dest_referencia: Referencia del SKU (para validar siembra)
        
        Returns:
            Lista de tiendas ordenadas por prioridad descendente
        """
        # Construir lista de candidatos
        candidates = []
        
        for (tienda, sku_check), indices in self.idx_tienda_sku.items():
            if sku_check != sku:
                continue
            if tienda == dest_tienda:
                continue
            
            disponible = self.allowed_to_send(tienda, sku)
            if disponible <= 0:
                continue
            
            cov_origen = self.get_cobertura(tienda, sku)
            cov_destino = self.get_cobertura(dest_tienda, sku)
            
            # Filtro: Solo si origen tiene más cobertura que destino
            if np.isfinite(cov_origen) and np.isfinite(cov_destino):
                if cov_origen <= cov_destino + COV_BUFFER_DAYS:
                    continue
            
            candidates.append(tienda)
        
        if not candidates:
            return []
        
        # Rankear por múltiples criterios
        ranked = []
        
        for origen in candidates:
            # Criterio 1: Misma región
            same_region = self._check_same_region(origen, dest_tienda)
            region_rank = 0 if same_region else 1
            
            # Criterio 2: Prioridad logística
            priority = self._get_delivery_priority(origen, dest_tienda)
            priority_rank = int(priority) if pd.notna(priority) else 999
            
            # Criterio 3: Cobertura origen (negado para orden desc)
            cov_origen = self.get_cobertura(origen, sku)
            cov_rank = -(cov_origen if np.isfinite(cov_origen) else 1e9)
            
            # Criterio 4: Tiempo de entrega
            lead_time = self._get_delivery_days(origen, dest_tienda)
            time_rank = float(lead_time) if pd.notna(lead_time) else 999
            
            ranked.append((
                region_rank,    # 0 = misma región (prioritario)
                priority_rank,  # Menor = mejor
                cov_rank,       # Más negativo = más cobertura (prioritario)
                time_rank,      # Menor = más rápido (prioritario)
                origen
            ))
        
        # Ordenar lexicográficamente
        ranked.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        
        # Extraer solo nombres de tiendas
        origins_sorted = [r[4] for r in ranked]
        
        # Forzar bodega principal al inicio si está disponible
        if self.bodega_principal and self.bodega_principal in origins_sorted:
            origins_sorted.remove(self.bodega_principal)
            origins_sorted.insert(0, self.bodega_principal)
        
        return origins_sorted
    
    def _check_same_region(self, tienda_a: str, tienda_b: str) -> bool:
        """
        Verifica si dos tiendas están en la misma región
        
        Usa RegionID o Region (en ese orden de prioridad)
        """
        if 'RegionID' not in self.stock_df.columns and 'Region' not in self.stock_df.columns:
            return False
        
        # Buscar info de región para cada tienda
        def get_region_info(tienda):
            rows = self.stock_df[self.stock_df['Tienda'] == tienda]
            if rows.empty:
                return (None, None)
            
            region_id = rows.iloc[0].get('RegionID')
            region = rows.iloc[0].get('Region')
            
            return (region_id, region)
        
        region_a = get_region_info(tienda_a)
        region_b = get_region_info(tienda_b)
        
        # Comparar por RegionID (prioritario)
        if pd.notna(region_a[0]) and pd.notna(region_b[0]):
            return int(region_a[0]) == int(region_b[0])
        
        # Fallback: comparar por Region (nombre)
        if pd.notna(region_a[1]) and pd.notna(region_b[1]):
            return str(region_a[1]).strip().upper() == str(region_b[1]).strip().upper()
        
        return False
    
    def _get_delivery_priority(self, origen: str, destino: str) -> float:
        """Obtiene prioridad logística de tiempos_df"""
        if self.tiempos_df is None or self.tiempos_df.empty:
            return np.nan
        
        origen_norm = str(origen).strip().upper()
        destino_norm = str(destino).strip().upper()
        
        match = self.tiempos_df[
            (self.tiempos_df['_O'] == origen_norm) & 
            (self.tiempos_df['_D'] == destino_norm)
        ]
        
        if match.empty:
            return np.nan
        
        return match.iloc[0]['_PRI_NUM']
    
    def _get_delivery_days(self, origen: str, destino: str) -> float:
        """Obtiene días de entrega de tiempos_df"""
        if self.tiempos_df is None or self.tiempos_df.empty:
            return np.nan
        
        origen_norm = str(origen).strip().upper()
        destino_norm = str(destino).strip().upper()
        
        match = self.tiempos_df[
            (self.tiempos_df['_O'] == origen_norm) & 
            (self.tiempos_df['_D'] == destino_norm)
        ]
        
        if match.empty:
            return np.nan
        
        return match.iloc[0]['_ETA_NUM']
    
    def execute_transfer(self, 
                        origen: str, 
                        destino: str,
                        sku: str,
                        cantidad: int,
                        referencia: str,
                        talla: str) -> bool:
        """
        Ejecuta un traslado: actualiza stock y registra
        
        Returns:
            True si el traslado se ejecutó, False si fue bloqueado por siembra
        """
        key_origen = (origen, sku)
        key_destino = (destino, sku)
        
        # Validar siembra ANTES de ejecutar
        if key_destino not in self.idx_tienda_sku:
            # Destino no tiene este SKU - requiere siembra
            if not self.can_seed_to_store(destino, referencia, sku):
                if self.debug:
                    logger.debug(f"[transfer-bloqueado] Siembra no permitida: "
                               f"{destino} / {referencia} / {sku}")
                return False
        
        # Obtener stocks antes
        stock_origen_antes = self.get_stock(origen, sku)
        stock_destino_antes = self.get_stock(destino, sku)
        
        # Actualizar origen
        if key_origen in self.idx_tienda_sku:
            indices_origen = self.idx_tienda_sku[key_origen]
            qty_per_row = cantidad / len(indices_origen)
            self.stock_df.loc[indices_origen, 'Existencia'] -= qty_per_row
        
        # Actualizar destino (o crear fila si es siembra)
        if key_destino in self.idx_tienda_sku:
            indices_destino = self.idx_tienda_sku[key_destino]
            qty_per_row = cantidad / len(indices_destino)
            self.stock_df.loc[indices_destino, 'Existencia'] += qty_per_row
        else:
            # Crear fila nueva (siembra validada)
            new_row = self._create_new_stock_row(destino, sku, referencia, talla, cantidad)
            new_index = len(self.stock_df)
            self.stock_df = pd.concat([
                self.stock_df, 
                pd.DataFrame([new_row], index=[new_index])
            ], ignore_index=False)
            
            # Actualizar índice
            self.idx_tienda_sku[key_destino] = [new_index]
            
            if self.debug:
                logger.debug(f"[siembra] Creada fila nueva: {destino} / {sku}")
        
        # Obtener stocks después
        stock_origen_despues = self.get_stock(origen, sku)
        stock_destino_despues = self.get_stock(destino, sku)
        
        # Registrar traslado
        self.transfers.append({
            'Tienda origen': origen,
            'Tienda destino': destino,
            'Stock tienda origen antes traslado': stock_origen_antes,
            'Stock tienda origen despues traslado': stock_origen_despues,
            'Stock tienda destino antes traslado': stock_destino_antes,
            'Stock tienda destino despues del traslado': stock_destino_despues,
            'Unidades a trasladar': cantidad,
            'Referencia': referencia,
            'Talla': talla
        })
        
        return True
    
    def _create_new_stock_row(self, 
                             tienda: str, 
                             sku: str, 
                             referencia: str, 
                             talla: str,
                             cantidad: int) -> dict:
        """
        Crea una fila nueva para stock (siembra)
        
        Copia metadatos de otras filas de la misma tienda si existen
        """
        # Buscar metadata de la tienda
        tienda_rows = self.stock_df[self.stock_df['Tienda'] == tienda]
        
        if not tienda_rows.empty:
            sample = tienda_rows.iloc[0]
            region = sample.get('Region')
            region_id = sample.get('RegionID')
            tipo = sample.get('Tipo')
            is_ecom = sample.get('IsEcom', False)
        else:
            region = None
            region_id = None
            tipo = None
            is_ecom = 'ECOM' in str(tienda).upper()
        
        # Obtener ADU si existe
        adu = self._get_adu_for_sku(tienda, sku)
        
        # MinObjetivo
        min_objetivo = MIN_POR_SKU_ECOM if is_ecom else MIN_POR_SKU_TIENDA
        
        # Cobertura inicial (infinito si ADU=0)
        cobertura = cantidad / adu if adu > 0 else np.inf
        
        return {
            'Tienda': tienda,
            'SKU': sku,
            'Referencia': referencia,
            'Talla': talla,
            'RANGO_CAT': None,  # Se podría inferir de talla
            'Region': region,
            'RegionID': region_id,
            'Tipo': tipo,
            'IsEcom': is_ecom,
            'MinObjetivo': min_objetivo,
            'ADU': adu,
            'Cobertura_dias': cobertura,
            'Existencia': cantidad
        }
    
    def process_base_needs(self, needs_df: pd.DataFrame):
        """
        Procesa necesidades base: empareja y ejecuta traslados
        
        Args:
            needs_df: DataFrame de necesidades (de identify_base_needs)
        """
        if needs_df.empty:
            logger.info("No hay necesidades base que procesar")
            return
        
        logger.info(f"Procesando {len(needs_df):,} necesidades base...")
        
        satisfechas = 0
        parciales = 0
        no_satisfechas = 0
        bloqueadas_siembra = 0
        
        for idx, need in needs_df.iterrows():
            tienda = need['Tienda']
            sku = need['SKU']
            ref = need['Referencia']
            talla = need['Talla']
            
            # Validar siembra ANTES de buscar orígenes
            key_destino = (tienda, sku)
            if key_destino not in self.idx_tienda_sku:
                # Requiere siembra - validar política
                if not self.can_seed_to_store(tienda, ref, sku):
                    bloqueadas_siembra += 1
                    continue
            
            # Calcular objetivo
            target = self.calculate_target_units(tienda, sku)
            
            # Rankear orígenes (pasando referencia para validación)
            origins = self.rank_origins_for_sku(sku, tienda, ref)
            
            if not origins:
                no_satisfechas += 1
                if self.debug:
                    logger.debug(f"Sin orígenes para {tienda} / {sku}")
                continue
            
            # Intentar llenar objetivo
            stock_inicial = self.get_stock(tienda, sku)
            gap = max(0, target - stock_inicial)
            
            movido_total = 0
            
            for origen in origins:
                if gap <= 0:
                    break
                
                disponible = self.allowed_to_send(origen, sku)
                if disponible <= 0:
                    continue
                
                # Respetar capacidad máxima de destino
                stock_actual_dest = self.get_stock(tienda, sku)
                cap_disponible = max(0, MAX_STOCK_PER_SKU - stock_actual_dest)
                
                # Cantidad a trasladar
                qty = min(disponible, gap, cap_disponible)
                
                if qty > 0:
                    success = self.execute_transfer(origen, tienda, sku, qty, ref, talla)
                    
                    if success:
                        movido_total += qty
                        gap -= qty
                    else:
                        # Traslado bloqueado (no debería pasar ya que validamos antes)
                        if self.debug:
                            logger.debug(f"Traslado bloqueado: {origen} → {tienda} / {sku}")
            
            if movido_total > 0:
                if gap == 0:
                    satisfechas += 1
                else:
                    parciales += 1
            else:
                no_satisfechas += 1
        
        logger.info(f"Necesidades base procesadas:")
        logger.info(f"  OK Satisfechas: {satisfechas}")
        logger.info(f"  ~ Parciales: {parciales}")
        logger.info(f"  X No satisfechas: {no_satisfechas}")
        if bloqueadas_siembra > 0:
            logger.info(f"  🚫 Bloqueadas por siembra: {bloqueadas_siembra}")
        logger.info(f"  Total traslados ejecutados: {len(self.transfers):,}")
    
    def run(self) -> pd.DataFrame:
        """
        Ejecuta motor principal de traslados
        
        Returns:
            DataFrame con traslados ejecutados
        """
        logger.info("=" * 60)
        logger.info("MOTOR PRINCIPAL DE TRASLADOS")
        logger.info("=" * 60)
        
        # FASE 1: Necesidades base
        needs_base = self.identify_base_needs()
        self.process_base_needs(needs_base)
        
        # FASE 2: Curvas (implementar después)
        # FASE 3: Drenaje (implementar después)
        
        # Retornar traslados
        if not self.transfers:
            logger.warning("No se generaron traslados")
            return pd.DataFrame(columns=[
                'Tienda origen', 'Tienda destino',
                'Stock tienda origen antes traslado',
                'Stock tienda origen despues traslado',
                'Stock tienda destino antes traslado',
                'Stock tienda destino despues del traslado',
                'Unidades a trasladar', 'Referencia', 'Talla'
            ])
        
        return pd.DataFrame(self.transfers)
