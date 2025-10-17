"""
Consultas SQL a las vistas de SIESA/Seven ERP
"""
from datetime import datetime, timedelta
from typing import Optional

class VentasQuery:
    """Consultas relacionadas con ventas"""
    
    @staticmethod
    def get_ventas_ultimos_n_meses(meses: int = 2) -> str:
        """
        Retorna query para obtener ventas de los últimos N meses
        desde la vista MP_VENTAS_CODE
        
        Args:
            meses: Número de meses hacia atrás (default: 2)
        
        Returns:
            Query SQL listo para ejecutar
        """
        return f"""
        -- Ventas unificadas de los últimos {meses} meses
        DECLARE @FechaInicio DATE = DATEADD(MONTH, -{meses}, GETDATE());
        
        SELECT 
            [C.O.],
            [Fecha],
            [Estado],
            [Bodega],
            [Descripcion C.O.],
            [Referencia],
            [Desc. item],
            [Talla],
            [Cantidad inv.],
            [Valor neto],
            [RANGO],
            [CLASIFICACION],
            [Fuente]
        FROM dbo.MP_VENTAS_CODE
        WHERE CAST([Fecha] AS DATE) >= @FechaInicio
        ORDER BY [Fecha] DESC, [C.O.], [Referencia];
        """
    
    @staticmethod
    def get_ventas_por_rango_fechas(fecha_inicio: str, fecha_fin: str) -> str:
        """
        Query parametrizada por rango de fechas
        
        Args:
            fecha_inicio: Fecha en formato 'YYYY-MM-DD'
            fecha_fin: Fecha en formato 'YYYY-MM-DD'
        """
        return f"""
        SELECT 
            [C.O.],
            [Fecha],
            [Estado],
            [Bodega],
            [Descripcion C.O.],
            [Referencia],
            [Desc. item],
            [Talla],
            [Cantidad inv.],
            [Valor neto],
            [RANGO],
            [CLASIFICACION],
            [Fuente]
        FROM dbo.MP_VENTAS_CODE
        WHERE CAST([Fecha] AS DATE) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        ORDER BY [Fecha] DESC, [C.O.], [Referencia];
        """
    
    @staticmethod
    def get_ventas_todas() -> str:
        """
        Query para obtener TODAS las ventas (sin filtro de fecha)
        ⚠️  CUIDADO: puede ser muy pesado en producción
        """
        return """
        SELECT 
            [C.O.],
            [Fecha],
            [Estado],
            [Bodega],
            [Descripcion C.O.],
            [Referencia],
            [Desc. item],
            [Talla],
            [Cantidad inv.],
            [Valor neto],
            [RANGO],
            [CLASIFICACION],
            [Fuente]
        FROM dbo.MP_VENTAS_CODE
        ORDER BY [Fecha] DESC, [C.O.], [Referencia];
        """


class StockQuery:
    """Consultas relacionadas con inventario"""
    
    @staticmethod
    def get_stock_actual() -> str:
        """
        Query para obtener stock actual desde MP_T400
        
        Columnas retornadas:
        - Referencia: Código de producto (con padding)
        - detalle ext. 2: Talla (con padding)
        - Bodega: Código numérico de bodega
        - C.O. bodega: Centro de operación
        - RANGO: BEBES / NIÑOS
        - CLASIFICACION: PRENDAS / CALZADO / etc.
        - Desc. bodega: Nombre de bodega (con padding)
        - Cant Disponible: Stock disponible
        - Cant Transito ent: Stock en tránsito de entrada
        - Existencia: Total (disponible + tránsito)
        
        Filtros aplicados en la vista:
        - Existencia <> 0
        - Cant Disponible <> 0
        """
        return """
        SELECT 
            [Referencia],
            [detalle ext. 2],
            [Bodega],
            [C.O. bodega],
            [RANGO],
            [CLASIFICACION],
            [Desc. bodega],
            [Cant Disponible],
            [Cant Transito ent],
            [Existencia]
        FROM dbo.MP_T400
        ORDER BY [Desc. bodega], [Referencia];
        """
    
    @staticmethod
    def get_stock_por_bodega(bodega: str) -> str:
        """
        Query parametrizada para obtener stock de una bodega específica
        
        Args:
            bodega: Nombre de la bodega
        """
        return f"""
        SELECT 
            [Referencia],
            [detalle ext. 2],
            [Bodega],
            [C.O. bodega],
            [RANGO],
            [CLASIFICACION],
            [Desc. bodega],
            [Cant Disponible],
            [Cant Transito ent],
            [Existencia]
        FROM dbo.MP_T400
        WHERE [Desc. bodega] = '{bodega}'
        ORDER BY [Referencia];
        """
    
    @staticmethod
    def get_stock_por_referencias(referencias: list[str]) -> str:
        """
        Query para obtener stock solo de referencias específicas
        
        Args:
            referencias: Lista de códigos de referencia
        """
        refs_str = "', '".join(referencias)
        return f"""
        SELECT 
            [Referencia],
            [detalle ext. 2],
            [Bodega],
            [C.O. bodega],
            [RANGO],
            [CLASIFICACION],
            [Desc. bodega],
            [Cant Disponible],
            [Cant Transito ent],
            [Existencia]
        FROM dbo.MP_T400
        WHERE LTRIM(RTRIM([Referencia])) IN ('{refs_str}')
        ORDER BY [Desc. bodega], [Referencia];
        """
