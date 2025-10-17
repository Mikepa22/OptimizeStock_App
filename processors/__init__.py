"""Módulo de procesadores de datos_processors"""
from .ventas_processor import VentasProcessor
from .stock_processor import StockProcessor

__all__ = [
    'VentasProcessor',
    'StockProcessor'
]