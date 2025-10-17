"""Módulo de acceso a base de datos_db"""
from .connection import DatabaseConnection
from .queries import VentasQuery, StockQuery

__all__ = [
    'DatabaseConnection',
    'VentasQuery',
    'StockQuery'
]