"""
Configuración de conexión a SQL Server
"""
from dataclasses import dataclass
from pathlib import Path
import os
from typing import Optional

@dataclass
class DatabaseConfig:
    """Configuración de conexión a base de datos"""
    server: str
    database: str
    driver: str = "{ODBC Driver 17 for SQL Server}"
    trusted_connection: bool = True
    username: Optional[str] = None
    password: Optional[str] = None
    
    @classmethod
    def from_env(cls, env_file: Optional[Path] = None):
        """
        Carga configuración desde variables de entorno o archivo .env
        
        Variables esperadas:
        - SQL_SERVER: nombre del servidor
        - SQL_DATABASE: nombre de la base de datos
        - SQL_DRIVER: driver ODBC (opcional)
        - SQL_TRUSTED: 'true' para Windows Auth, 'false' para SQL Auth
        - SQL_USER: usuario (solo si SQL_TRUSTED=false)
        - SQL_PASSWORD: contraseña (solo si SQL_TRUSTED=false)
        """
        from dotenv import load_dotenv
    
        # Cargar .env si existe
        if env_file is None:
            env_file = Path('.env')
    
        if env_file.exists():
            load_dotenv(env_file, override=True)
        else:
        # Intentar cargar .env sin especificar ruta (busca automáticamente)
            load_dotenv(override=True)
        
        trusted = os.getenv("SQL_TRUSTED", "true").lower() == "true"
        
        return cls(
            server=os.getenv("SQL_SERVER", "localhost"),
            database=os.getenv("SQL_DATABASE", "SIESA"),
            driver=os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
            trusted_connection=trusted,
            username=os.getenv("SQL_USER") if not trusted else None,
            password=os.getenv("SQL_PASSWORD") if not trusted else None
        )
    
    def connection_string(self) -> str:
        """Genera connection string para pyodbc"""
        base = (f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};")
        
        if self.trusted_connection:
            conn_str = base + "Trusted_Connection=yes;"
        else:
            if not self.username or not self.password:
                raise ValueError("SQL_USER y SQL_PASSWORD requeridos cuando SQL_TRUSTED=false")
            conn_str = base + f"UID={self.username};PWD={self.password};"
        
        # Para ODBC Driver 18 con AWS RDS o Azure SQL
        if "Driver 18" in self.driver:
            conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
        
        return conn_str
        
    def __repr__(self) -> str:
        """Representación segura sin credenciales"""
        auth = "Windows Auth" if self.trusted_connection else f"SQL Auth (user={self.username})"
        return f"DatabaseConfig(server={self.server}, database={self.database}, {auth})"
