"""
Database connection module for multi-database support
"""
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from typing import Dict, Any


class DatabaseConnector:
    """Manages database connections for different database types"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database connector
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config
        self.engine = None
        
    def get_connection_string(self) -> str:
        """
        Build connection string based on database type
        
        Returns:
            SQLAlchemy connection string
        """
        db_type = self.config.get('db_type', '').lower()
        host = self.config.get('host')
        port = self.config.get('port')
        database = self.config.get('database')
        username = self.config.get('username')
        password = self.config.get('password')
        
        if db_type == 'mysql':
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == 'postgresql':
            return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == 'mssql':
            # For Windows authentication, username and password can be empty
            if username and password:
                return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            else:
                return f"mssql+pyodbc://{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        
        elif db_type == 'oracle':
            return f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == 'sqlite':
            return f"sqlite:///{database}"
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def connect(self):
        """
        Create and return database engine
        
        Returns:
            SQLAlchemy engine
        """
        try:
            connection_string = self.get_connection_string()
            # Using NullPool to avoid connection pool issues during migration
            self.engine = create_engine(
                connection_string,
                poolclass=NullPool,
                echo=False
            )
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return self.engine
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    def get_engine(self):
        """Get the database engine"""
        if self.engine is None:
            return self.connect()
        return self.engine
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
