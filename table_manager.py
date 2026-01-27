"""
Table management module for schema extraction and table creation
"""
from sqlalchemy import Table, MetaData, Column, inspect, text
from sqlalchemy.engine import Engine
from typing import Optional


class TableManager:
    """Manages table operations including schema extraction and creation"""
    
    def __init__(self, engine: Engine, table_name: str):
        """
        Initialize table manager
        
        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table
        """
        self.engine = engine
        self.table_name = table_name
        self.metadata = MetaData()
        
    def table_exists(self) -> bool:
        """
        Check if table exists in database
        
        Returns:
            True if table exists, False otherwise
        """
        inspector = inspect(self.engine)
        return self.table_name in inspector.get_table_names()
    
    def get_table_object(self) -> Optional[Table]:
        """
        Get SQLAlchemy Table object by reflecting from database
        
        Returns:
            Table object or None if table doesn't exist
        """
        if not self.table_exists():
            return None
        
        try:
            table = Table(
                self.table_name,
                self.metadata,
                autoload_with=self.engine
            )
            return table
        except Exception as e:
            raise RuntimeError(f"Failed to reflect table {self.table_name}: {str(e)}")
    
    def get_row_count(self) -> int:
        """
        Get the number of rows in the table
        
        Returns:
            Row count
        """
        if not self.table_exists():
            return 0
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name}")
                )
                count = result.scalar()
                return count
        except Exception as e:
            raise RuntimeError(f"Failed to get row count for {self.table_name}: {str(e)}")
    
    def create_table_from_source(self, source_table: Table):
        """
        Create table in destination database based on source table schema
        
        Args:
            source_table: Source table object to copy schema from
        """
        if self.table_exists():
            print(f"Table {self.table_name} already exists in destination")
            return
        
        try:
            # Create new table with same schema
            new_table = Table(
                self.table_name,
                self.metadata,
                *[self._clone_column(col) for col in source_table.columns],
                # Note: Primary keys and indexes are included in column definitions
                # Foreign keys are intentionally not copied to avoid dependency issues
            )
            
            # Create table in database
            self.metadata.create_all(self.engine)
            print(f"✓ Table {self.table_name} created successfully")
            
        except Exception as e:
            raise RuntimeError(f"Failed to create table {self.table_name}: {str(e)}")
    
    def _clone_column(self, column: Column) -> Column:
        """
        Clone a column definition (without foreign key constraints)
        
        Args:
            column: Source column
            
        Returns:
            Cloned column
        """
        # Create new column with same properties but no foreign keys
        return Column(
            column.name,
            column.type,
            primary_key=column.primary_key,
            nullable=column.nullable,
            default=column.default,
            autoincrement=column.autoincrement,
        )
    
    def truncate_table(self):
        """Truncate (empty) the table"""
        if not self.table_exists():
            return
        
        try:
            with self.engine.connect() as conn:
                # Different databases have different truncate syntax
                db_name = self.engine.dialect.name
                
                if db_name == 'sqlite':
                    # SQLite doesn't support TRUNCATE
                    conn.execute(text(f"DELETE FROM {self.table_name}"))
                else:
                    conn.execute(text(f"TRUNCATE TABLE {self.table_name}"))
                
                conn.commit()
                print(f"✓ Table {self.table_name} truncated")
                
        except Exception as e:
            raise RuntimeError(f"Failed to truncate table {self.table_name}: {str(e)}")
