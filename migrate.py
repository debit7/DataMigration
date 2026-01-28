"""
Main data migration script
"""
import argparse
import yaml
from pathlib import Path
from tqdm import tqdm
from sqlalchemy import select, insert

from db_connector import DatabaseConnector
from table_manager import TableManager


class DataMigration:
    """Main data migration class"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize data migration
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.source_connector = None
        self.dest_connector = None
        self.source_manager = None
        self.dest_manager = None
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            raise RuntimeError(f"Failed to load config from {config_path}: {str(e)}")
    
    def connect_databases(self):
        """Establish connections to source and destination databases"""
        print("\n=== Data Migration Tool ===")
        print(f"Source: {self.config['source']['db_type']}://{self.config['source']['host']}:{self.config['source']['port']}/{self.config['source']['database']}.{self.config['source']['table']}")
        print(f"Destination: {self.config['destination']['db_type']}://{self.config['destination']['host']}:{self.config['destination']['port']}/{self.config['destination']['database']}.{self.config['destination']['table']}")
        
        print("\nChecking connections...")
        
        # Connect to source
        self.source_connector = DatabaseConnector(self.config['source'])
        self.source_connector.connect()
        print("✓ Source connection successful")
        
        # Connect to destination
        self.dest_connector = DatabaseConnector(self.config['destination'])
        self.dest_connector.connect()
        print("✓ Destination connection successful")
        
        # Initialize table managers
        self.source_manager = TableManager(
            self.source_connector.get_engine(),
            self.config['source']['table']
        )
        
        self.dest_manager = TableManager(
            self.dest_connector.get_engine(),
            self.config['destination']['table']
        )
    
    def show_row_counts(self):
        """Display row counts for source and destination tables"""
        print()
        source_count = self.source_manager.get_row_count()
        dest_count = self.dest_manager.get_row_count()
        
        print(f"Source table row count: {source_count:,}")
        print(f"Destination table row count: {dest_count:,}")
        
        return source_count, dest_count
    
    def prepare_destination_table(self):
        """Prepare destination table (create if needed, truncate if configured)"""
        print("\nPreparing destination table...")
        
        # Get source table schema
        source_table = self.source_manager.get_table_object()
        if source_table is None:
            raise RuntimeError(f"Source table {self.config['source']['table']} does not exist!")
        
        # Create destination table if needed
        if self.config['migration'].get('create_table_if_missing', True):
            if not self.dest_manager.table_exists():
                print(f"Creating table {self.config['destination']['table']}...")
                self.dest_manager.create_table_from_source(source_table)
            else:
                print(f"✓ Table {self.config['destination']['table']} already exists")
        
        # Truncate if configured
        if self.config['migration'].get('truncate_destination', False):
            print("Truncating destination table...")
            self.dest_manager.truncate_table()
        
        print("✓ Destination table ready")
    
    def migrate_data(self):
        """Migrate data from source to destination in batches"""
        batch_size = self.config['migration'].get('batch_size', 1000)
        show_progress = self.config['migration'].get('show_progress', True)
        
        # Get source table
        source_table = self.source_manager.get_table_object()
        
        # Get destination table
        dest_table = self.dest_manager.get_table_object()
        
        # Count total rows
        total_rows = self.source_manager.get_row_count()
        
        if total_rows == 0:
            print("\nNo data to migrate (source table is empty)")
            return
        
        print(f"\nStarting migration with batch size: {batch_size:,}")
        
        # Initialize progress bar
        pbar = None
        if show_progress:
            pbar = tqdm(total=total_rows, unit=' rows', unit_scale=True)
        
        try:
            source_engine = self.source_connector.get_engine()
            dest_engine = self.dest_connector.get_engine()
            
            offset = 0
            migrated_count = 0
            
            while offset < total_rows:
                # Fetch batch from source
                with source_engine.connect() as source_conn:
                    # Add ORDER BY for SQL Server compatibility with OFFSET/LIMIT
                    # Use primary key if available, otherwise use all columns
                    primary_keys = [col for col in source_table.columns if col.primary_key]
                    if primary_keys:
                        query = select(source_table).order_by(*primary_keys).limit(batch_size).offset(offset)
                    else:
                        # If no primary key, order by first column
                        query = select(source_table).order_by(source_table.columns[0]).limit(batch_size).offset(offset)
                    
                    result = source_conn.execute(query)
                    rows = result.fetchall()
                    
                    if not rows:
                        break
                    
                    # Convert rows to dictionaries
                    batch_data = []
                    for row in rows:
                        # Convert row to dictionary using column names
                        row_dict = dict(zip(source_table.columns.keys(), row))
                        batch_data.append(row_dict)
                    
                    # Insert batch into destination
                    if batch_data:
                        with dest_engine.connect() as dest_conn:
                            dest_conn.execute(insert(dest_table), batch_data)
                            dest_conn.commit()
                        
                        migrated_count += len(batch_data)
                        
                        if pbar:
                            pbar.update(len(batch_data))
                
                offset += batch_size
            
            if pbar:
                pbar.close()
            
            print(f"\n✓ Migration completed! Migrated {migrated_count:,} rows")
            
        except Exception as e:
            if pbar:
                pbar.close()
            raise RuntimeError(f"Migration failed: {str(e)}")
    
    def verify_migration(self, initial_source_count: int):
        """Verify that migration was successful"""
        print("\nVerifying migration...")
        
        final_source_count = self.source_manager.get_row_count()
        final_dest_count = self.dest_manager.get_row_count()
        
        print(f"Final source count: {final_source_count:,}")
        print(f"Final destination count: {final_dest_count:,}")
        
        # Check if counts match (source count might have changed during migration)
        if final_dest_count >= initial_source_count:
            print("✓ Verification: All rows migrated successfully")
        else:
            print(f"⚠ Warning: Expected at least {initial_source_count:,} rows, found {final_dest_count:,}")
    
    def run(self):
        """Execute the complete migration process"""
        try:
            # Connect to databases
            self.connect_databases()
            
            # Show initial counts
            source_count, dest_count = self.show_row_counts()
            
            # Prepare destination
            self.prepare_destination_table()
            
            # Migrate data
            self.migrate_data()
            
            # Verify migration
            self.verify_migration(source_count)
            
            print("\n=== Migration Complete ===\n")
            
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            raise
        finally:
            # Close connections
            if self.source_connector:
                self.source_connector.close()
            if self.dest_connector:
                self.dest_connector.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Database Migration Tool")
    parser.add_argument(
        '--config',
        '-c',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Run migration
    migration = DataMigration(config_path=args.config)
    migration.run()


if __name__ == "__main__":
    main()
