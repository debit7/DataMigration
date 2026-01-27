"""
Data synchronization script - syncs only new/missing rows from source to destination
"""
import argparse
import yaml
from tqdm import tqdm
from sqlalchemy import select, insert, inspect

from db_connector import DatabaseConnector
from table_manager import TableManager


class DataSync:
    """Synchronize data between source and destination databases"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize data sync
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.source_connector = None
        self.dest_connector = None
        self.source_manager = None
        self.dest_manager = None
        self.primary_key_column = None
        
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
        print("\n=== Data Synchronization Tool ===")
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
    
    def get_primary_key_column(self, table):
        """
        Get the primary key column name from a table
        
        Args:
            table: SQLAlchemy Table object
            
        Returns:
            Primary key column name or None
        """
        for column in table.columns:
            if column.primary_key:
                return column.name
        return None
    
    def analyze_sync_status(self):
        """Analyze what needs to be synced"""
        print("\nAnalyzing sync status...")
        
        source_count = self.source_manager.get_row_count()
        dest_count = self.dest_manager.get_row_count()
        
        print(f"Source table row count: {source_count:,}")
        print(f"Destination table row count: {dest_count:,}")
        
        if source_count > dest_count:
            rows_to_sync = source_count - dest_count
            print(f"\n→ Need to sync {rows_to_sync:,} rows from source to destination")
            return True, rows_to_sync, source_count, dest_count
        elif source_count == dest_count:
            print("\n✓ Tables are already in sync (same row count)")
            return False, 0, source_count, dest_count
        else:
            print(f"\n⚠ Warning: Destination has MORE rows than source!")
            print(f"   Destination: {dest_count:,} | Source: {source_count:,}")
            print(f"   No sync needed (destination has {dest_count - source_count:,} extra rows)")
            return False, 0, source_count, dest_count
    
    def prepare_destination_table(self):
        """Prepare destination table (create if needed)"""
        print("\nPreparing destination table...")
        
        # Get source table schema
        source_table = self.source_manager.get_table_object()
        if source_table is None:
            raise RuntimeError(f"Source table {self.config['source']['table']} does not exist!")
        
        # Get primary key column
        self.primary_key_column = self.get_primary_key_column(source_table)
        if self.primary_key_column:
            print(f"✓ Detected primary key column: {self.primary_key_column}")
        else:
            print("⚠ Warning: No primary key detected - will sync based on row offset")
        
        # Create destination table if needed
        if self.config['migration'].get('create_table_if_missing', True):
            if not self.dest_manager.table_exists():
                print(f"Creating table {self.config['destination']['table']}...")
                self.dest_manager.create_table_from_source(source_table)
            else:
                print(f"✓ Table {self.config['destination']['table']} already exists")
        
        print("✓ Destination table ready")
    
    def sync_data(self, rows_to_sync: int, source_count: int, dest_count: int):
        """
        Sync missing data from source to destination
        
        Args:
            rows_to_sync: Number of rows to sync
            source_count: Total rows in source
            dest_count: Total rows in destination
        """
        batch_size = self.config['migration'].get('batch_size', 1000)
        show_progress = self.config['migration'].get('show_progress', True)
        
        # Get source and destination tables
        source_table = self.source_manager.get_table_object()
        dest_table = self.dest_manager.get_table_object()
        
        print(f"\nStarting sync with batch size: {batch_size:,}")
        print(f"Syncing rows from position {dest_count:,} to {source_count:,}")
        
        # Initialize progress bar
        pbar = None
        if show_progress:
            pbar = tqdm(total=rows_to_sync, unit=' rows', unit_scale=True)
        
        try:
            source_engine = self.source_connector.get_engine()
            dest_engine = self.dest_connector.get_engine()
            
            # Start from where destination left off
            offset = dest_count
            synced_count = 0
            
            while offset < source_count:
                # Calculate batch size (don't exceed remaining rows)
                current_batch_size = min(batch_size, source_count - offset)
                
                # Fetch batch from source (starting from offset)
                with source_engine.connect() as source_conn:
                    # If we have a primary key, order by it for consistency
                    if self.primary_key_column:
                        query = (
                            select(source_table)
                            .order_by(source_table.c[self.primary_key_column])
                            .limit(current_batch_size)
                            .offset(offset)
                        )
                    else:
                        query = select(source_table).limit(current_batch_size).offset(offset)
                    
                    result = source_conn.execute(query)
                    rows = result.fetchall()
                    
                    if not rows:
                        break
                    
                    # Convert rows to dictionaries
                    batch_data = []
                    for row in rows:
                        row_dict = dict(zip(source_table.columns.keys(), row))
                        batch_data.append(row_dict)
                    
                    # Insert batch into destination
                    if batch_data:
                        with dest_engine.connect() as dest_conn:
                            dest_conn.execute(insert(dest_table), batch_data)
                            dest_conn.commit()
                        
                        synced_count += len(batch_data)
                        
                        if pbar:
                            pbar.update(len(batch_data))
                
                offset += current_batch_size
            
            if pbar:
                pbar.close()
            
            print(f"\n✓ Sync completed! Synced {synced_count:,} new rows")
            
        except Exception as e:
            if pbar:
                pbar.close()
            raise RuntimeError(f"Sync failed: {str(e)}")
    
    def verify_sync(self):
        """Verify that sync was successful"""
        print("\nVerifying sync...")
        
        source_count = self.source_manager.get_row_count()
        dest_count = self.dest_manager.get_row_count()
        
        print(f"Source count: {source_count:,}")
        print(f"Destination count: {dest_count:,}")
        
        if source_count == dest_count:
            print("✓ Verification: Tables are now in sync!")
        else:
            diff = abs(source_count - dest_count)
            print(f"⚠ Warning: Tables still have {diff:,} row difference")
    
    def run(self):
        """Execute the complete sync process"""
        try:
            # Connect to databases
            self.connect_databases()
            
            # Analyze what needs syncing
            needs_sync, rows_to_sync, source_count, dest_count = self.analyze_sync_status()
            
            if not needs_sync:
                print("\n=== No Sync Needed ===\n")
                return
            
            # Prepare destination
            self.prepare_destination_table()
            
            # Sync data
            self.sync_data(rows_to_sync, source_count, dest_count)
            
            # Verify sync
            self.verify_sync()
            
            print("\n=== Sync Complete ===\n")
            
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
    parser = argparse.ArgumentParser(description="Database Synchronization Tool")
    parser.add_argument(
        '--config',
        '-c',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Run sync
    sync = DataSync(config_path=args.config)
    sync.run()


if __name__ == "__main__":
    main()
