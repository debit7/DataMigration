# Data Migration Tool

A flexible Python tool for migrating data between databases with batch processing and automatic table creation.

## Features

- **Multi-Database Support**: Works with MySQL, PostgreSQL, MS SQL Server, Oracle, and SQLite
- **Batch Processing**: Migrates data in configurable batches to handle large datasets efficiently
- **Auto Table Creation**: Automatically creates destination tables if they don't exist
- **Data Validation**: Shows source and destination row counts before and after migration
- **Progress Tracking**: Real-time progress bar during migration
- **Configurable**: Easy YAML-based configuration

## Installation

1. Clone or download this project
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install the appropriate database driver for your database type (see requirements.txt)

## Configuration

Edit `config.yaml` to set your source and destination database connections:

```yaml
source:
  db_type: "mysql"  # mysql, postgresql, mssql, oracle, sqlite
  host: "localhost"
  port: 3306
  database: "source_db"
  username: "source_user"
  password: "source_password"
  table: "source_table"

destination:
  db_type: "mysql"
  host: "localhost"
  port: 3306
  database: "dest_db"
  username: "dest_user"
  password: "dest_password"
  table: "dest_table"

migration:
  batch_size: 1000
  create_table_if_missing: true
  truncate_destination: false
  show_progress: true
```

## Usage

### Full Migration

Run a complete migration (migrates all data):

```bash
python migrate.py
```

### Incremental Sync

Run sync to migrate only new/missing rows (if source has more data than destination):

```bash
python sync.py
```

You can also specify a custom config file:

```bash
python migrate.py --config my_config.yaml
python sync.py --config my_config.yaml
```

## Example Output

### Full Migration (migrate.py)

```
=== Data Migration Tool ===
Source: mysql://localhost:3306/source_db.source_table
Destination: mysql://localhost:3306/dest_db.dest_table

Checking connections...
✓ Source connection successful
✓ Destination connection successful

Source table row count: 125,430
Destination table row count: 0

Creating destination table (if missing)...
✓ Table structure verified

Starting migration with batch size: 1000
Migrating: 100%|██████████| 125430/125430 [00:45<00:00, 2787.33 rows/s]

Migration completed!
Final destination count: 125,430
Verification: ✓ All rows migrated successfully
```

### Incremental Sync (sync.py)

```
=== Data Synchronization Tool ===
Source: mssql://localhost:1433/source_db.orders
Destination: mssql://localhost:1433/dest_db.orders

Checking connections...
✓ Source connection successful
✓ Destination connection successful

Analyzing sync status...
Source table row count: 15,250
Destination table row count: 12,000

→ Need to sync 3,250 rows from source to destination

Preparing destination table...
✓ Detected primary key column: id
✓ Table orders already exists
✓ Destination table ready

Starting sync with batch size: 1000
Syncing rows from position 12,000 to 15,250
Syncing: 100%|██████████| 3250/3250 [00:03<00:00, 1083.33 rows/s]

✓ Sync completed! Synced 3,250 new rows

Verifying sync...
Source count: 15,250
Destination count: 15,250
✓ Verification: Tables are now in sync!

=== Sync Complete ===
```

## Security Note

For production use:
- Store credentials in environment variables or a secure vault
- Use encrypted connections
- Follow your organization's security best practices

## License

MIT License

## Commands to setup environment
 python -m venv venv 
 .\venv\Scripts\Activate.ps1
 pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
 
