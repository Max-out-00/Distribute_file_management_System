
import sqlite3
from pathlib import Path


# Database file used for development.
# Keep the existing metadata.db untouched for now.
DATABASE = Path(__file__).resolve().with_name(
    "metadata_runtime.db"
)

# Central database schema for the project.
SCHEMA = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "schema.sql"
)


def get_connection():
    """Open a connection to the metadata database."""
    connection = sqlite3.connect(DATABASE)

    # Access query results using column names.
    connection.row_factory = sqlite3.Row

    # Enforce foreign-key relationships in SQLite.
    connection.execute("PRAGMA foreign_keys = ON")

    return connection


def initialize_database():
    """Create the database tables from schema.sql."""
    if not SCHEMA.is_file():
        raise FileNotFoundError(
            f"Schema file not found: {SCHEMA}"
        )

    with SCHEMA.open("r", encoding="utf-8") as schema_file:
        schema_sql = schema_file.read()

    connection = get_connection()

    try:
        connection.executescript(schema_sql)
        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    initialize_database()
    print(
        f"Database initialized successfully: {DATABASE}"
    )
