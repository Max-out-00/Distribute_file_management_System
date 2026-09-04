import sqlite3
from pathlib import Path


DATABASE = Path(__file__).parent / "metadata.db"
SCHEMA = Path(__file__).parent.parent / "docs" / "schema.sql"


def get_connection():
    return sqlite3.connect(DATABASE)


def initialize_database():
    connection = get_connection()

    with open(SCHEMA, "r") as file:
        schema = file.read()

    connection.executescript(schema)
    connection.commit()
    connection.close()


if __name__ == "__main__":
    initialize_database()
    print("Database initialized successfully.")