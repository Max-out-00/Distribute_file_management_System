import sqlite3
<<<<<<< HEAD:metadata-server/db.py
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
=======

conn = sqlite3.connect('metadata.db')

c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS FILES (
                FILE_ID INTEGER PRIMARY KEY NOT NULL,
                FILE_NAME TEXT NOT NULL,
                SIZE INTEGER NOT NULL,
                CREATED_AT TEXT NOT NULL,
                STATUS TEXT
            )''')

c.execute('''CREATE TABLE IF NOT EXISTS CHUNKS (
                CHUNK_ID INTEGER PRIMARY KEY NOT NULL,
                FILE_ID INTEGER,
                CHUNK_SIZE INTEGER,
                CHECK_SUM INTEGER,
                FOREIGN KEY (FILE_ID) REFERENCES FILES(FILE_ID)
            )''')

c.execute('''CREATE TABLE IF NOT EXISTS NODES (
                NODE_ID INTEGER PRIMARY KEY NOT NULL,
                ADDRESS VARCHAR(100) NOT NULL,
                LAST_HEARTBEAT TEXT NOT NULL,
                STATUS BOOLEAN,
                DISK_USED VARCHAR(100),
                DISK_TOTAL INTEGER
            )''')

c.execute('''CREATE TABLE IF NOT EXISTS CHUNKS_LOCATION (
                CHUNKS_ID INTEGER,
                NODE_ID INTEGER,
                STATUS BOOLEAN,
                FOREIGN KEY (CHUNKS_ID) REFERENCES CHUNKS(CHUNK_ID),
                FOREIGN KEY (NODE_ID) REFERENCES NODES(NODE_ID)
            )''')

conn.commit()
conn.close()
>>>>>>> a1eaaa35caf4ac27691fd20a7ebc64a8760cff1b:metadata_server/db.py
