import sqlite3

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