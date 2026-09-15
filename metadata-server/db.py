import sqlite3
conn = sqlite3.connect('_metadata.db_')

c = conn.cursor();

c.execute('''CREATE TABLE IF NOT EXISTS FILES (
                FILE_ID INTEGER PRIMARY KEY NOT NULL,
                FILE_NAME TEXT NOT NULL,
                SIZE INTEGER NOT NULL,
                CREATED_AT TEXT NOT NULL,
                STATUS TEXT
                )
                ''')
