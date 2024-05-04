import sqlite3
import os

db_path = os.path.realpath('../data/beatstream.sqlite')
conn = sqlite3.connect(db_path)

cursor = conn.cursor()

cursor.execute("SELECT id, current_song FROM users LIMIT 10")
print(cursor.fetchall())

cursor.execute("SELECT * FROM recommendations ORDER BY id DESC LIMIT 10")
print(cursor.fetchall())

conn.close()