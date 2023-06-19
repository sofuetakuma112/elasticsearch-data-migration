import sqlite3
import math

from utils.constants import SQLITE_DIR_FULL_PATH

conn = sqlite3.connect(f"{SQLITE_DIR_FULL_PATH}/co2.db")
cursor = conn.cursor()

# データベース内のNaNの数をカウント
cursor.execute("SELECT COUNT(*) FROM co2 WHERE TEMP != TEMP")
nan_count = cursor.fetchone()[0]

print("NaNの数:", nan_count)

conn.close()
