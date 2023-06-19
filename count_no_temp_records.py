import sqlite3

from utils.constants import SQLITE_DIR_FULL_PATH

conn = sqlite3.connect(f"{SQLITE_DIR_FULL_PATH}/co2.db")
cursor = conn.cursor()

# クエリを実行して一致するレコードの数を取得
query = """
SELECT COUNT(*) FROM co2
WHERE TEMP IS NULL
"""
cursor.execute(query)
result = cursor.fetchone()[0]

# 結果を表示
print("一致するレコードの数:", result)

# コネクションを閉じる
cursor.close()
conn.close()