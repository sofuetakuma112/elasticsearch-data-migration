import sqlite3

# SQLiteデータベースへの接続
conn = sqlite3.connect("example.db")

# カーソルを取得
cursor = conn.cursor()

# テーブルの作成
cursor.execute(
    """CREATE TABLE IF NOT EXISTS old_elasticsearch (
                    number TEXT,
                    TEMP REAL NULL,
                    utctime TEXT NULL,
                    RH REAL NULL,
                    ip TEXT NULL,
                    PPM REAL NULL,
                    Temperature REAL NULL,
                    JPtime TEXT,
                    data TEXT NULL,
                    index_name TEXT NULL,
                    ms TEXT NULL,
                    PRIMARY KEY (number, JPtime)
                )"""
)

# 変更をコミット
conn.commit()

# 接続を閉じる
conn.close()
