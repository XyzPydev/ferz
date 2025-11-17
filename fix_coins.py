import sqlite3
import shutil
import sys
import os
import time

def backup_db(path):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    bak = f"{path}.backup.{timestamp}"
    shutil.copy2(path, bak)
    return bak

def main(db_path):
    if not os.path.exists(db_path):
        print(f"Файл не знайдено: {db_path}")
        return 1

    print("Створюю резервну копію...")
    bak_file = backup_db(db_path)
    print("Резервна копія:", bak_file)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Перевірка: чи є таблиця users і чи є колонка coins
    cur.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cur.fetchall()]
    if not cols:
        print("Таблиця 'users' не знайдена.")
        conn.close()
        return 1
    if "coins" not in cols:
        print("Колонка 'coins' не знайдена в таблиці 'users'.")
        conn.close()
        return 1

    # Показати приклади перед змінами
    print("\nПриклади до змін (перші 10):")
    for r in cur.execute("SELECT rowid, coins FROM users LIMIT 10"):
        print(r)

    # Підрахуємо кількість рядків, що мають ненульове значення coins (для інфо)
    before_count = cur.execute("SELECT COUNT(*) FROM users WHERE coins IS NOT NULL").fetchone()[0]
    print(f"\nРядків з coins IS NOT NULL перед зміною: {before_count}")

    # Виконуємо оновлення:
    # - COALESCE(coins,0) щоб уникнути NULL (але WHERE не-null обрізає більшість)
    # - ділимо на 1_000_000_000.0 (float) щоб уникнути цілої арифметики
    # - ROUND(... ) округлює до цілих
    # - CAST(... AS INTEGER) зберігає як int
    cur.execute("BEGIN")
    cur.execute("""
        UPDATE users
        SET coins = CAST(ROUND(COALESCE(coins, 0) / 1000000000.0) AS INTEGER)
        WHERE coins IS NOT NULL
    """)
    # отримати кількість змінених рядків для останнього запиту
    changed = cur.execute("SELECT changes()").fetchone()[0]
    conn.commit()

    print(f"\nОновлено рядків: {changed}")

    # Показати приклади після змін
    print("\nПриклади після змін (перші 10):")
    for r in cur.execute("SELECT rowid, coins FROM users LIMIT 10"):
        print(r)

    conn.close()
    print("\nГотово.")
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Використання: python fix_coins.py \"unkeuiiiypppee (1).db\"")
        sys.exit(1)
    db_path = sys.argv[1]
    sys.exit(main(db_path))