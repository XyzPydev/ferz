import sqlite3
import shutil
import sys
import os
import time
from decimal import Decimal, ROUND_HALF_UP

def backup_db(path):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    bak = f"{path}.backup.{timestamp}"
    shutil.copy2(path, bak)
    return bak

def to_int_round_half_up(value):
    """
    Безпечне округлення до цілих: використовує Decimal з ROUND_HALF_UP.
    Працює з float, int та рядками.
    """
    if value is None:
        return None
    # Конвертуємо через str щоб зменшити вплив бінарного представлення float
    try:
        d = Decimal(str(value))
    except Exception:
        # як крайній випадок — спробуємо прямо Decimal(value)
        d = Decimal(value)
    return int(d.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def main(db_path):
    if not os.path.exists(db_path):
        print(f"Файл не знайдено: {db_path}")
        return 1

    print("Створюю резервну копію...")
    bak = backup_db(db_path)
    print("Резервна копія створена:", bak)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Перевірки
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
    if cur.fetchone() is None:
        print("Таблиця 'users' не знайдена.")
        conn.close()
        return 1

    cur.execute("PRAGMA table_info(users);")
    cols = [r[1] for r in cur.fetchall()]
    if "coins" not in cols:
        print("Колонка 'coins' не знайдена в таблиці 'users'.")
        conn.close()
        return 1

    # Показати приклади до змін
    print("\nПриклади до змін (up to 10):")
    for r in cur.execute("SELECT rowid, coins FROM users LIMIT 10"):
        print(r)

    # Виберемо всі рядки, де coins IS NOT NULL
    rows = list(cur.execute("SELECT rowid, coins FROM users WHERE coins IS NOT NULL"))
    print(f"\nЗнайдено рядків з coins IS NOT NULL: {len(rows)}")

    if not rows:
        print("Немає значень для оновлення.")
        conn.close()
        return 0

    # Починаємо транзакцію і оновлюємо построково, записуючи явне INTEGER
    updated = 0
    conn.execute("BEGIN")
    for rowid, coins in rows:
        try:
            new_int = to_int_round_half_up(coins)
        except Exception as e:
            print(f"Пропускаю rowid={rowid} через помилку перетворення: {e}")
            continue

        # Якщо вже правильне значення — пропускаємо
        if (isinstance(coins, int) and coins == new_int) or (str(coins) == str(new_int)):
            continue

        cur.execute("UPDATE users SET coins = ? WHERE rowid = ?", (new_int, rowid))
        updated += 1

    conn.commit()
    print(f"\nКількість оновлених рядків: {updated}")

    # Показати приклади після змін
    print("\nПриклади після змін (up to 10):")
    for r in cur.execute("SELECT rowid, coins FROM users LIMIT 10"):
        print(r)

    conn.close()
    print("\nГотово.")
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Використання: python fix_coins_to_int.py \"unkeuiiiypppee (1).db\"")
        sys.exit(1)
    sys.exit(main(sys.argv[1]))