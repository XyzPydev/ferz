import asyncio
import json
import math
import random
import hashlib
import re
import time
import random as rnd
import string
from datetime import datetime, timedelta, UTC, timezone
from decimal import Decimal
from hashlib import md5
from typing import Optional, Union
from zoneinfo import ZoneInfo
import uuid
import aiosqlite
import pytz
from aiogram import Bot, Dispatcher, types
from aiogram import F
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.fsm.state import StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

API_TOKEN = "8423747322:AAGwYPPEob82mQJsbYL02dJMwDXE-34JP94"

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

DB_PATH = "unkeuiiiypppee (1).db"

BANNED_FILE = "banned.json"
FARM_DB_PATH = "farms.db"


# ================== ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ==================
async def init_db():
    # База для ферм
    async with aiosqlite.connect(FARM_DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS farms (
                user_id INTEGER PRIMARY KEY,
                farm_type INTEGER,
                level INTEGER,
                current_energy INTEGER,
                max_energy INTEGER,
                last_farm_time INTEGER,
                total_farmed_time REAL,
                pending_fezcoin INTEGER,
                purchase_time INTEGER
            )
        """)
        await db.commit()

    # Основная база (users и остальные таблицы)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")

        # Таблица пользователей (без изменений)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                coins INTEGER DEFAULT 0,
                last_bonus TEXT,
                win_amount INTEGER DEFAULT 0,
                lose_amount INTEGER DEFAULT 0,
                fezcoin REAL DEFAULT 0.0,
                fezcoin_sold REAL DEFAULT 0.0,
                last_farm_collect TEXT,
                upgrades INTEGER DEFAULT 0,
                total_farmed_fez REAL DEFAULT 0.0,
                firewall INTEGER DEFAULT 0,
                last_firewall_activation TEXT,
                bank_amount REAL DEFAULT 0.0,
                last_interest TEXT,
                status INTEGER DEFAULT 0,
                verified INTEGER DEFAULT 0,
                hidden INTEGER DEFAULT 0,
                last_box TEXT,
                referrer_id INTEGER,
                referral_earnings INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                last_active TEXT,
                escrow REAL DEFAULT 0.0,
                boss_experience INTEGER DEFAULT 0,
                total_exchanged_exp INTEGER DEFAULT 0,
                total_gg_from_exp INTEGER DEFAULT 0,
                subscribed INTEGER DEFAULT 0,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
        """)

        # Проверка и добавление недостающих столбцов в users
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in await cursor.fetchall()]
        if 'coins' not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN coins INTEGER DEFAULT 0")
        if 'fezcoin' not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN fezcoin REAL DEFAULT 0.0")

        # Остальные таблицы (без изменений)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS coin_game (
                user_id INTEGER PRIMARY KEY,
                bet INTEGER DEFAULT 0,
                last_played TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS coin_spam (
                user_id INTEGER PRIMARY KEY,
                last_click REAL NOT NULL,
                click_count INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS fez_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER,
                buyer_id INTEGER,
                amount REAL NOT NULL,
                price INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT DEFAULT 'open',
                order_type TEXT NOT NULL CHECK (order_type IN ('sell', 'buy')),
                completed_at TEXT,
                FOREIGN KEY (seller_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (buyer_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                promo_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                coins INTEGER DEFAULT 0,
                fezcoin REAL DEFAULT 0.0,
                max_activations INTEGER NOT NULL,
                activations INTEGER DEFAULT 0,
                creator_id INTEGER,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                FOREIGN KEY (creator_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promo_activations (
                promo_id INTEGER,
                user_id INTEGER,
                activated_at TEXT NOT NULL,
                PRIMARY KEY (promo_id, user_id),
                FOREIGN KEY (promo_id) REFERENCES promo_codes(promo_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS deposits (
                user_id INTEGER,
                deposit_id INTEGER,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL,
                last_interest TEXT,
                interest_rate REAL DEFAULT 0.0,
                PRIMARY KEY (user_id, deposit_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS duels (
                duel_id INTEGER PRIMARY KEY AUTOINCREMENT,
                challenger_id INTEGER NOT NULL,
                opponent_id INTEGER,
                stake INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                message_id INTEGER,
                chat_id INTEGER,
                created_at TEXT NOT NULL,
                result TEXT,
                challenger_choice TEXT,
                opponent_choice TEXT,
                completed_at TEXT,
                FOREIGN KEY (challenger_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (opponent_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bosses (
                boss_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                hp_total INTEGER NOT NULL,
                hp_current INTEGER DEFAULT -1,
                created_at TEXT NOT NULL,
                start_time TEXT,
                active INTEGER DEFAULT 1
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS player_boss_damage (
                user_id INTEGER,
                boss_id INTEGER,
                damage_dealt INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, boss_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (boss_id) REFERENCES bosses(boss_id) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS weapons (
                weapon_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                cost_fez REAL NOT NULL,
                base_damage INTEGER NOT NULL,
                bonus_damage INTEGER NOT NULL,
                bonus_chance REAL DEFAULT 0.1
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS player_weapons (
                user_id INTEGER,
                weapon_id INTEGER,
                quantity INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, weapon_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (weapon_id) REFERENCES weapons(weapon_id) ON DELETE CASCADE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rate (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                rate REAL NOT NULL DEFAULT 1.0
            )
        """)

        # Инициализация exchange_rate
        cursor = await db.execute("SELECT COUNT(*) FROM exchange_rate")
        if (await cursor.fetchone())[0] == 0:
            await db.execute("INSERT INTO exchange_rate (id, rate) VALUES (1, 1.0)")

        # Индексы
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fez_orders_seller_id ON fez_orders(seller_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fez_orders_buyer_id ON fez_orders(buyer_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fez_orders_status ON fez_orders(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fez_orders_order_type ON fez_orders(order_type)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_promo_codes_name ON promo_codes(name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_user_id ON promo_activations(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_duels_challenger_id ON duels(challenger_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_duels_opponent_id ON duels(opponent_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_duels_status ON duels(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bosses_active ON bosses(active)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_player_boss_damage_user_boss ON player_boss_damage(user_id, boss_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_weapons_category ON weapons(category)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_player_weapons_user ON player_weapons(user_id)")

        # Заполнение оружия по умолчанию
        cursor = await db.execute("SELECT COUNT(*) FROM weapons")
        if (await cursor.fetchone())[0] == 0:
            weapons = [
                ('Knife', 'Light Arms', 20.0, 20, 60),
                ('Slingshot', 'Light Arms', 60.0, 60, 100),
                ('Pistol', 'Light Arms', 100.0, 100, 140),
                ('Rifle', 'Light Arms', 200.0, 200, 300),
                ('Grenade', 'Light Arms', 300.0, 400, 700),
                ('Machine Gun', 'Heavy Artillery', 400.0, 600, 900),
                ('Rocket Launcher', 'Heavy Artillery', 600.0, 900, 1400),
                ('Cannon', 'Heavy Artillery', 1000.0, 1400, 2000),
                ('Tank', 'Heavy Artillery', 1600.0, 2000, 3000),
                ('Bomber', 'Heavy Artillery', 2400.0, 3000, 4000),
                ('Laser Gun', 'Exotic Weapons', 3000.0, 4000, 6000),
                ('Plasma Cannon', 'Exotic Weapons', 4000.0, 6000, 9000),
                ('Nuclear Missile', 'Exotic Weapons', 6000.0, 9000, 14000),
                ('Death Ray', 'Exotic Weapons', 10000.0, 14000, 20000),
                ('Black Hole Generator', 'Exotic Weapons', 16000.0, 20000, 30000)
            ]
            for w in weapons:
                await db.execute("""
                    INSERT INTO weapons (name, category, cost_fez, base_damage, bonus_damage)
                    VALUES (?, ?, ?, ?, ?)
                """, w)

        await db.commit()

# Middleware для проверки бана и подписки


# Middleware для проверки бана и подписки
def init_banned_file():
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            json.load(f)
    except FileNotFoundError:
        with open(BANNED_FILE, "w", encoding="utf-8") as f:
            json.dump({"banned": []}, f, ensure_ascii=False, indent=2)

# Middleware для проверки бана, подписки и регистрации
class BannedUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        # Поддержка как Message, так и CallbackQuery
        if not isinstance(event, (Message, CallbackQuery)):
            return await handler(event, data)

        user_id = event.from_user.id

        # Для Message: определяем тип чата
        if isinstance(event, Message):
            chat_type = event.chat.type
            is_private = chat_type == "private"
            # is_group = chat_type in ["group", "supergroup"]  # Не используется дальше, можно убрать
        else:  # CallbackQuery
            # Для колбэков чат из сообщения, если есть
            is_private = event.message.chat.type == "private" if event.message else False

        # Разрешить команду /start для Message независимо от всего
        if isinstance(event, Message) and event.text and event.text.startswith("/start"):
            return await handler(event, data)

        # Проверка регистрации
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            if not await cursor.fetchone():
                if isinstance(event, Message) and is_private:
                    await event.reply(
                        "<b>❌ Ошибка доступа</b>\n\n"
                        "🚫 Вы не зарегистрированы в системе.\n"
                        "📝 Используйте команду <code>/start</code>, чтобы начать!\n"
                        "🌟 Присоединяйтесь к нашему приключению!",
                        parse_mode="HTML"
                    )
                elif isinstance(event, CallbackQuery):
                    await event.answer("❌ Вы не зарегистрированы. Введите /start.", show_alert=True)
                return

        # Проверка бана
        try:
            with open(BANNED_FILE, "r", encoding="utf-8") as f:
                banned_data = json.load(f)
                if user_id in banned_data["banned"]:
                    if isinstance(event, Message) and is_private:
                        text = (
                            f"<b>🚫 @{event.from_user.username or event.from_user.first_name}, вы забанены!</b>\n\n"
                            "💸 <b>Цена разбана:</b> <code>200</code> Fezcoin\n"
                            "📞 Свяжитесь с администратором для разблокировки:\n"
                            "👉 Нажмите кнопку ниже, чтобы написать админу!"
                        )
                        kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="📩 Связаться с админом", url="https://t.me/Ferzister")]
                        ])
                        await event.reply(text, reply_markup=kb, parse_mode="HTML")
                    elif isinstance(event, CallbackQuery):
                        await event.answer("🚫 Вы забанены! Свяжитесь с админом.", show_alert=True)
                    return
        except FileNotFoundError:
            init_banned_file()

        # Проверка подписки на канал и чат
        try:
            channel_status = await bot.get_chat_member(chat_id="@CNLferz", user_id=user_id)
            chat_status = await bot.get_chat_member(chat_id="@chatFerzister", user_id=user_id)

            # Добавлен 'restricted' для учета muted пользователей
            is_channel_subscribed = channel_status.status in ["member", "restricted", "creator", "administrator"]
            is_chat_subscribed = chat_status.status in ["member", "restricted", "creator", "administrator"]

            if not (is_channel_subscribed and is_chat_subscribed):
                if isinstance(event, Message):
                    text = (
                        f"<b>📢 @{event.from_user.username or event.from_user.first_name}, подпишитесь!</b>\n\n"
                        "🔗 Для использования бота подпишитесь на:\n"
                    )
                    inline_keyboard = []
                    if not is_channel_subscribed:
                        text += "📢 <b>Канал:</b> @CNLferz\n"
                        inline_keyboard.append([InlineKeyboardButton(text="📢 Подписаться на канал", url="https://t.me/CNLferz")])
                    if not is_chat_subscribed:
                        text += "💬 <b>Чат:</b> https://t.me/chatFerzister\n"
                        inline_keyboard.append([InlineKeyboardButton(text="💬 Присоединиться к чату", url="https://t.me/chatFerzister")])
                    text += "\n👇 Нажмите кнопки ниже, чтобы подписаться!"
                    kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
                    await event.reply(text, reply_markup=kb, parse_mode="HTML")
                elif isinstance(event, CallbackQuery):
                    await event.answer("📢 Подпишитесь на канал и чат сначала!", show_alert=True)
                return
            else:
                # Проверка и начисление реферального бонуса (только для Message, т.к. для колбэков не нужно повторять)
                if isinstance(event, Message):
                    async with aiosqlite.connect(DB_PATH) as db:
                        cursor = await db.execute("SELECT subscribed, referrer_id FROM users WHERE user_id = ?", (user_id,))
                        row = await cursor.fetchone()
                        if row and row[0] == 0:  # Не подтверждено ранее
                            await db.execute("UPDATE users SET subscribed = 1 WHERE user_id = ?", (user_id,))
                            if row[1] is not None:  # Есть referrer
                                await db.execute(
                                    "UPDATE users SET fezcoin = fezcoin + 3.0, referral_earnings = referral_earnings + 3.0 WHERE user_id = ?",
                                    (row[1],)
                                )
                                await db.commit()
                                try:
                                    await bot.send_message(
                                        row[1],
                                        f"🎉 Вы получили +3 Fezcoin за реферала @{event.from_user.username or event.from_user.first_name} (ID: {user_id})!"
                                    )
                                except Exception as e:
                                    print(f"Ошибка уведомления referrer {row[1]}: {e}")
        except TelegramBadRequest as e:
            print(f"Ошибка проверки подписки для user_id {user_id}: {e}")
            if isinstance(event, Message):
                await event.reply(
                    "<b>❌ Ошибка подписки</b>\n\n"
                    "⚠️ Не удалось проверить подписку.\n"
                    "🔗 Убедитесь, что вы подписаны на:\n"
                    "📢 <b>Канал:</b> @CNLferz\n"
                    "💬 <b>Чат:</b> @chatFerzister\n\n"
                    "👇 Попробуйте подписаться и повторите снова!",
                    parse_mode="HTML"
                )
            elif isinstance(event, CallbackQuery):
                await event.answer("❌ Ошибка проверки подписки. Подпишитесь и попробуйте снова.", show_alert=True)
            return

        return await handler(event, data)

# Регистрация middleware
dp.message.middleware(BannedUserMiddleware())
dp.callback_query.middleware(BannedUserMiddleware())  #

@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        await message.reply("❌ Формат: /ban <ID>", parse_mode="HTML")
        return
    target_id = int(args[1])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (target_id,))
        if not await cursor.fetchone():
            await message.reply("❌ Пользователь не найден.", parse_mode="HTML")
            return
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            banned_data = json.load(f)
        if target_id not in banned_data["banned"]:
            banned_data["banned"].append(target_id)
            with open(BANNED_FILE, "w", encoding="utf-8") as f:
                json.dump(banned_data, f, ensure_ascii=False, indent=2)
        await message.reply(f"✅ Пользователь ID {target_id} забанен.", parse_mode="HTML")
    except FileNotFoundError:
        init_banned_file()
        with open(BANNED_FILE, "w", encoding="utf-8") as f:
            json.dump({"banned": [target_id]}, f, ensure_ascii=False, indent=2)
        await message.reply(f"✅ Пользователь ID {target_id} забанен.", parse_mode="HTML")

@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        await message.reply("❌ Формат: /unban <ID>", parse_mode="HTML")
        return
    target_id = int(args[1])
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            banned_data = json.load(f)
        if target_id in banned_data["banned"]:
            banned_data["banned"].remove(target_id)
            with open(BANNED_FILE, "w", encoding="utf-8") as f:
                json.dump(banned_data, f, ensure_ascii=False, indent=2)
            await message.reply(f"✅ Пользователь ID {target_id} разбанен.", parse_mode="HTML")
        else:
            await message.reply(f"❌ Пользователь ID {target_id} не забанен.", parse_mode="HTML")
    except FileNotFoundError:
        init_banned_file()
        await message.reply(f"❌ Пользователь ID {target_id} не забанен.", parse_mode="HTML")

# =================================== РАССЫЛКА ===========================
@dp.message(Command("rass"))
async def cmd_rass(message: types.Message):
    user_id = message.from_user.id
    chat_type = message.chat.type
    is_private = chat_type == "private"

    # Проверка регистрации
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            if is_private:
                await message.reply(
                    "❌ Вы не зарегистрированы. Введите /start.",
                    parse_mode="HTML"
                )
            return  # Игнорировать в группах

    # Проверка прав администратора
    if user_id != ADMIN_ID:
        if is_private:
            await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    # Получаем текст рассылки (всё после /rass)
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        if is_private:
            await message.reply(
                "❌ <b>Неверный формат.</b>\n\n"
                "Используйте: /rass <текст рассылки>\n"
                "Пример: /rass Привет, <b>игроки</b>! Новый бонус: /bonus",
                parse_mode="HTML"
            )
        return

    text = args[1]
    success_count = 0
    failed_count = 0

    # Загружаем список забаненных пользователей
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            banned_data = json.load(f)
            banned_users = banned_data.get("banned", [])
    except FileNotFoundError:
        init_banned_file()
        banned_users = []

    # Получаем всех пользователей из базы
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        users = await cursor.fetchall()

    if not users:
        if is_private:
            await message.reply("❌ В базе нет зарегистрированных пользователей.", parse_mode="HTML")
        return

    # Отправляем сообщение каждому пользователю
    for user in users:
        target_user_id = user[0]

        # Пропускаем забаненных пользователей
        if target_user_id in banned_users:

            failed_count += 1
            continue

        # Проверка подписки на канал и чат
        try:
            channel_status = await bot.get_chat_member(chat_id="@CNLferz", user_id=target_user_id)
            chat_status = await bot.get_chat_member(chat_id="@chatFerzister", user_id=target_user_id)

            is_channel_subscribed = channel_status.status in ["member", "creator", "administrator"]
            is_chat_subscribed = chat_status.status in ["member", "creator", "administrator"]

            if not (is_channel_subscribed and is_chat_subscribed):

                failed_count += 1
                continue
        except Exception as e:

            failed_count += 1
            continue

        # Отправляем сообщение
        try:
            await bot.send_message(
                chat_id=target_user_id,
                text=text,
                parse_mode="HTML"
            )
            success_count += 1
        except Exception as e:
            print(f"Ошибка отправки сообщения пользователю {target_user_id}: {e}")
            failed_count += 1
        # Задержка для соблюдения лимитов Telegram
        await asyncio.sleep(0.05)

    # Отправляем отчет администратору
    if is_private:
        await message.reply(
            f"✅ <b>Рассылка завершена</b>\n\n"
            f"📬 Отправлено: <code>{success_count}</code> пользователям\n"
            f"❌ Не удалось отправить: <code>{failed_count}</code> пользователям",
            parse_mode="HTML"
        )

# =================================== ДОНАТ ===========================
async def add_donat_bonus(user_id: int, fez: float):
    """Начисляет бонус рефереру (+5% от доната)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
            referrer = await cursor.fetchone()
            if referrer and referrer[0]:
                bonus = fez * 0.05
                await db.execute("UPDATE users SET fezcoin = fezcoin + ?, referral_earnings = referral_earnings + ? WHERE user_id = ?", (bonus, bonus, referrer[0]))
                await db.commit()
                try:
                    await bot.send_message(
                        referrer[0],
                        f"🎉 Вам начислено {bonus:.1f} Fezcoin за реферала!",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"Ошибка отправки бонуса рефереру: {e}")

@dp.message(Command("dhh"))
async def cmd_dhh(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    args = message.text.split()
    if len(args) != 3:
        await message.reply(
            "❌ <b>Неверный формат.</b>\n\n"
            "Используйте: /dhh <сумма> <ID>\n"
            "Пример: /dhh 500 123456789",
            parse_mode="HTML"
        )
        return

    try:
        amount = int(args[1])
        user_id = int(args[2])
    except ValueError:
        await message.reply(
            "❌ <b>Ошибка:</b> Сумма и ID должны быть числами.\n"
            "Пример: /dhh 500 123456789",
            parse_mode="HTML"
        )
        return

    if amount < 10:
        await message.reply(
            "❌ <b>Ошибка:</b> Минимальная сумма — 10 Fezcoin.",
            parse_mode="HTML"
        )
        return
    if amount > 1000000000:
        await message.reply(
            "❌ <b>Ошибка:</b> Максимальная сумма — 100 000 Fezcoin.",
            parse_mode="HTML"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user_exists = await cursor.fetchone()
            if not user_exists:
                await message.reply(
                    f"❌ <b>Ошибка:</b> Пользователь с ID {user_id} не найден.",
                    parse_mode="HTML"
                )
                return

        await db.execute("UPDATE users SET fezcoin = fezcoin + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()

    await add_donat_bonus(user_id, amount)

    try:
        await bot.send_message(
            user_id,
            f"✅ <b>Донат зачислен!</b>\n\n"
            f"💎 Вам начислено <code>{amount}</code> Fezcoin.\n\n"
            "Спасибо за поддержку! Проверьте /profile.\n"
            "Вопросы? Пишите @Ferzister. Удачи! 🎉",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка отправки сообщения пользователю: {e}")
        await message.reply(
            f"✅ <b>Донат зачислен:</b> {amount} Fezcoin для ID {user_id}.\n"
            f"⚠️ Не удалось уведомить пользователя.",
            parse_mode="HTML"
        )
        return

    await message.reply(
        f"✅ <b>Донат обработан:</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"💎 Начислено: <code>{amount}</code> Fezcoin",
        parse_mode="HTML"
    )



@dp.message(Command("donat"))
@dp.message(F.text.lower().in_(["донат"]))
async def cmd_donat(message: types.Message):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /donat доступна только в личных сообщениях с ботом! Перейдите в приватный чат.",
            parse_mode="HTML"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT rate FROM exchange_rate WHERE id = 1")
        rate = (await cursor.fetchone())[0]

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Связаться с админом", url="https://t.me/Ferzister")]
        ]
    )
    text = (

        "💎 <b>Донат в Fezcoin</b> 💎\n\n"
        
        "🎉 <b>Поддержи наш бот и стань частью элиты!</b> 🎉\n\n"
        "💰 <b>Текущий курс:</b> 1 руб = <code>{:.1f}</code> Fezcoin\n"
        "🤝 <b>Торг уместен!</b> Обсуди с админом индивидуальные условия.\n\n"
        "🌟 <b>Почему стоит донатить?</b>\n"
        "• <b>Поддержка проекта</b>: Помоги боту развиваться и получать новые крутые функции!\n"
        "• <b>Игровые бонусы</b>: Fezcoin открывает доступ к уникальным возможностям и преимуществам.\n"
        "• <b>Статус</b>: Стань заметным игроком с эксклюзивными наградами!\n\n"
        "📩 <b>Как купить Fezcoin?</b>\n"
        "Свяжитесь с администратором через кнопку ниже, чтобы обсудить сумму и получить Fezcoin на ваш баланс.\n\n"
        "👇 <b>Готов поддержать? Жми!</b>\n"

    ).format(rate)
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dp.message(Command("kk"))
@dp.message(F.text.lower().in_(["количество"]))
async def cmd_kk(message: types.Message):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /kk доступна только в личных сообщениях с ботом! Перейдите в приватный чат.",
            parse_mode="HTML"
        )
        return

    if message.from_user.id != ADMIN_ID:
        await message.reply(
            "❌ <b>Ошибка:</b> У вас нет прав для этой команды. Только администраторы могут изменять курс.",
            parse_mode="HTML"
        )
        return

    args = message.text.split()
    if len(args) != 2:
        await message.reply(
            "❌ <b>Неверный формат.</b>\n\n"
            "Используйте: /kk <b>курс</b>\n"
            "Пример: /kk 9\n"
            "Курс должен быть положительным числом (например, 9 для 1 руб = 9 Fezcoin).",
            parse_mode="HTML"
        )
        return

    try:
        new_rate = float(args[1])
        if new_rate <= 0:
            raise ValueError("Курс должен быть положительным.")
    except ValueError:
        await message.reply(
            "❌ <b>Ошибка:</b> Курс должен быть положительным числом.\n"
            "Пример: /kk 9",
            parse_mode="HTML"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE exchange_rate SET rate = ? WHERE id = 1", (new_rate,))
        await db.commit()

    # Fixed success message with explicit <code> tags around the dynamic rate
    success_text = (
        f"✅ <b>Курс обновлен!</b>\n\n"
        f"📊 <b>Новый курс:</b> 1 руб = <code>{new_rate:.1f}</code> Fezcoin"
    )

    try:
        await message.reply(success_text, parse_mode="HTML")
    except Exception as e:
        print(f"Error sending /kk success message: {e}")
        # Fallback: Send without HTML if parsing fails
        fallback_text = f"✅ Курс обновлен! Новый курс: 1 руб = {new_rate:.1f} Fezcoin"
        await message.reply(fallback_text)


# Global variables (assumed to be defined elsewhere, included for clarity)
# Global variables (assumed to be defined elsewhere, included for clarity)
PAGE_SIZE = 50  # Количество пользователей на странице
USER_PAGES = {}  # Хранение текущей страницы для каждого пользователя
USER_SORT = {}  # Хранение текущей сортировки для каждого пользователя
USER_FILTER = {}  # Хранение текущего фильтра для каждого пользователя

# Инициализация banned.json, если файл отсутствует

def is_user_banned(user_id):
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            banned_data = json.load(f)
            return user_id in banned_data.get("banned", [])
    except FileNotFoundError:
        init_banned_file()
        return False
    except Exception as e:
        print(f"Ошибка при чтении banned.json: {e}")
        return False

# Команда /user
@dp.message(Command("user"))
async def cmd_user(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT verified FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        verified = result[0]

    if user_id != ADMIN_ID and verified != 1:
        await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    args = message.text.split()
    if len(args) != 2:
        await message.reply(
            "❌ <b>Неверный формат.</b>\n\n"
            "Используйте: /user <code>ID пользователя</code>\n"
            "Пример: /user 123456789",
            parse_mode="HTML"

        )
        return

    try:
        target_user_id = int(args[1])
    except ValueError:

        await message.reply("❌ <b>Ошибка:</b> ID должен быть числом.", parse_mode="HTML")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT user_id, username, coins, last_bonus, win_amount, lose_amount, fezcoin, fezcoin_sold,
                   last_farm_collect, upgrades, total_farmed_fez, firewall, last_firewall_activation,
                   bank_amount, last_interest, status, hidden, last_box, referrer_id, referral_earnings,
                   created_at, last_active, escrow, boss_experience, total_exchanged_exp, total_gg_from_exp, verified
            FROM users WHERE user_id = ?
        """, (target_user_id,))
        user_data = await cursor.fetchone()

        if not user_data:

            await message.reply(f"❌ <b>Пользователь с ID {target_user_id} не найден.</b>", parse_mode="HTML"
                                 )
            return

        # Получаем количество рефералов
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (target_user_id,))
        referral_count = (await cursor.fetchone())[0]

        # Получаем количество депозитов
        cursor = await db.execute("SELECT COUNT(*) FROM deposits WHERE user_id = ?", (target_user_id,))
        deposit_count = (await cursor.fetchone())[0]

        # Получаем общее количество оружия
        cursor = await db.execute("""
            SELECT SUM(quantity) FROM player_weapons pw
            JOIN weapons w ON pw.weapon_id = w.weapon_id
            WHERE pw.user_id = ?
        """, (target_user_id,))
        total_weapons = (await cursor.fetchone())[0] or 0

        # Получаем устройства фермы
        cursor = await db.execute("SELECT device_type, quantity FROM farm_devices WHERE user_id = ?", (target_user_id,))
        farm_devices = await cursor.fetchall()

        # Получаем открытые ордера
        cursor = await db.execute("""
            SELECT order_id, amount, price, order_type FROM fez_orders
            WHERE (seller_id = ? OR buyer_id = ?) AND status = 'open'
        """, (target_user_id, target_user_id))
        open_orders = await cursor.fetchall()

    target_user_id, username, coins, last_bonus, win_amount, lose_amount, fezcoin, fezcoin_sold, \
        last_farm_collect, upgrades, total_farmed_fez, firewall, last_firewall_activation, \
        bank_amount, last_interest, status, hidden, last_box, referrer_id, referral_earnings, \
        created_at, last_active, escrow, boss_experience, total_exchanged_exp, total_gg_from_exp, verified = user_data

    moscow_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(created_at).replace(tzinfo=pytz.UTC).astimezone(moscow_tz).strftime(
        '%Y-%m-%d %H:%M:%S')
    last_active_msk = datetime.fromisoformat(last_active).replace(tzinfo=pytz.UTC).astimezone(moscow_tz).strftime(
        '%Y-%m-%d %H:%M:%S') if last_active else "Неактивен"

    farm_text = "\n".join([f"• {device}: {qty}" for device, qty in farm_devices]) if farm_devices else "Нет устройств"
    orders_text = "\n".join(
        [f"• #{order_id}: {amount} Fez за {price} GG ({'Покупка' if type_ == 'buy' else 'Продажа'})" for
         order_id, amount, price, type_ in open_orders]) if open_orders else "Нет открытых ордеров"

    text = (
        f"👤 <b>Профиль пользователя ID {target_user_id}</b>\n\n"
        f"📛 Имя пользователя: <code>@{username}</code>\n"
        f"💰 Монеты: <code>{format_balance(coins)}</code> GG\n"
        f"💎 Fezcoin: <code>{fezcoin:.1f}</code>\n"
        f"💸 Проданные Fezcoin: <code>{fezcoin_sold:.1f}</code>\n"
        f"📈 Выигрыши: <code>{format_balance(win_amount)}</code> | Проигрыши: <code>{format_balance(lose_amount)}</code>\n"
        f"🔗 Рефералы: {referral_count} | Заработок: <code>{referral_earnings:.1f}</code> Fezcoin\n"
        f"🏦 Сумма в банке: <code>{format_balance(bank_amount)}</code> GG\n"
        f"🛡️ Фаервол: {firewall}\n"
        f"⚔️ Улучшения: {upgrades}\n"
        f"🌾 Всего добыто Fez: <code>{total_farmed_fez:.1f}</code>\n"
        f"💼 Эскроу: <code>{escrow:.1f}</code> Fezcoin\n"
        f"👹 Опыт с боссов: {boss_experience}\n"
        f"🔄 Обменяно опыта: {total_exchanged_exp}\n"
        f"🎁 GG от опыта: <code>{format_balance(total_gg_from_exp)}</code>\n"
        f"🏆 Статус: {status}\n"
        f"🕶️ Скрыт в топах: {'Да' if hidden else 'Нет'}\n"
        f"🚫 Забанен: {'Да' if is_user_banned(target_user_id) else 'Нет'}\n"
        f"✅ Верифицирован: {'Да' if verified else 'Нет'}\n"
        f"📦 Депозиты: {deposit_count}\n"
        f"🔫 Всего оружия: {total_weapons}\n"
        f"📅 Создан: <code>{created_at_msk}</code>\n"
        f"🕐 Последняя активность: <code>{last_active_msk}</code>\n\n"
        f"🌾 <b>Устройства фермы:</b>\n{farm_text}\n\n"
        f"📊 <b>Открытые ордера:</b>\n{orders_text}"
    )


    await message.reply(text, parse_mode="HTML")

# Команда /users
@dp.message(Command("users"))
async def cmd_users(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT verified FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        verified = result[0]

    if user_id != ADMIN_ID and verified != 1:
        await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    tg_id = message.from_user.id
    USER_PAGES[tg_id] = 0
    USER_SORT[tg_id] = "default"
    USER_FILTER[tg_id] = "all"

    users = await fetch_users_data(tg_id)
    if not users:

        await message.reply("Пользователей нет в базе.", parse_mode="HTML")
        return

    await send_users_page(message, tg_id, users, 0)

async def fetch_users_data(tg_id):
    async with aiosqlite.connect(DB_PATH) as db:
        # Применяем фильтр
        filter_type = USER_FILTER.get(tg_id, "all")
        conditions = []
        if filter_type == "rich":
            conditions.append("coins > 1000000")
        elif filter_type == "banned":
            # Для фильтра "banned" будем фильтровать после выборки, так как бан хранится в banned.json
            pass

        # Применяем условия для сортировки active/inactive
        sort_type = USER_SORT.get(tg_id, "default")
        if sort_type == "active":
            conditions.append("last_active IS NOT NULL")
        elif sort_type == "inactive":
            conditions.append("last_active IS NULL")

        query = "SELECT user_id, username, coins, last_active FROM users"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Применяем сортировку
        if sort_type == "money_high":
            query += " ORDER BY coins DESC"
        elif sort_type == "money_low":
            query += " ORDER BY coins ASC"
        elif sort_type == "active":
            query += " ORDER BY last_active DESC"
        elif sort_type == "inactive":
            query += " ORDER BY coins DESC"
        # Для default - без ORDER BY

        cursor = await db.execute(query)
        users = await cursor.fetchall()

        # Фильтрация забаненных пользователей через banned.json
        if filter_type == "banned":
            banned_users = []
            try:
                with open(BANNED_FILE, "r", encoding="utf-8") as f:
                    banned_data = json.load(f)
                    banned_users = banned_data.get("banned", [])
            except FileNotFoundError:
                init_banned_file()
            users = [user for user in users if user[0] in banned_users]

        return users

async def send_users_page(message, tg_id, users, page):
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    users_page = users[start_idx:end_idx]

    current_filter = USER_FILTER.get(tg_id, "all")
    filter_display = {
        "all": "Все",
        "banned": "Заблокированные",
        "rich": "С монетами > 1кк"
    }
    current_sort = USER_SORT.get(tg_id, "default")
    sort_display = {
        "default": "По умолчанию",
        "money_high": "Монеты (убыв.)",
        "money_low": "Монеты (возр.)",
        "active": "Активные",
        "inactive": "Неактивные",
    }

    total_pages = ((len(users) - 1) // PAGE_SIZE) + 1
    text = (
        f"👥 <b>Количество игроков:</b> {len(users)}\n"
        f"<b>Страница:</b> {page + 1}/{total_pages}\n"
        f"Фильтр: {filter_display[current_filter]}\n"
        f"Сортировка: {sort_display[current_sort]}\n\n"
    )

    banned_users = []
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            banned_data = json.load(f)
            banned_users = banned_data.get("banned", [])
    except FileNotFoundError:
        init_banned_file()

    for idx, (user_id, username, coins, last_active) in enumerate(users_page, start=start_idx + 1):
        display_name = f"@{username}" if username else f"ID {user_id}"
        status_emoji = "🚫" if user_id in banned_users else ("🟢" if last_active else "🔴")
        text += f"{idx}) {display_name} [{user_id}] - {format_balance(coins)} 💰 - {status_emoji}\n"

    # Кнопки
    buttons = []
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅ Предыдущая страница", callback_data=f"users_page:{page - 1}"))
    if end_idx < len(users):
        nav_buttons.append(InlineKeyboardButton(text="Следующая страница ➡", callback_data=f"users_page:{page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    filter_buttons = [
        [
            InlineKeyboardButton(text=f"Все{' ✅' if current_filter == 'all' else ''}",
                                callback_data="users_filter:all"),
            InlineKeyboardButton(text=f"Заблокированные{' ✅' if current_filter == 'banned' else ''}",
                                callback_data="users_filter:banned")
        ],
        [
            InlineKeyboardButton(text=f"С монетами > 1кк{' ✅' if current_filter == 'rich' else ''}",
                                callback_data="users_filter:rich")
        ]
    ]
    buttons.extend(filter_buttons)

    sort_buttons = [
        [
            InlineKeyboardButton(text=f"Монеты (убыв.){' ✅' if current_sort == 'money_high' else ''}",
                                callback_data="users_sort:money_high"),
            InlineKeyboardButton(text=f"Монеты (возр.){' ✅' if current_sort == 'money_low' else ''}",
                                callback_data="users_sort:money_low")
        ],
        [
            InlineKeyboardButton(text=f"Активные{' ✅' if current_sort == 'active' else ''}",
                                callback_data="users_sort:active"),
            InlineKeyboardButton(text=f"Неактивные{' ✅' if current_sort == 'inactive' else ''}",
                                callback_data="users_sort:inactive")
        ],
        [
            InlineKeyboardButton(text=f"По умолчанию{' ✅' if current_sort == 'default' else ''}",
                                callback_data="users_sort:default")
        ]
    ]
    buttons.extend(sort_buttons)

    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)

    try:
        if hasattr(message, 'edit_text'):  # Если вызвано из коллбэка
            await message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
        else:  # Если вызвано командой /users
            await message.reply(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message can't be edited" in str(e):
            await message.reply(text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            raise

@dp.callback_query(lambda c: c.data.startswith("users_page:"))
async def change_users_page(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT verified FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Вы не зарегистрированы. Введите /start.", show_alert=True)
            return
        verified = result[0]

    if user_id != ADMIN_ID and verified != 1:
        await call.answer("❌ У вас нет прав для этой команды!", show_alert=True)
        return

    page = int(call.data.split(":")[1])
    tg_id = call.from_user.id

    users = await fetch_users_data(tg_id)
    if not users:
        await call.message.edit_text("Пользователей нет в базе.", parse_mode="HTML")
        await call.answer()
        return

    USER_PAGES[tg_id] = page
    await send_users_page(call.message, tg_id, users, page)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("users_filter:"))
async def change_users_filter(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT verified FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Вы не зарегистрированы. Введите /start.", show_alert=True)
            return
        verified = result[0]

    if user_id != ADMIN_ID and verified != 1:
        await call.answer("❌ У вас нет прав для этой команды!", show_alert=True)
        return

    filter_type = call.data.split(":")[1]
    tg_id = call.from_user.id

    current_filter = USER_FILTER.get(tg_id, "all")
    if current_filter == filter_type:
        await call.answer("Этот фильтр уже выбран!", show_alert=True)
        return

    USER_FILTER[tg_id] = filter_type
    USER_PAGES[tg_id] = 0

    users = await fetch_users_data(tg_id)
    if not users:
        await call.message.edit_text("Пользователей по этому фильтру нет.", parse_mode="HTML")
        await call.answer()
        return

    await send_users_page(call.message, tg_id, users, 0)
    await call.answer(f"Фильтр изменен на: {filter_type}")

@dp.callback_query(lambda c: c.data.startswith("users_sort:"))
async def change_users_sort(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT verified FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Вы не зарегистрированы. Введите /start.", show_alert=True)
            return
        verified = result[0]

    if user_id != ADMIN_ID and verified != 1:
        await call.answer("❌ У вас нет прав для этой команды!", show_alert=True)
        return

    sort_type = call.data.split(":")[1]
    tg_id = call.from_user.id

    current_sort = USER_SORT.get(tg_id, "default")
    if current_sort == sort_type:
        await call.answer("Эта сортировка уже выбрана!", show_alert=True)
        return

    USER_SORT[tg_id] = sort_type
    USER_PAGES[tg_id] = 0

    users = await fetch_users_data(tg_id)
    if not users:
        await call.message.edit_text("Пользователей нет в базе.", parse_mode="HTML")
        await call.answer()
        return

    await send_users_page(call.message, tg_id, users, 0)
    await call.answer(f"Сортировка изменена на: {sort_type}")

# =================================== АДМИН: СНЯТИЕ ВЕРИФИКАЦИИ ===========================
@dp.message(Command("unver"))
async def cmd_unver(message: types.Message):
    user_id = message.from_user.id
    chat_type = message.chat.type
    is_private = chat_type == "private"

    # Проверка регистрации
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            if is_private:
                await message.reply(
                    "❌ Вы не зарегистрированы. Введите /start.",
                    parse_mode="HTML"
                )
            return  # Игнорировать в группах

    # Проверка прав администратора
    if user_id != ADMIN_ID:
        if is_private:
            await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    # Получаем ID пользователя из команды
    args = message.text.split()
    if len(args) != 2:
        if is_private:
            await message.reply(
                "❌ <b>Неверный формат.</b>\n\n"
                "Используйте: /unver <user_id>\n"
                "Пример: /unver 123456789",
                parse_mode="HTML"
            )
        return

    try:
        target_user_id = int(args[1])
    except ValueError:
        if is_private:
            await message.reply(
                "❌ <b>Ошибка:</b> ID должен быть числом.",
                parse_mode="HTML"
            )
        return

    # Проверяем, существует ли пользователь
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT username, verified FROM users WHERE user_id = ?", (target_user_id,))
        result = await cursor.fetchone()
        if not result:
            if is_private:
                await message.reply(
                    f"❌ Пользователь с ID <code>{target_user_id}</code> не найден.",
                    parse_mode="HTML"
                )
            return

        username, verified = result
        if not verified:
            if is_private:
                await message.reply(
                    f"❌ Пользователь @{username or target_user_id} не верифицирован.",
                    parse_mode="HTML"
                )
            return

        # Снимаем верификацию
        await db.execute("UPDATE users SET verified = 0 WHERE user_id = ?", (target_user_id,))
        await db.commit()

    if is_private:
        await message.reply(
            f"✅ Верификация снята с пользователя @{username or target_user_id}!",
            parse_mode="HTML"
        )

# =================================== АДМИН: СПИСОК КОМАНД ===========================
@dp.message(Command("s"))
async def cmd_s(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return

    if user_id != ADMIN_ID:
        await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    text = (
        "🔧 <b>Админские команды:</b>\n\n"
        "• <code>/dhh &lt;сумма&gt; &lt;ID&gt;</code> — Начислить донат (Fezcoin)\n"
        "• <code>/kk &lt;курс&gt;</code> — Установить курс обмена (1 руб = X Fezcoin)\n"
        "• <code>/new_promo &lt;coins&gt; &lt;max_activations&gt; [name]</code> — Создать промокод\n"
        "• <code>/user &lt;ID&gt;</code> — Просмотр детальной информации о пользователе\n"
        "• <code>/users</code> — Список всех игроков (с пагинацией, фильтрами, сортировкой)\n"
        "• <code>/ban &lt;ID&gt;</code> — Забанить пользователя\n"
        "• <code>/unban &lt;ID&gt;</code> — Разбанить пользователя\n"
        "• <code>/set_status &lt;ID&gt; &lt;статус&gt;</code> — Установить статус пользователя\n"
        "• <code>/hhh &lt;ID&gt; &lt;сумма&gt;</code> — Начислить GG пользователю\n"
        "• <code>/uhhh &lt;ID&gt; &lt;сумма&gt;</code> — Снять GG с пользователя\n"
        "• <code>/new_boss</code> — Создать нового босса\n"
        "• <code>/ver &lt;ID&gt;</code> — Верифицировать пользователя\n"
        "• <code>/unver &lt;ID&gt;</code> — Снять верификацию пользователя"
        "• <code>/rass</code> — Рассылка"
    )
    await message.reply(text, parse_mode="HTML")




# =================================== РЕФЕРАЛЬНАЯ СИСТЕМА ===========================



# =================================== РЕФЕРАЛЬНАЯ СИСТЕМА ===========================
@dp.message(Command("ref"))
@dp.message(F.text.lower().in_(["рефералка", "реф"]))
async def cmd_ref(message: types.Message):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /ref доступна только в личных сообщениях с ботом! Перейдите в приватный чат.",
            parse_mode="HTML"
        )
        return

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, referral_earnings FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await message.reply(
                "❌ <b>Ошибка:</b> Вы не зарегистрированы. Введите /start, чтобы начать.",
                parse_mode="HTML"
            )
            return
        referral_earnings = user_data[1]

        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (user_id,))
        referral_count = (await cursor.fetchone())[0]

    bot_info = await bot.get_me()
    bot_username = bot_info.username
    referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    text = (
        "╔═════════════════════╗\n"
        "   👥 <b>Реферальная система</b>\n"
        "╚═════════════════════╝\n\n"
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{referral_link}</code>\n\n"
        f"📊 <b>Ваша статистика:</b>\n"
        f"• 👤 Приглашено друзей: <b>{referral_count}</b>\n"
        f"• 💎 Заработано Fezcoin: <b>{referral_earnings:.1f}</b>\n\n"
        "📖 <b>Правила рефералки:</b>\n"
        "• +3 Fezcoin за каждого нового реферала, подписавшегося на канал и чат.\n"
        "• +5% от всех донатов ваших рефералов.\n"
        "• Бонусы начисляются автоматически после активации реферала.\n"
        "• Распространяйте ссылку в соцсетях, чатах и среди друзей!\n\n"
        "💡 <b>Совет:</b> Чем активнее ваши рефералы, тем больше ваш доход!"
    )
    await message.reply(text, parse_mode="HTML")

# =================================== СТАРТ ===========================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or ""
    args = message.text.split()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        is_new_user = not result
        referrer_id = None

        if is_new_user:
            if len(args) > 1 and args[1].startswith("ref_"):
                try:
                    referrer_id = int(args[1][4:])
                    if referrer_id == user_id:
                        referrer_id = None
                except ValueError:
                    referrer_id = None

            await db.execute("""
                INSERT INTO users (
                    user_id, username, coins, win_amount, lose_amount,
                    fezcoin, referrer_id, referral_earnings, created_at, subscribed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
            """, (user_id, username, 0, 0, 0, 0.0, referrer_id, 0, 0))
            await db.commit()

    if len(args) > 1 and args[1].startswith("promo_"):
        promo_name = args[1][6:]
        await cmd_promo(message, promo_name=promo_name)  # Предполагается, что cmd_promo определена где-то в коде
        return

    text = (
        "<b>👑 Добро пожаловать, {name}!</b>\n\n"
        "✨ Ты в нашем уютном боте!\n"
        "🔸 Здесь тебя ждут монеты, топы и бонусы.\n\n"
        "📋 <i>Пользуйся командами в списке /help</i>\n"
        "💬 Вопросы? — <a href='https://t.me/Ferzister'>пиши админу</a>!\n"
        "★ Удачи и приятного использования! ★"
    ).format(name=message.from_user.full_name)
    await message.reply(text, parse_mode="HTML")



# =================================== ПРОМОКОДЫ ===========================

class PromoCreate(StatesGroup):
    coins = State()
    max_activations = State()


# Обработчики команд для промокодов
@dp.message(Command("new_promo"))
async def cmd_new_promo(message: types.Message):
    user_id = message.from_user.id

    # Проверка регистрации и статуса verified
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins, verified FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await message.reply(
                "❌ <b>Ошибка:</b> Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                parse_mode="HTML"
            )
            return
        user_coins, verified = user_data[1], user_data[2]

    # Проверка доступа: администратор или verified == 1
    if user_id != ADMIN_ID and verified != 1:
        await message.reply(
            "❌ <b>Ошибка доступа:</b> У вас нет прав для использования этой команды. Только администраторы или верифицированные пользователи могут создавать промокоды.",
            parse_mode="HTML"
        )
        return

    args = message.text.split()[1:]
    if len(args) < 2:
        text = (
            "🌟 <b>Создание нового промокода</b> 🌟\n\n"
            "📋 <b>Использование:</b> /new_promo (coins) (max_activations) [name]\n\n"
            "🔹 <b>coins</b> — количество монет за одну активацию (минимум 1000 GG)\n"
            "🔹 <b>max_activations</b> — максимальное число активаций (от 1 до 10000)\n"
            "🔹 <b>name</b> — опциональное имя промокода (должно быть уникальным), иначе генерируется случайно\n\n"
            f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n"
            "💰 <b>Стоимость создания:</b> coins * max_activations (списывается с вашего баланса GG).\n\n"
            "⚠️ <b>Важно:</b> Убедитесь, что у вас достаточно средств. Если параметров недостаточно, укажите их заново."
        )
        await message.reply(text, parse_mode="HTML")
        return

    coins = parse_bet_input(args[0])
    if coins < 1000:
        await message.reply(
            "❌ <b>Ошибка ввода:</b> Количество монет должно быть не менее 1000 GG. Пожалуйста, попробуйте снова.",
            parse_mode="HTML"
        )
        return

    max_activations = parse_bet_input(args[1])
    if max_activations < 1 or max_activations > 10000:
        await message.reply(
            "❌ <b>Ошибка ввода:</b> Количество активаций должно быть от 1 до 10000. Укажите корректное значение.",
            parse_mode="HTML"
        )
        return

    creation_cost = coins * max_activations

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        user_coins = (await cursor.fetchone())[0]

        if user_coins < creation_cost:
            await message.reply(
                f"❌ <b>Недостаточно средств:</b> Для создания требуется <code>{format_balance(creation_cost)}</code> GG. "
                f"Ваш текущий баланс: <code>{format_balance(user_coins)}</code> GG. Пополните баланс.",
                parse_mode="HTML"
            )
            return

        if len(args) > 2:
            promo_name = args[2]
            cursor = await db.execute("SELECT promo_id FROM promo_codes WHERE name = ?", (promo_name,))
            if await cursor.fetchone():
                await message.reply(
                    "❌ <b>Имя занято:</b> Промокод с таким именем уже существует. Выберите другое.",
                    parse_mode="HTML"
                )
                return
        else:
            promo_name = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            while True:
                cursor = await db.execute("SELECT promo_id FROM promo_codes WHERE name = ?", (promo_name,))
                if not await cursor.fetchone():
                    break
                promo_name = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

        now = datetime.now(timezone.utc).isoformat()
        await db.execute(
            "INSERT INTO promo_codes (name, coins, max_activations, creator_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (promo_name, coins, max_activations, user_id, now)
        )
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (creation_cost, user_id))
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        updated_coins = (await cursor.fetchone())[0]
        await db.commit()

    bot_info = await bot.get_me()
    bot_username = bot_info.username
    activation_url = f"https://t.me/{bot_username}?start=promo_{promo_name}"

    text = (
        "🎉 <b>Промокод успешно создан!</b> 🎉\n\n"
        f"✅ <b>Имя промокода:</b> <code>{promo_name}</code>\n"
        f"💰 <b>Награда за активацию:</b> <code>{format_balance(coins)}</code> GG\n"
        f"🔄 <b>Максимум активаций:</b> {max_activations}\n"
        f"💸 <b>Стоимость создания:</b> <code>{format_balance(creation_cost)}</code> GG (списано с вашего баланса)\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(updated_coins)}</code> GG\n\n"
        f"🔗 <b>Ссылка для активации:</b> {activation_url}\n"
        "Или просто используйте команду: /promo {promo_name}\n\n"
        "🌟 Распространяйте промокод среди друзей для большего эффекта!"
    ).format(promo_name=promo_name)
    await message.reply(text, parse_mode="HTML")


@dp.message(lambda m: m.text and m.text.startswith("/start promo_"))
async def cmd_start_promo(message: types.Message):
    args = message.text.split()
    if len(args) < 2 or not args[1].startswith("promo_"):
        await message.reply(
            "❌ <b>Ошибка:</b> Неверный формат ссылки для активации промокода. Пожалуйста, проверьте и попробуйте снова.",
            parse_mode="HTML"
        )
        return

    promo_name = args[1][6:]  # Remove "promo_" prefix
    await cmd_promo(message, promo_name=promo_name)


@dp.message(Command("promo"))
async def cmd_promo(message: types.Message, promo_name: str = None):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /promo доступна только в личных сообщениях с ботом! Перейдите в приватный чат.",
            parse_mode="HTML"
        )
        return

    user_id = message.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply(
                "❌ <b>Ошибка:</b> Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                parse_mode="HTML"
            )
            return
        user_coins = result[1]

    # Handle promo code activation
    if promo_name or len(message.text.split()) > 1:
        if not promo_name:
            args = message.text.split()
            promo_name = args[1].strip()

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT promo_id, coins, max_activations, activations FROM promo_codes WHERE name = ?", (promo_name,))
            promo = await cursor.fetchone()
            if not promo:
                await message.reply(
                    "❌ <b>Промокод не найден:</b> Убедитесь, что имя введено правильно, и попробуйте снова.",
                    parse_mode="HTML"
                )
                return

            promo_id, coins, max_activations, activations = promo

            if activations >= max_activations:
                # Delete promo code if all activations are used
                await db.execute("DELETE FROM promo_codes WHERE promo_id = ?", (promo_id,))
                await db.execute("DELETE FROM promo_activations WHERE promo_id = ?", (promo_id,))
                await db.commit()
                await message.reply(
                    "❌ <b>Лимит исчерпан:</b> Этот промокод исчерпал все активации и был удален.",
                    parse_mode="HTML"
                )
                return

            cursor = await db.execute("SELECT user_id FROM promo_activations WHERE promo_id = ? AND user_id = ?",
                                      (promo_id, user_id))
            if await cursor.fetchone():
                await message.reply(
                    "❌ <b>Уже активировано:</b> Вы уже использовали этот промокод ранее.",
                    parse_mode="HTML"
                )
                return

            now = datetime.now(UTC).isoformat()
            await db.execute("INSERT INTO promo_activations (promo_id, user_id, activated_at) VALUES (?, ?, ?)",
                             (promo_id, user_id, now))
            await db.execute("UPDATE promo_codes SET activations = activations + 1 WHERE promo_id = ?", (promo_id,))
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (coins, user_id))

            # Check if promo has reached max activations after this use
            cursor = await db.execute("SELECT activations FROM promo_codes WHERE promo_id = ?", (promo_id,))
            updated_activations = (await cursor.fetchone())[0]
            if updated_activations >= max_activations:
                await db.execute("DELETE FROM promo_codes WHERE promo_id = ?", (promo_id,))
                await db.execute("DELETE FROM promo_activations WHERE promo_id = ?", (promo_id,))

            # Get updated balance
            cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
            updated_coins = (await cursor.fetchone())[0]
            await db.commit()

        await message.reply(
            "🎉 <b>Успешная активация!</b> 🎉\n\n"
            f"✨ <b>Промокод {promo_name} активирован!</b>\n"
            f"💰 Вы получили <code>{format_balance(coins)}</code> GG\n"
            f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(updated_coins)}</code> GG\n\n"
            "🌟 Спасибо за использование!",
            parse_mode="HTML"
        )
        return

    # Show promo menu with balance
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✨ Создать новый промокод", callback_data="promo_create")],
            [InlineKeyboardButton(text="📋 Мои промокоды", callback_data="promo_my")]
        ]
    )
    text = (
        "🌟 <b>Меню промокодов</b> 🌟\n\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
        "Здесь вы можете активировать, создавать или просматривать свои промокоды.\n\n"
        "🔹 <b>Активация:</b> Используйте /promo [имя_промокода] для получения наград.\n"
        "🔹 Выберите действие ниже:"
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(lambda c: c.data == "promo_create")
async def promo_create(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await call.answer(
                "❌ Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                show_alert=True)
            return
        user_coins = user_data[1]

        cursor = await db.execute("SELECT COUNT(*) FROM promo_codes WHERE creator_id = ?", (user_id,))
        count = (await cursor.fetchone())[0]
        if count >= 5:
            await call.answer("❌ Лимит достигнут: Вы можете создать не более 5 промокодов. Удалите старые.",
                              show_alert=True)
            return

    await state.set_state(PromoCreate.coins)
    await state.update_data(user_id=user_id)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться назад", callback_data="promo_back")]
        ]
    )
    text = (
        "🌟 <b>Создание промокода</b> 🌟\n\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
        "Шаг 1: Введите количество монет GG за одну активацию (минимум 1000).\n\n"
        "⚠️ <b>Важно:</b> Стоимость создания зависит от этого значения и числа активаций."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.message(PromoCreate.coins)
async def process_promo_coins(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await message.reply(
                "❌ <b>Ошибка:</b> Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                parse_mode="HTML"
            )
            await state.clear()
            return
        user_coins = user_data[1]

    coins = parse_bet_input(message.text)
    if coins < 1000:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="promo_back")]
            ]
        )
        await message.reply(
            f"❌ <b>Ошибка ввода:</b> Количество монет должно быть не менее 1000 GG.\n"
            f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
            "❗ Процесс создания прерван. Вернитесь в меню и попробуйте заново.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return

    await state.update_data(coins=coins)
    await state.set_state(PromoCreate.max_activations)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться назад", callback_data="promo_create")]
        ]
    )
    text = (
        "🌟 <b>Создание промокода</b> 🌟\n\n"
        f"💰 <b>Монеты за активацию:</b> <code>{format_balance(coins)}</code> GG\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n"
        "Шаг 2: Введите максимальное количество активаций (от 1 до 10000).\n\n"
        "⚠️ <b>Подсказка:</b> Общая стоимость = монеты * активации."
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dp.message(PromoCreate.max_activations)
async def process_promo_activations(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await message.reply(
                "❌ <b>Ошибка:</b> Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                parse_mode="HTML"
            )
            await state.clear()
            return
        user_coins = user_data[1]

    max_activations = parse_bet_input(message.text)
    data = await state.get_data()
    coins = data.get("coins")

    if max_activations < 1 or max_activations > 10000:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="promo_back")]
            ]
        )
        await message.reply(
            f"❌ <b>Ошибка ввода:</b> Количество активаций должно быть от 1 до 10000.\n"
            f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
            "❗ Процесс создания прерван. Вернитесь в меню и попробуйте заново.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return

    creation_cost = coins * max_activations

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        user_coins = (await cursor.fetchone())[0]

        if user_coins < creation_cost:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="promo_back")]
                ]
            )
            await message.reply(
                f"❌ <b>Недостаточно средств:</b> Для создания требуется <code>{format_balance(creation_cost)}</code> GG. "
                f"Ваш текущий баланс: <code>{format_balance(user_coins)}</code> GG. Пополните баланс.\n\n"
                "❗ Процесс создания прерван. Вернитесь в меню.",
                reply_markup=kb, parse_mode="HTML"
            )
            await state.clear()
            return

        promo_name = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        while True:
            cursor = await db.execute("SELECT promo_id FROM promo_codes WHERE name = ?", (promo_name,))
            if not await cursor.fetchone():
                break
            promo_name = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

        now = datetime.now(UTC).isoformat()
        await db.execute(
            "INSERT INTO promo_codes (name, coins, max_activations, creator_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (promo_name, coins, max_activations, user_id, now)
        )
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (creation_cost, user_id))
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        updated_coins = (await cursor.fetchone())[0]
        await db.commit()

    bot_info = await bot.get_me()
    bot_username = bot_info.username
    activation_url = f"https://t.me/{bot_username}?start=promo_{promo_name}"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="promo_back")]
        ]
    )
    text = (
        "🎉 <b>Промокод успешно создан!</b> 🎉\n\n"
        f"✅ <b>Имя промокода:</b> <code>{promo_name}</code>\n"
        f"💰 <b>Награда за активацию:</b> <code>{format_balance(coins)}</code> GG\n"
        f"🔄 <b>Максимум активаций:</b> {max_activations}\n"
        f"💸 <b>Стоимость создания:</b> <code>{format_balance(creation_cost)}</code> GG (списано с вашего баланса)\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(updated_coins)}</code> GG\n\n"
        f"🔗 <b>Ссылка для активации:</b> {activation_url}\n"
        "Или просто используйте команду: /promo {promo_name}\n\n"
        "🌟 Распространяйте промокод среди друзей для большего эффекта!"
    ).format(promo_name=promo_name)
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()


@dp.callback_query(lambda c: c.data == "promo_my")
async def promo_my(call: types.CallbackQuery):
    user_id = call.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await call.answer(
                "❌ Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                show_alert=True)
            return
        user_coins = user_data[1]

        cursor = await db.execute(
            "SELECT promo_id, name FROM promo_codes WHERE creator_id = ?",
            (user_id,)
        )
        promos = await cursor.fetchall()

    if not promos:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться назад", callback_data="promo_back")]
            ]
        )
        text = (
            "📋 <b>Мои промокоды</b> 📋\n\n"
            f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
            "❌ У вас пока нет созданных промокодов. Создайте новый в меню!"
        )
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer()
        return

    kb_rows = []
    for promo_id, name in promos:
        kb_rows.append([InlineKeyboardButton(text=f"🔹 {name}", callback_data=f"promo_detail:{promo_id}")])

    kb_rows.append([InlineKeyboardButton(text="⬅️ Вернуться назад", callback_data="promo_back")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    text = (
        "📋 <b>Список моих промокодов</b> 📋\n\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
        "Выберите промокод ниже, чтобы просмотреть детали или управлять им:"
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("promo_detail:"))
async def promo_detail(call: types.CallbackQuery):
    user_id = call.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await call.answer(
                "❌ Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                show_alert=True)
            return
        user_coins = user_data[1]

        promo_id = int(call.data.split(":")[1])
        cursor = await db.execute(
            "SELECT name, coins, max_activations, activations FROM promo_codes WHERE promo_id = ? AND creator_id = ?",
            (promo_id, user_id)
        )
        promo = await cursor.fetchone()
        if not promo:
            await call.answer("❌ Промокод не найден. Возможно, он был удален.", show_alert=True)
            return

    name, coins, max_activations, activations = promo
    remaining = max_activations - activations

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить промокод", callback_data=f"promo_delete:{promo_id}")],
            [InlineKeyboardButton(text="⬅️ Вернуться назад", callback_data="promo_my")]
        ]
    )
    text = (
        "📋 <b>Детали промокода</b> 📋\n\n"
        f"✅ <b>Имя:</b> <code>{name}</code>\n"
        f"💰 <b>Награда за активацию:</b> <code>{format_balance(coins)}</code> GG\n"
        f"🔄 <b>Всего активаций:</b> {max_activations}\n"
        f"🔄 <b>Использовано:</b> {activations}\n"
        f"🔄 <b>Осталось:</b> {remaining}\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
        "⚠️ <b>Подсказка:</b> Если остаток активаций > 0, при удалении вы получите возврат средств."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("promo_delete:"))
async def promo_delete(call: types.CallbackQuery):
    user_id = call.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await call.answer(
                "❌ Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                show_alert=True)
            return
        user_coins = user_data[1]

        promo_id = int(call.data.split(":")[1])
        cursor = await db.execute(
            "SELECT coins, max_activations, activations FROM promo_codes WHERE promo_id = ? AND creator_id = ?",
            (promo_id, user_id)
        )
        promo = await cursor.fetchone()
        if not promo:
            await call.answer("❌ Промокод не найден. Возможно, он уже удален.", show_alert=True)
            return

        coins, max_activations, activations = promo
        remaining = max_activations - activations
        refund = coins * remaining

        await db.execute("DELETE FROM promo_codes WHERE promo_id = ?", (promo_id,))
        await db.execute("DELETE FROM promo_activations WHERE promo_id = ?", (promo_id,))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (refund, user_id))
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        updated_coins = (await cursor.fetchone())[0]
        await db.commit()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться назад", callback_data="promo_my")]
        ]
    )
    text = (
        "🗑 <b>Промокод успешно удален!</b> 🗑\n\n"
        f"💰 <b>Возврат средств:</b> <code>{format_balance(refund)}</code> GG (за неиспользованные активации)\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(updated_coins)}</code> GG\n\n"
        "🌟 Теперь вы можете создать новый промокод."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data == "promo_back")
async def promo_back(call: types.CallbackQuery):
    user_id = call.from_user.id

    # Check if user exists and get balance
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, coins FROM users WHERE user_id = ?", (user_id,))
        user_data = await cursor.fetchone()
        if not user_data:
            await call.answer(
                "❌ Пользователь не найден. Пожалуйста, начните с команды /start, чтобы зарегистрироваться в боте.",
                show_alert=True)
            return
        user_coins = user_data[1]

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✨ Создать новый промокод", callback_data="promo_create")],
            [InlineKeyboardButton(text="📋 Мои промокоды", callback_data="promo_my")]
        ]
    )
    text = (
        "🌟 <b>Меню промокодов</b> 🌟\n\n"
        f"💰 <b>Ваш текущий баланс:</b> <code>{format_balance(user_coins)}</code> GG\n\n"
        "Здесь вы можете активировать, создавать или просматривать свои промокоды.\n\n"
        "🔹 <b>Активация:</b> Используйте /promo [имя_промокода] для получения наград.\n"
        "🔹 Выберите действие ниже:"
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.message(lambda m: m.text and m.text.lower().startswith(("промо", "промокод")))
async def txt_promo(message: types.Message):
    await cmd_promo(message)


# =================================== КРИПТОВАЛЮТА FEZCOIN ===========================


class CryptoCreateSell(StatesGroup):
    amount = State()
    price = State()

class CryptoCreateBuy(StatesGroup):
    amount = State()
    price = State()

class CryptoFulfillBuyFromSell(StatesGroup):
    amount = State()

class CryptoFulfillSellToBuy(StatesGroup):
    amount = State()


async def return_to_main_menu(message: types.Message, state: FSMContext):
    """Возвращает в главное меню криптовалюты."""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📈 Создать ордер", callback_data="crypto_create_order")
            ],
            [
                InlineKeyboardButton(text="🛒 Рынок", callback_data="crypto_market"),
                InlineKeyboardButton(text="📋 Мои ордера", callback_data="crypto_myorders")
            ],
        ]
    )
    text = (
        "✨ <b>Fezcoin: Криптовалютная биржа</b> ✨\n\n"
        "💎 Добро пожаловать в мир Fezcoin! Покупайте, продавайте и обменивайте валюту с другими игроками.\n\n"
        "➡️ <b>Как это работает?</b>\n"
        "— Создавайте ордера на продажу или покупку (комиссия 5% при исполнении).\n"
        "— Исполняйте ордера на рынке.\n"
        "— Безопасные сделки.\n\n"
        "➡️ <b>Как получить Fezcoin?</b>\n"
        "— Торговля, авто-ферма, покупка (/donat).\n"
        "— Скоро: события и майнинг!\n\n"
        "➡️ <b>Советы:</b>\n"
        "— Следите за рынком.\n"
        "— Торгуйте с умом!\n\n"
        "═ Выберите действие: ═"
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()

@dp.message(Command("crypto"))
async def cmd_crypto(message: types.Message, state: FSMContext):
    """Обработчик команды /crypto. Работает только в личных сообщениях."""
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /crypto доступна только в личных сообщениях с ботом!",
            parse_mode="HTML"
        )
        return

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
    state_key = StorageKey(bot_id=bot.id, chat_id=message.chat.id, user_id=message.from_user.id)
    state = FSMContext(dp.storage, key=state_key)
    await return_to_main_menu(message, state)

@dp.message(lambda m: m.text and m.text.lower() in ["крипта"])
async def txt_crypto(message: types.Message, state: FSMContext):
    """Обработчик текстового ввода 'крипта'. Работает только в личных сообщениях."""
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда 'крипта' доступна только в личных сообщениях с ботом!",
            parse_mode="HTML"
        )
        return

    await cmd_crypto(message, state)

@dp.callback_query(lambda c: c.data == "crypto_create_order")
async def crypto_create_order(call: types.CallbackQuery, state: FSMContext):
    """Подменю создания ордера."""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📈 На продажу", callback_data="crypto_sell_order")],
            [InlineKeyboardButton(text="📉 На покупку", callback_data="crypto_buy_order")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_back")],
        ]
    )
    text = (
        "📈 <b>Создание ордера</b> 📈\n\n"
        "Выберите тип ордера:\n\n"
        "💡 <b>Продажа:</b> Выставьте Fezcoin на продажу за GG.\n"
        "💡 <b>Покупка:</b> Запросите Fezcoin, заблокировав GG на эскроу."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda c: c.data == "crypto_sell_order")
async def crypto_sell_order(call: types.CallbackQuery, state: FSMContext):
    """Обработчик создания ордера на продажу."""
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        fezcoin = result[0] if result else 0
        cursor = await db.execute("SELECT COUNT(*) FROM fez_orders WHERE seller_id = ? AND status = 'open' AND order_type = 'sell'", (user_id,))
        active_orders = (await cursor.fetchone())[0]
        if active_orders > 0:
            await call.answer("❌ У вас уже есть активный ордер на продажу!", show_alert=True)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_create_order")]
                ]
            )
            await call.message.edit_text(
                "🚫 <b>Ограничение</b> 🚫\n\n"
                "У вас уже есть активный ордер на продажу.\n"
                "📌 Отмените его в разделе 'Мои ордера' и попробуйте снова.\n\n"
                "💡 <b>Совет:</b> Управляйте своими ордерами, чтобы не пропустить выгодные сделки!",
                reply_markup=kb, parse_mode="HTML"
            )
            return
    if fezcoin <= 0:
        await call.answer("❌ У вас нет Fezcoin для продажи.", show_alert=True)
        return
    await state.set_state(CryptoCreateSell.amount)
    await state.update_data(user_id=user_id)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_create_order")]
        ]
    )
    text = (
        "📈 <b>Создание ордера на продажу</b> 📈\n\n"
        f"💎 Ваш баланс: <code>{format_balance(fezcoin)}</code> Fezcoin\n"
        "➡️ Введите количество Fezcoin для продажи (целое число, минимум 1):\n\n"
        "💡 <b>Совет:</b> Убедитесь, что у вас достаточно Fezcoin на балансе."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CryptoCreateSell.amount)
async def process_create_sell_amount(message: types.Message, state: FSMContext):
    """Обработка ввода количества для ордера на продажу."""
    amount = parse_bet_input(message.text)
    data = await state.get_data()
    user_id = data.get("user_id")
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        fezcoin = result[0] if result else 0
    if amount < 1 or amount > fezcoin:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
            ]
        )
        await message.reply(
            "🚫 <b>Ошибка ввода</b> 🚫\n\n"
            f"Вы ввели некорректное количество Fezcoin. Должно быть целое число от 1 до {format_balance(fezcoin)}.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Проверьте ваш баланс и убедитесь, что вводите правильное значение.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return
    await state.update_data(amount=amount)
    await state.set_state(CryptoCreateSell.price)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_sell_order")]
        ]
    )
    text = (
        "📈 <b>Укажите цену продажи</b> 📈\n\n"
        f"💎 Количество для продажи: <code>{format_balance(amount)}</code> Fezcoin\n"
        "➡️ Введите цену за 1 Fezcoin (в GG, минимум 100):\n\n"
        "💡 <b>Совет:</b> Установите конкурентную цену, чтобы привлечь покупателей!"
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")

@dp.message(CryptoCreateSell.price)
async def process_create_sell_price(message: types.Message, state: FSMContext):
    """Обработка ввода цены для ордера на продажу."""
    price = parse_bet_input(message.text)
    data = await state.get_data()
    user_id = data.get("user_id")
    amount = data.get("amount")
    if price < 100:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
            ]
        )
        await message.reply(
            "🚫 <b>Ошибка ввода</b> 🚫\n\n"
            "Цена за 1 Fezcoin должна быть не менее 100 GG.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Убедитесь, что цена соответствует рыночным условиям.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return
    now = datetime.now(ZoneInfo("UTC")).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO fez_orders (seller_id, amount, price, created_at, order_type) VALUES (?, ?, ?, ?, 'sell')",
            (user_id, amount, price, now)
        )
        await db.execute("UPDATE users SET fezcoin = fezcoin - ? WHERE user_id = ?", (amount, user_id))
        await db.commit()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
        ]
    )
    text = (
        "🎉 <b>Ордер на продажу успешно создан!</b> 🎉\n\n"
        f"💎 Выставлено на продажу: <code>{format_balance(amount)}</code> Fezcoin\n"
        f"💰 Цена за 1 Fezcoin: <code>{format_balance(price)}</code> GG\n"
        f"═ Итоговая сумма: <code>{format_balance(amount * price)}</code> GG\n\n"
        "📌 Проверьте статус ордера в разделе 'Мои ордера'.\n"
        "⚠️ <b>Примечание:</b> При исполнении ордера взимается комиссия 5%."
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()

@dp.callback_query(lambda c: c.data == "crypto_buy_order")
async def crypto_buy_order(call: types.CallbackQuery, state: FSMContext):
    """Обработчик создания ордера на покупку."""
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        coins = result[0] if result else 0
        cursor = await db.execute("SELECT COUNT(*) FROM fez_orders WHERE buyer_id = ? AND status = 'open' AND order_type = 'buy'", (user_id,))
        active_orders = (await cursor.fetchone())[0]
        if active_orders > 0:
            await call.answer("❌ У вас уже есть активный ордер на покупку!", show_alert=True)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_create_order")]
                ]
            )
            await call.message.edit_text(
                "🚫 <b>Ограничение</b> 🚫\n\n"
                "У вас уже есть активный ордер на покупку.\n"
                "📌 Отмените его в разделе 'Мои ордера' и попробуйте снова.\n\n"
                "💡 <b>Совет:</b> Управляйте своими ордерами, чтобы не пропустить выгодные сделки!",
                reply_markup=kb, parse_mode="HTML"
            )
            return
    if coins < 100:
        await call.answer("❌ У вас недостаточно GG для создания ордера.", show_alert=True)
        return
    await state.set_state(CryptoCreateBuy.amount)
    await state.update_data(user_id=user_id)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_create_order")]
        ]
    )
    text = (
        "📉 <b>Создание ордера на покупку</b> 📉\n\n"
        f"💰 Ваш баланс: <code>{format_balance(coins)}</code> GG\n"
        "➡️ Введите количество Fezcoin для покупки (целое число, минимум 1):\n\n"
        "💡 <b>Совет:</b> Убедитесь, что у вас достаточно GG для покупки."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CryptoCreateBuy.amount)
async def process_create_buy_amount(message: types.Message, state: FSMContext):
    """Обработка ввода количества для ордера на покупку."""
    amount = parse_bet_input(message.text)
    if amount < 1:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
            ]
        )
        await message.reply(
            "🚫 <b>Ошибка ввода</b> 🚫\n\n"
            "Количество Fezcoin должно быть целым числом не менее 1.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Убедитесь, что вводите корректное целое число.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return
    await state.update_data(amount=amount)
    await state.set_state(CryptoCreateBuy.price)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_buy_order")]
        ]
    )
    text = (
        "📉 <b>Укажите цену покупки</b> 📉\n\n"
        f"💎 Количество для покупки: <code>{format_balance(amount)}</code> Fezcoin\n"
        "➡️ Введите цену за 1 Fezcoin (в GG, минимум 100):\n\n"
        "💡 <b>Совет:</b> Проверьте текущие рыночные цены, чтобы установить выгодную цену."
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")

@dp.message(CryptoCreateBuy.price)
async def process_create_buy_price(message: types.Message, state: FSMContext):
    """Обработка ввода цены для ордера на покупку."""
    price = parse_bet_input(message.text)
    data = await state.get_data()
    user_id = data.get("user_id")
    amount = data.get("amount")
    if price < 100:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
            ]
        )
        await message.reply(
            "🚫 <b>Ошибка ввода</b> 🚫\n\n"
            "Цена за 1 Fezcoin должна быть не менее 100 GG.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Убедитесь, что цена соответствует вашим ожиданиям и рыночным условиям.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return
    total = amount * price
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        coins = (await cursor.fetchone())[0]
        if total > coins:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
                ]
            )
            await message.reply(
                "🚫 <b>Недостаточно средств</b> 🚫\n\n"
                f"Для покупки <code>{format_balance(amount)}</code> Fezcoin по цене <code>{format_balance(price)}</code> GG требуется <code>{format_balance(total)}</code> GG.\n"
                f"💰 Ваш текущий баланс: <code>{format_balance(coins)}</code> GG.\n"
                "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
                "💡 <b>Совет:</b> Пополните баланс или уменьшите сумму ордера.",
                reply_markup=kb, parse_mode="HTML"
            )
            await state.clear()
            return
        now = datetime.now(ZoneInfo("UTC")).isoformat()
        await db.execute(
            "INSERT INTO fez_orders (buyer_id, amount, price, created_at, order_type) VALUES (?, ?, ?, ?, 'buy')",
            (user_id, amount, price, now)
        )
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (total, user_id))
        await db.execute("UPDATE users SET escrow = escrow + ? WHERE user_id = ?", (total, user_id))
        await db.commit()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_create_order")]
        ]
    )
    text = (
        "🎉 <b>Ордер на покупку успешно создан!</b> 🎉\n\n"
        f"💎 Запрошено: <code>{format_balance(amount)}</code> Fezcoin\n"
        f"💰 Цена за 1 Fezcoin: <code>{format_balance(price)}</code> GG\n"
        f"═ Итоговая сумма: <code>{format_balance(total)}</code> GG (заблокировано на эскроу)\n\n"
        "📌 Проверьте статус ордера в разделе 'Мои ордера'.\n"
        "⚠️ <b>Примечание:</b> При исполнении ордера взимается комиссия 5%."
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()

@dp.callback_query(lambda c: c.data == "crypto_market")
async def crypto_market(call: types.CallbackQuery):
    """Подменю рынка."""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📈 Купить", callback_data="crypto_sell_orders_page:0")],
            [InlineKeyboardButton(text="📉 Продать", callback_data="crypto_buy_orders_page:0")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_back")],
        ]
    )
    text = (
        "🛒 <b>Рынок Fezcoin</b> 🛒\n\n"
        "Выберите тип ордеров для просмотра:\n\n"
        "💡 <b>Ордера на покупку:</b> Найдите запросы на покупку Fezcoin.\n"
        "💡 <b>Ордера на продажу:</b> Просмотрите доступные предложения Fezcoin."

    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("crypto_sell_orders_page"))
async def crypto_sell_orders_page(call: types.CallbackQuery):
    """Отображение страниц с ордерами на продажу."""
    _, page = call.data.split(":")
    page = int(page)
    orders_per_page = 7
    offset = page * orders_per_page
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM fez_orders WHERE status = 'open' AND order_type = 'sell'")
        total_orders = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT order_id, seller_id, amount, price FROM fez_orders WHERE status = 'open' AND order_type = 'sell' ORDER BY price ASC LIMIT ? OFFSET ?",
            (orders_per_page, offset)
        )
        rows = await cursor.fetchall()

    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_market")]
            ]
        )
        text = (
            "🛒 <b>Ордера на продажу</b> 🛒\n\n"
            "❌ Нет открытых ордеров на продажу.\n"
            "➡️ Создайте свой ордер в разделе 'Создать ордер'!\n\n"
            "💡 <b>Совет:</b> Будьте первым, чтобы предложить Fezcoin по выгодной цене!"
        )
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer()
        return

    text = (
        "🛒 <b>Ордера на продажу</b> 🛒\n\n"
        f"📄 Страница {page + 1} из {max(1, (total_orders + orders_per_page - 1) // orders_per_page)}\n"
        "➡️ Выберите ордер для покупки:"
    )
    kb_rows = []
    for idx, (order_id, seller_id, amount, price) in enumerate(rows, start=1):
        try:
            seller = await bot.get_chat(seller_id)
            seller_name = seller.first_name or f"ID {seller_id}"
        except Exception:
            seller_name = f"ID {seller_id}"
        display_name = seller_name[:9] + "..." if len(seller_name) > 9 else seller_name
        button_text = f"{display_name}, {format_balance(price)} GG ({format_balance(amount)} Fez)"
        kb_rows.append([InlineKeyboardButton(text=button_text, callback_data=f"crypto_buy_from_sell:{order_id}:{page}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Пред.", callback_data=f"crypto_sell_orders_page:{page - 1}"))
    if total_orders > offset + orders_per_page:
        nav_buttons.append(InlineKeyboardButton(text="След. ➡️", callback_data=f"crypto_sell_orders_page:{page + 1}"))
    if nav_buttons:
        kb_rows.append(nav_buttons)
    kb_rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="crypto_market")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("crypto_buy_orders_page"))
async def crypto_buy_orders_page(call: types.CallbackQuery):
    """Отображение страниц с ордерами на покупку."""
    _, page = call.data.split(":")
    page = int(page)
    orders_per_page = 7
    offset = page * orders_per_page
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM fez_orders WHERE status = 'open' AND order_type = 'buy'")
        total_orders = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT order_id, buyer_id, amount, price FROM fez_orders WHERE status = 'open' AND order_type = 'buy' ORDER BY price DESC LIMIT ? OFFSET ?",
            (orders_per_page, offset)
        )
        rows = await cursor.fetchall()

    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_market")]
            ]
        )
        text = (
            "🛒 <b>Ордера на покупку</b> 🛒\n\n"
            "❌ Нет открытых ордеров на покупку.\n"
            "➡️ Создайте свой ордер в разделе 'Создать ордер'!\n\n"
            "💡 <b>Совет:</b> Запросите Fezcoin по своей цене, чтобы привлечь продавцов!"
        )
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer()
        return

    text = (
        "🛒 <b>Ордера на покупку</b> 🛒\n\n"
        f"📄 Страница {page + 1} из {max(1, (total_orders + orders_per_page - 1) // orders_per_page)}\n"
        "➡️ Выберите ордер для продажи:"
    )
    kb_rows = []
    for idx, (order_id, buyer_id, amount, price) in enumerate(rows, start=1):
        try:
            buyer = await bot.get_chat(buyer_id)
            buyer_name = buyer.first_name or f"ID {buyer_id}"
        except Exception:
            buyer_name = f"ID {buyer_id}"
        display_name = buyer_name[:9] + "..." if len(buyer_name) > 9 else buyer_name
        button_text = f"{display_name}, {format_balance(price)} GG ({format_balance(amount)} Fez)"
        kb_rows.append([InlineKeyboardButton(text=button_text, callback_data=f"crypto_sell_to_buy:{order_id}:{page}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Пред.", callback_data=f"crypto_buy_orders_page:{page - 1}"))
    if total_orders > offset + orders_per_page:
        nav_buttons.append(InlineKeyboardButton(text="След. ➡️", callback_data=f"crypto_buy_orders_page:{page + 1}"))
    if nav_buttons:
        kb_rows.append(nav_buttons)
    kb_rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="crypto_market")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("crypto_buy_from_sell"))
async def crypto_buy_from_sell(call: types.CallbackQuery, state: FSMContext):
    """Подтверждение покупки из ордера на продажу."""
    _, order_id, page = call.data.split(":")
    order_id = int(order_id)
    page = int(page)
    buyer_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT seller_id, amount, price, status, order_type FROM fez_orders WHERE order_id = ?",
                                 (order_id,))
        result = await cursor.fetchone()
        if not result or result[3] != 'open' or result[4] != 'sell':
            await call.answer("❌ Ордер не найден или закрыт.", show_alert=True)
            return
        seller_id, amount, price, _, _ = result
        if seller_id == buyer_id:
            await call.answer("❌ Нельзя исполнить свой ордер.", show_alert=True)
            return
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (buyer_id,))
        buyer_coins = (await cursor.fetchone())[0]
        try:
            seller = await bot.get_chat(seller_id)
            seller_name = seller.first_name or f"ID {seller_id}"
        except Exception:
            seller_name = f"ID {seller_id}"

    await state.set_state(CryptoFulfillBuyFromSell.amount)
    await state.update_data(order_id=order_id, page=page, buyer_id=buyer_id, seller_id=seller_id, max_amount=amount, price=price)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"crypto_sell_orders_page:{page}")]
        ]
    )
    text = (
        "🛒 <b>Покупка Fezcoin</b> 🛒\n\n"
        f"📄 Ордер #{order_id}\n"
        f"👤 Продавец: {seller_name}\n"
        f"💎 Доступно: <code>{format_balance(amount)}</code> Fezcoin\n"
        f"💰 Цена за 1 Fezcoin: <code>{format_balance(price)}</code> GG\n"
        f"💰 Ваш баланс: <code>{format_balance(buyer_coins)}</code> GG\n\n"
        f"➡️ Введите количество Fezcoin, которое хотите купить (целое число, от 1 до {format_balance(amount)}):\n\n"
        "💡 <b>Совет:</b> Убедитесь, что у вас достаточно GG для покупки."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CryptoFulfillBuyFromSell.amount)
async def process_fulfill_buy_from_sell(message: types.Message, state: FSMContext):
    """Обработка количества для покупки из ордера на продажу."""
    amount = parse_bet_input(message.text)
    data = await state.get_data()
    order_id = data.get("order_id")
    page = data.get("page")
    buyer_id = data.get("buyer_id")
    seller_id = data.get("seller_id")
    max_amount = data.get("max_amount")
    price = data.get("price")

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (buyer_id,))
        buyer_coins = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT amount, status FROM fez_orders WHERE order_id = ?", (order_id,))
        result = await cursor.fetchone()
        if not result or result[1] != 'open':
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
                ]
            )
            await message.reply(
                "🚫 <b>Ошибка</b> 🚫\n\n"
                "Ордер не найден или уже закрыт.\n"
                "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и выберите другой ордер.",
                reply_markup=kb, parse_mode="HTML"
            )
            await state.clear()
            return
        current_amount = result[0]

    if amount < 1 or amount > max_amount or amount > current_amount:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
            ]
        )
        await message.reply(
            "🚫 <b>Ошибка ввода</b> 🚫\n\n"
            f"Вы ввели некорректное количество. Должно быть целое число от 1 до {format_balance(min(max_amount, current_amount))} Fezcoin.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Проверьте доступное количество в ордере перед вводом.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return
    total_cost = amount * price
    if buyer_coins < total_cost:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
            ]
        )
        await message.reply(
            "🚫 <b>Недостаточно средств</b> 🚫\n\n"
            f"Для покупки <code>{format_balance(amount)}</code> Fezcoin требуется <code>{format_balance(total_cost)}</code> GG.\n"
            f"💰 Ваш текущий баланс: <code>{format_balance(buyer_coins)}</code> GG.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Пополните баланс или уменьшите количество.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return

    async with aiosqlite.connect(DB_PATH) as db:
        commission = math.floor(total_cost * 0.05)
        seller_receives = total_cost - commission
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (total_cost, buyer_id))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (seller_receives, seller_id))
        await db.execute("UPDATE users SET fezcoin = fezcoin + ? WHERE user_id = ?", (amount, buyer_id))
        await db.execute("UPDATE users SET fezcoin_sold = fezcoin_sold + ? WHERE user_id = ?", (amount, seller_id))
        new_order_amount = current_amount - amount
        if new_order_amount <= 0:
            await db.execute("UPDATE fez_orders SET status = 'closed' WHERE order_id = ?", (order_id,))
        else:
            await db.execute("UPDATE fez_orders SET amount = ? WHERE order_id = ?", (new_order_amount, order_id))
        await db.commit()

    try:
        seller = await bot.get_chat(seller_id)
        seller_name = seller.first_name or f"ID {seller_id}"
    except Exception:
        seller_name = f"ID {seller_id}"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
        ]
    )
    text = (
        "🎉 <b>Покупка успешно завершена!</b> 🎉\n\n"
        f"📄 Ордер #{order_id}\n"
        f"👤 Продавец: {seller_name}\n"
        f"💎 Куплено: <code>{format_balance(amount)}</code> Fezcoin\n"
        f"💰 Потрачено: <code>{format_balance(total_cost)}</code> GG\n"
        f"📊 Комиссия 5%: <code>{format_balance(commission)}</code> GG\n\n"
        "📌 Проверьте ваш баланс в разделе 'Мои ордера'."
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    try:
        await bot.send_message(
            seller_id,
            f"🎉 <b>Ордер #{order_id} исполнен!</b> 🎉\n\n"
            f"💎 Продано: <code>{format_balance(amount)}</code> Fezcoin\n"
            f"💰 Вы получили: <code>{format_balance(seller_receives)}</code> GG\n"
            f"📊 Комиссия 5%: <code>{format_balance(commission)}</code> GG\n\n"
            "📌 Проверьте ваш баланс в разделе 'Мои ордера'.",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("crypto_sell_to_buy"))
async def crypto_sell_to_buy(call: types.CallbackQuery, state: FSMContext):
    """Подтверждение продажи в ордер на покупку."""
    _, order_id, page = call.data.split(":")
    order_id = int(order_id)
    page = int(page)
    seller_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT buyer_id, amount, price, status, order_type FROM fez_orders WHERE order_id = ?",
                                 (order_id,))
        result = await cursor.fetchone()
        if not result or result[3] != 'open' or result[4] != 'buy':
            await call.answer("❌ Ордер не найден или закрыт.", show_alert=True)
            return
        buyer_id, amount, price, _, _ = result
        if buyer_id == seller_id:
            await call.answer("❌ Нельзя исполнить свой ордер.", show_alert=True)
            return
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (seller_id,))
        seller_fez = (await cursor.fetchone())[0]
        try:
            buyer = await bot.get_chat(buyer_id)
            buyer_name = buyer.first_name or f"ID {buyer_id}"
        except Exception:
            buyer_name = f"ID {buyer_id}"

    await state.set_state(CryptoFulfillSellToBuy.amount)
    await state.update_data(order_id=order_id, page=page, seller_id=seller_id, buyer_id=buyer_id, max_amount=amount, price=price)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"crypto_buy_orders_page:{page}")]
        ]
    )
    text = (
        "🛒 <b>Продажа Fezcoin</b> 🛒\n\n"
        f"📄 Ордер #{order_id}\n"
        f"👤 Покупатель: {buyer_name}\n"
        f"💎 Доступно для продажи: <code>{format_balance(amount)}</code> Fezcoin\n"
        f"💰 Цена за 1 Fezcoin: <code>{format_balance(price)}</code> GG\n"
        f"💎 Ваш баланс Fezcoin: <code>{format_balance(seller_fez)}</code>\n\n"
        f"➡️ Введите количество Fezcoin, которое хотите продать (целое число, от 1 до {format_balance(amount)}):\n\n"
        "💡 <b>Совет:</b> Убедитесь, что у вас достаточно Fezcoin для продажи."
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CryptoFulfillSellToBuy.amount)
async def process_fulfill_sell_to_buy(message: types.Message, state: FSMContext):
    """Обработка количества для продажи в ордер на покупку."""
    amount = parse_bet_input(message.text)
    data = await state.get_data()
    order_id = data.get("order_id")
    page = data.get("page")
    seller_id = data.get("seller_id")
    buyer_id = data.get("buyer_id")
    max_amount = data.get("max_amount")
    price = data.get("price")

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (seller_id,))
        seller_fez = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT amount, status FROM fez_orders WHERE order_id = ?", (order_id,))
        result = await cursor.fetchone()
        if not result or result[1] != 'open':
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
                ]
            )
            await message.reply(
                "🚫 <b>Ошибка</b> 🚫\n\n"
                "Ордер не найден или уже закрыт.\n"
                "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и выберите другой ордер.",
                reply_markup=kb, parse_mode="HTML"
            )
            await state.clear()
            return
        current_amount = result[0]

    if amount < 1 or amount > max_amount or amount > current_amount or amount > seller_fez:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
            ]
        )
        await message.reply(
            "🚫 <b>Ошибка ввода</b> 🚫\n\n"
            f"Вы ввели некорректное количество. Должно быть целое число от 1 до {format_balance(min(max_amount, current_amount, seller_fez))} Fezcoin.\n"
            "❗ <b>Ввод завершен.</b> Пожалуйста, вернитесь в меню и попробуйте снова.\n\n"
            "💡 <b>Совет:</b> Проверьте ваш баланс Fezcoin и доступное количество в ордере.",
            reply_markup=kb, parse_mode="HTML"
        )
        await state.clear()
        return

    total_coins = amount * price
    async with aiosqlite.connect(DB_PATH) as db:
        commission = math.floor(total_coins * 0.05)
        seller_receives = total_coins - commission
        await db.execute("UPDATE users SET fezcoin = fezcoin - ? WHERE user_id = ?", (amount, seller_id))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (seller_receives, seller_id))
        await db.execute("UPDATE users SET fezcoin = fezcoin + ? WHERE user_id = ?", (amount, buyer_id))
        await db.execute("UPDATE users SET escrow = escrow - ? WHERE user_id = ?", (total_coins, buyer_id))
        await db.execute("UPDATE users SET fezcoin_sold = fezcoin_sold + ? WHERE user_id = ?", (amount, seller_id))
        new_order_amount = current_amount - amount
        if new_order_amount <= 0:
            await db.execute("UPDATE fez_orders SET status = 'closed' WHERE order_id = ?", (order_id,))
        else:
            await db.execute("UPDATE fez_orders SET amount = ? WHERE order_id = ?", (new_order_amount, order_id))
        await db.commit()

    try:
        buyer = await bot.get_chat(buyer_id)
        buyer_name = buyer.first_name or f"ID {buyer_id}"
    except Exception:
        buyer_name = f"ID {buyer_id}"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="crypto_back")]
        ]
    )
    text = (
        "🎉 <b>Продажа успешно завершена!</b> 🎉\n\n"
        f"📄 Ордер #{order_id}\n"
        f"👤 Покупатель: {buyer_name}\n"
        f"💎 Продано: <code>{format_balance(amount)}</code> Fezcoin\n"
        f"💰 Получено: <code>{format_balance(seller_receives)}</code> GG\n"
        f"📊 Комиссия 5%: <code>{format_balance(commission)}</code> GG\n\n"
        "📌 Проверьте ваш баланс в разделе 'Мои ордера'."
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    try:
        await bot.send_message(
            buyer_id,
            f"🎉 <b>Ордер #{order_id} исполнен!</b> 🎉\n\n"
            f"💎 Куплено: <code>{format_balance(amount)}</code> Fezcoin\n"
            f"💰 Заплачено: <code>{format_balance(total_coins)}</code> GG\n"
            f"📊 Комиссия 5%: <code>{format_balance(commission)}</code> GG\n\n"
            "📌 Проверьте ваш баланс в разделе 'Мои ордера'.",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await state.clear()

@dp.callback_query(lambda c: c.data == "crypto_myorders")
async def crypto_myorders(call: types.CallbackQuery):
    """Отображение ордеров пользователя."""
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT order_id, order_type, amount, price, status FROM fez_orders WHERE (seller_id = ? OR buyer_id = ?) AND status = 'open'",
            (user_id, user_id)
        )
        rows = await cursor.fetchall()
    kb_rows = []
    if not rows:
        text = (
            "📋 <b>Мои ордера</b> 📋\n\n"
            "❌ У вас нет активных ордеров.\n"
            "➡️ Создайте новый ордер в разделе 'Создать ордер'!\n\n"
            "💡 <b>Совет:</b> Активные ордера помогут вам быстрее купить или продать Fezcoin!"
        )
    else:
        text = (
            "📋 <b>Мои ордера</b> 📋\n\n"
            "Ваши активные ордера на бирже Fezcoin:\n\n"
        )
        for row in rows:
            order_id, order_type, amount, price, status = row
            type_text = "Продажа" if order_type == 'sell' else "Покупка"
            status_text = "Открыт" if status == 'open' else "Закрыт" if status == 'closed' else "Отменен"
            text += (
                f"💎 Ордер #{order_id} ({type_text}):\n"
                f"Количество: <code>{format_balance(amount)}</code> Fezcoin\n"
                f"Цена: <code>{format_balance(price)}</code> GG\n"
                f"Статус: {status_text}\n\n"
            )
            if status == 'open':
                kb_rows.append([InlineKeyboardButton(text=f"❌ Удалить #{order_id}", callback_data=f"crypto_cancel_order:{order_id}")])

    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_back")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("crypto_cancel_order"))
async def crypto_cancel_order(call: types.CallbackQuery):
    """Обработка отмены ордера."""
    _, order_id = call.data.split(":")
    order_id = int(order_id)
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT seller_id, buyer_id, amount, price, status, order_type FROM fez_orders WHERE order_id = ?", (order_id,))
        result = await cursor.fetchone()
        if not result or result[4] != 'open':
            await call.answer("❌ Ордер не найден или уже закрыт/отменен.", show_alert=True)
            return
        seller_id, buyer_id, amount, price, _, order_type = result
        if (order_type == 'sell' and seller_id != user_id) or (order_type == 'buy' and buyer_id != user_id):
            await call.answer("❌ Вы не можете отменить чужой ордер.", show_alert=True)
            return
        if order_type == 'sell':
            await db.execute("UPDATE users SET fezcoin = fezcoin + ? WHERE user_id = ?", (amount, user_id))
        elif order_type == 'buy':
            total = amount * price
            await db.execute("UPDATE users SET escrow = escrow - ? WHERE user_id = ?", (total, user_id))
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (total, user_id))
        await db.execute("UPDATE fez_orders SET status = 'cancelled' WHERE order_id = ?", (order_id,))
        await db.commit()
    await call.answer("✅ Ордер успешно отменен!", show_alert=True)
    await crypto_myorders(call)

@dp.callback_query(lambda c: c.data == "crypto_back")
async def crypto_back(call: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню криптовалюты."""
    await state.clear()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📈 Создать ордер", callback_data="crypto_create_order")
            ],
            [
                InlineKeyboardButton(text="🛒 Рынок", callback_data="crypto_market"),
                InlineKeyboardButton(text="📋 Мои ордера", callback_data="crypto_myorders")
            ],
        ]
    )
    text = (
        "✨ <b>Fezcoin: Криптовалютная биржа</b> ✨\n\n"
        "💎 Добро пожаловать в мир Fezcoin! Покупайте, продавайте и обменивайте валюту с другими игроками.\n\n"
        "➡️ <b>Как это работает?</b>\n"
        "— Создавайте ордера на продажу или покупку (комиссия 5% при исполнении).\n"
        "— Исполняйте ордера на рынке.\n"
        "— Безопасные сделки.\n\n"
        "➡️ <b>Как получить Fezcoin?</b>\n"
        "— Торговля, авто-ферма, покупка (/donat).\n"
        "— Скоро: события и майнинг!\n\n"
        "➡️ <b>Советы:</b>\n"
        "— Следите за рынком.\n"
        "— Торгуйте с умом!\n\n"
        "═ Выберите действие: ═"
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

# =================================== ПРОФИЛЬ ===========================

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT username, coins, win_amount, lose_amount, fezcoin, fezcoin_sold, status, verified FROM users WHERE user_id = ?", (user_id,)
        )
        result = await cursor.fetchone()
        if not result:
            await message.reply("Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        username, coins, win_amount, lose_amount, fezcoin, fezcoin_sold, status, verified = result
        username = username if username else "—"
        text = (
            "<b>🪪 Ваш Профиль:</b>\n"
            f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
            f"👤 <b>Username:</b> @{username}\n"
            f"💎 <b>Статус:</b> {emojis[status]}\n"
            f"💰 <b>Монет:</b> <code>{format_balance(coins)}</code>\n"
            f"🏆 <b>Выиграно:</b> <code>{format_balance(win_amount)}</code>\n"
            f"💸 <b>Проиграно:</b> <code>{format_balance(lose_amount)}</code>\n\n"
            f"<i>💎 <b>Fezcoin:</b> <code>{format_balance(fezcoin)}</code></i>\n"
            f"<i>📈 <b>Fezcoin продано:</b> <code>{format_balance(fezcoin_sold)}</code></i>\n\n"
        )
        if verified:
            text += "✅ <b><i>Верифицированный аккаунт</i></b>\n"

    await message.reply(text, parse_mode="HTML")

# Команда /ver (верификация пользователя)
@dp.message(Command("ver"))
async def cmd_ver(message: types.Message):
    user_id = message.from_user.id
    chat_type = message.chat.type
    is_private = chat_type == "private"

    # Проверка регистрации
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            if is_private:
                await message.reply(
                    "❌ Вы не зарегистрированы. Введите /start.",
                    parse_mode="HTML"
                )
            return  # Игнорировать в группах

    # Проверка прав администратора
    if user_id != ADMIN_ID:
        if is_private:
            await message.reply("❌ У вас нет прав для этой команды.", parse_mode="HTML")
        return

    # Получаем ID пользователя из команды
    args = message.text.split()
    if len(args) != 2:
        if is_private:
            await message.reply(
                "❌ <b>Неверный формат.</b>\n\n"
                "Используйте: /ver <user_id>\n"
                "Пример: /ver 123456789",
                parse_mode="HTML"
            )
        return

    try:
        target_user_id = int(args[1])
    except ValueError:
        if is_private:
            await message.reply(
                "❌ <b>Ошибка:</b> ID должен быть числом.",
                parse_mode="HTML"
            )
        return

    # Проверяем, существует ли пользователь
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT username, verified FROM users WHERE user_id = ?", (target_user_id,))
        result = await cursor.fetchone()
        if not result:
            if is_private:
                await message.reply(
                    f"❌ Пользователь с ID <code>{target_user_id}</code> не найден.",
                    parse_mode="HTML"
                )
            return

        username, verified = result
        if verified:
            if is_private:
                await message.reply(
                    f"❌ Пользователь @{username or target_user_id} уже верифицирован.",
                    parse_mode="HTML"
                )
            return

        # Устанавливаем верификацию
        await db.execute("UPDATE users SET verified = 1 WHERE user_id = ?", (target_user_id,))
        await db.commit()

    if is_private:
        await message.reply(
            f"✅ Пользователь @{username or target_user_id} успешно верифицирован!",
            parse_mode="HTML"
        )

@dp.message(lambda m: m.text and m.text.lower() in ["профиль", "я"])
async def txt_profile(message: types.Message):
    await cmd_profile(message)


# =================================== СТАТУС ===========================

# Статусы: эмодзи и цены
emojis = ["", "⚡", "🔥", "💥", "🦾", "💣", "🚀", "♠️", "👻", "👑", "💎", "🌟" ,  "🎰", "🎩"]
emoji_prices = [
    0,  # Status 0
    10000,  # Status 1
    25000,  # Status 2
    100000,  # Status 3
    500000,  # Status 4
    2000000,  # Status 5
    7500000,  # Status 6
    25000000,  # Status 7
    100000000,  # Status 8
    250000000,  # Status 9
    1000000000,  # Status 10
    10000000000,  # Status 11
    None,  # Status 12 (Эксклюзивный, только через админа)
    None,  # Status 13 (Эксклюзивный, только через админа)
]

status_bonus_map = {
    0: [50, 100, 150, 200, 250, 300, 350, 400, 450, 500],
    1: [100, 200, 3000, 400, 500, 600, 700, 800, 900, 1000],
    2: [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000],
    3: [400, 800, 1200, 1600, 2000, 2400, 2800, 3200, 3600, 4000],
    4: [800, 1600, 2400, 3200, 4000, 4800, 5600, 6400, 7200, 8000],
    5: [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000],
    6: [3000, 6000, 9000, 12000, 15000, 18000, 21000, 24000, 27000, 30000],
    7: [7000, 14000, 21000, 28000, 35000, 42000, 49000, 56000, 63000, 70000],
    8: [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000],
    9: [50000, 100000, 150000, 200000, 250000, 300000, 350000, 400000, 450000, 500000],
    10: [100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000],
    11: [300000, 600000, 900000, 1200000, 1500000, 1800000, 2100000, 2400000, 2700000, 3000000],
    12: [500000, 1000000, 1500000, 2000000, 2500000, 3000000, 3500000, 4000000, 4500000, 5000000],
    13: [700000, 1400000, 2100000, 2800000, 3500000, 4200000, 4900000, 5600000, 6300000, 7000000],
}

ADMIN_IDS = [6492780518]  # Список ID админов (замените на реальные)


async def show_status_list(message: types.Message, user_coins: int, current_status: int):
    """Показывает список доступных статусов."""
    text = (
        "✨ <b>Покупка статуса</b> ✨\n\n"
        "🔹 Статусы дают уникальные эмодзи и увеличивают бонусы!\n"
        "🔸 Используйте: <code>/buy [номер]</code> или <code>купить [номер]</code>\n\n"
        "<b>Доступные статусы:</b>\n"
    )
    for i, emoji in enumerate(emojis):
        if i == 0:
            continue  # Пропускаем пустой статус
        price = emoji_prices[i]
        if i <= 11:
            text += f"{i}. {emoji} — <code>{format_balance(price)}</code> GG\n"
        else:
            text += f"<b>{i}. {emoji} — Эксклюзивный (платный) 🔒</b>\n"
    text += (
        f"\n💰 Ваш баланс: <code>{format_balance(user_coins)}</code> GG\n"
        f"💎 Текущий статус: {emojis[current_status]}\n"
        "➡️ Укажите номер статуса для покупки (1-11)."
    )
    await message.reply(text, parse_mode="HTML")


async def buy_status_logic(message: types.Message, status_id: int):
    """Логика покупки статуса."""
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins, status FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_coins, current_status = result

    if status_id < 1 or status_id > 11:
        await message.reply(
            "❌ Неверный номер статуса. Выберите от 1 до 11. Статусы 12-14 — эксклюзивные, их можно получить только через администратора.",
            parse_mode="HTML"
        )
        return
    if status_id <= current_status:
        await message.reply(
            f"❌ Ваш текущий статус ({emojis[current_status]}) равен или выше выбранного!",
            parse_mode="HTML"
        )
        return
    price = emoji_prices[status_id]
    if user_coins < price:
        await message.reply(
            f"❌ Недостаточно GG! Нужно: <code>{format_balance(price)}</code> GG",
            parse_mode="HTML"
        )
        return

    # Обновление статуса и списание монет
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET status = ?, coins = coins - ? WHERE user_id = ?",
            (status_id, price, user_id)
        )
        await db.commit()

    await message.reply(
        f"🎉 <b>Статус куплен!</b>\n\n"
        f"💎 Новый статус: {emojis[status_id]}\n"
        f"💰 Потрачено: <code>{format_balance(price)}</code> GG\n"
        f"📊 Новый баланс: <code>{format_balance(user_coins - price)}</code> GG\n"
        f"🔹 Теперь ваши бонусы стали больше!",
        parse_mode="HTML"
    )


@dp.message(Command(commands=["buy"]))
async def cmd_buy_status(message: types.Message):
    user_id = message.from_user.id
    args = message.text.strip().split()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins, status FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_coins, current_status = result

    # Показать список статусов, если команда без аргументов
    if len(args) < 2:
        await show_status_list(message, user_coins, current_status)
        return

    # Обработка покупки статуса
    try:
        status_id = int(args[1])
        await buy_status_logic(message, status_id)
    except ValueError:
        await message.reply(
            "❌ Укажите корректный номер статуса (число от 1 до 11).",
            parse_mode="HTML"
        )


@dp.message(lambda m: m.text and m.text.lower().startswith("купить"))
async def txt_buy_status(message: types.Message):
    text_parts = message.text.strip().lower().split()

    # Если просто "купить", показываем список статусов
    if len(text_parts) == 1:
        user_id = message.from_user.id
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT coins, status FROM users WHERE user_id = ?", (user_id,))
            result = await cursor.fetchone()
            if not result:
                await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
                return
            user_coins, current_status = result
        await show_status_list(message, user_coins, current_status)
        return

    # Если "купить [номер]", обрабатываем номер
    if len(text_parts) >= 2:
        try:
            status_id = int(text_parts[1])
            await buy_status_logic(message, status_id)
        except ValueError:
            await message.reply(
                "❌ Укажите корректный номер статуса после 'купить' (число от 1 до 11).",
                parse_mode="HTML"
            )
        return


@dp.message(lambda m: m.text and (m.text.lower() == "статус" or m.text.lower() == "/status"))
async def txt_status(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins, status FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_coins, current_status = result
    await show_status_list(message, user_coins, current_status)


@dp.message(Command("set_status"))
async def cmd_set_status(message: types.Message):
    user_id = message.from_user.id
    args = message.text.split()

    # Проверка, является ли пользователь админом
    if user_id not in ADMIN_IDS:
        await message.reply(
            "❌ <b>Ошибка:</b> Эта команда доступна только администраторам.",
            parse_mode="HTML"
        )
        return

    # Проверка формата команды
    if len(args) != 3:
        await message.reply(
            "📋 <b>Использование:</b>\n"
            "<code>/set_status [user_id] [status]</code>\n"
            "Пример: <code>/set_status 123456789 5</code>\n"
            f"🔢 Доступные статусы: 0-{len(emojis) - 1}",
            parse_mode="HTML"
        )
        return

    try:
        target_user_id = int(args[1])
        new_status = int(args[2])
    except ValueError:
        await message.reply(
            "❌ <b>Ошибка:</b> Укажите корректные ID пользователя и номер статуса (числа).",
            parse_mode="HTML"
        )
        return

    # Проверка валидности статуса
    if new_status < 0 or new_status >= len(emojis):
        await message.reply(
            f"❌ <b>Ошибка:</b> Неверный номер статуса. Выберите от 0 до {len(emojis) - 1}.",
            parse_mode="HTML"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # Проверка, существует ли пользователь
        cursor = await db.execute("SELECT username, status FROM users WHERE user_id = ?", (target_user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply(
                f"❌ <b>Ошибка:</b> Пользователь с ID {target_user_id} не найден.",
                parse_mode="HTML"
            )
            return
        username, current_status = result
        username = username if username else f"Игрок {target_user_id}"

        # Обновление статуса
        await db.execute(
            "UPDATE users SET status = ? WHERE user_id = ?",
            (new_status, target_user_id)
        )
        await db.commit()

    # Формирование сообщения об успехе
    await message.reply(
        f"✅ <b>Статус изменен!</b>\n\n"
        f"👤 Пользователь: @{username} (ID: {target_user_id})\n"
        f"💎 Новый статус: {emojis[new_status]} (#{new_status})\n"
        f"🔹 Предыдущий статус: {emojis[current_status]} (#{current_status})",
        parse_mode="HTML"
    )

    # Уведомление пользователю (если это не тот же админ)
    if target_user_id != user_id:
        try:
            await bot.send_message(
                target_user_id,
                f"🎉 <b>Ваш статус обновлен!</b>\n\n"
                f"💎 Новый статус: {emojis[new_status]} (#{new_status})\n"
                f"🔹 Теперь ваши бонусы стали больше!",
                parse_mode="HTML"
            )
        except Exception:
            await message.reply(
                f"⚠️ Не удалось уведомить пользователя @{username} (ID: {target_user_id}).",
                parse_mode="HTML"
            )

#=================================== БОНУС ===========================
@dp.message(Command("bonus"))
async def cmd_bonus(message: types.Message):
    user_id = message.from_user.id
    now = datetime.now(UTC)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT coins, last_bonus, status FROM users WHERE user_id = ?", (user_id,)
        )
        result = await cursor.fetchone()
        if not result:
            await message.reply("Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        coins, last_bonus, user_status = result

        # Проверка времени последнего бонуса
        if last_bonus:
            last_bonus_dt = datetime.fromisoformat(last_bonus)
            if last_bonus_dt.tzinfo is None:
                last_bonus_dt = last_bonus_dt.replace(tzinfo=UTC)
            if now - last_bonus_dt < timedelta(hours=1):
                mins = int((timedelta(hours=1) - (now - last_bonus_dt)).total_seconds() // 60)
                await message.reply(
                    f"⏳ Бонус можно получить через {mins} мин.\n"
                    f"💎 Ваш статус: {emojis[user_status]}",
                    parse_mode="HTML"
                )
                return

        # Выбор бонуса в зависимости от статуса
        possible_bonuses = status_bonus_map.get(user_status, status_bonus_map[0])
        bonus = random.choice(possible_bonuses)
        coins += bonus

        # Обновление базы данных
        await db.execute(
            "UPDATE users SET coins = ?, last_bonus = ? WHERE user_id = ?",
            (coins, now.isoformat(), user_id)
        )
        await db.commit()

    await message.reply(
        f"🎁 <b>Бонус получен!</b>\n\n"
        f"💰 Вы получили: <code>{format_balance(bonus)}</code> GG\n"
        f"💎 Ваш статус: {emojis[user_status]}\n"
        f"📊 Новый баланс: <code>{format_balance(coins)}</code> GG\n"
        f"⏳ Следующий бонус через 1 час!",
        parse_mode="HTML"
    )

@dp.message(lambda m: m.text and m.text.lower() == "бонус")
async def txt_bonus(message: types.Message):
    await cmd_bonus(message)

#=================================== ТОП ===========================
@dp.message(Command("top"))
async def cmd_top(message: types.Message):
    user_id = message.from_user.id

    # Проверка регистрации пользователя
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins, status FROM users WHERE user_id = ?", (user_id,))
        user_row = await cursor.fetchone()
        if not user_row:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_coins, user_status = user_row

        # Топ-10 игроков (исключая скрытых)
        cursor = await db.execute("SELECT user_id, coins, status FROM users WHERE hidden = 0 ORDER BY coins DESC LIMIT 10")
        rows = await cursor.fetchall()

        # Поиск места текущего игрока (если не скрыт)
        cursor = await db.execute("SELECT user_id FROM users WHERE hidden = 0 ORDER BY coins DESC")
        all_users = await cursor.fetchall()
        rank = next((i + 1 for i, (uid,) in enumerate(all_users) if uid == user_id), None)

    if not rows:
        await message.reply("Пока нет игроков в топе.", parse_mode="HTML")
        return

    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    diamond_medals = ["💎", "💎", "💎", "💎", "💎", "💎", "💎"]

    for i, (uid, coins, status) in enumerate(rows, start=1):
        try:
            user = await bot.get_chat(uid)
            name = user.first_name
            if len(name) > 10:
                name = name[:10] + "..."
        except Exception:
            name = f"Игрок {uid}"

        if i <= 3:
            medal = medals[i - 1]
        else:
            medal = diamond_medals[i - 4]
        text += f"{medal} <b>{name}</b> <code>[{emojis[status]}]</code> — <code>{format_balance(coins)}</code>\n"

    if rank and rank > 10:
        text += f"\n✨ Ваше место: <b>{rank}</b> — <code>{format_balance(user_coins)}</code>\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏆 Выигрыши", callback_data=f"top_wins_{user_id}"),
                InlineKeyboardButton(text="💸 Проигрыши", callback_data=f"top_losses_{user_id}")
            ]
        ]
    )

    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dp.message(lambda m: m.text and m.text.lower() == "топ")
async def txt_top(message: types.Message):
    await cmd_top(message)


@dp.callback_query(lambda c: c.data.startswith("top_wins_"))
async def top_wins(call: types.CallbackQuery):
    parts = call.data.split("_")
    if len(parts) != 3:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return
    try:
        original_user_id = int(parts[2])
    except ValueError:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    if call.from_user.id != original_user_id:
        await call.answer("❌ Эта кнопка не для вас! Используйте /top, чтобы открыть свой топ.", show_alert=True)
        return

    user_id = call.from_user.id

    # Проверка регистрации пользователя
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT win_amount, status FROM users WHERE user_id = ?", (user_id,))
        user_row = await cursor.fetchone()
        if not user_row:
            await call.message.edit_text("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            await call.answer()
            return
        user_wins, user_status = user_row

        cursor = await db.execute(
            "SELECT user_id, win_amount, status FROM users WHERE hidden = 0 ORDER BY win_amount DESC LIMIT 10")
        rows = await cursor.fetchall()

        cursor = await db.execute("SELECT user_id FROM users WHERE hidden = 0 ORDER BY win_amount DESC")
        all_users = await cursor.fetchall()
        rank = next((i + 1 for i, (uid,) in enumerate(all_users) if uid == user_id), None)

    if not rows:
        await call.message.edit_text("Пока нет игроков в топе по выигрышам.", parse_mode="HTML")
        await call.answer()
        return

    text = "🏆 <b>Топ игроков по выигрышам</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    diamond_medals = ["💎", "💎", "💎", "💎", "💎", "💎", "💎"]

    for i, (uid, wins, status) in enumerate(rows, start=1):
        try:
            user = await bot.get_chat(uid)
            name = user.first_name
            if len(name) > 10:
                name = name[:10] + "..."
        except Exception:
            name = f"Игрок {uid}"

        if i <= 3:
            medal = medals[i - 1]
        else:
            medal = diamond_medals[i - 4]
        text += f"{medal} <b>{name}</b> <code>[{emojis[status]}]</code> — <code>{format_balance(wins)}</code>\n"

    if rank and rank > 10:
        text += f"\n✨ Ваше место: <b>{rank}</b> — <code>{format_balance(user_wins)}</code>\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏆 Балансы", callback_data=f"top_balance_{user_id}"),
                InlineKeyboardButton(text="💸 Проигрыши", callback_data=f"top_losses_{user_id}")
            ]
        ]
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("top_losses_"))
async def top_losses(call: types.CallbackQuery):
    parts = call.data.split("_")
    if len(parts) != 3:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return
    try:
        original_user_id = int(parts[2])
    except ValueError:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    if call.from_user.id != original_user_id:
        await call.answer("❌ Эта кнопка не для вас! Используйте /top, чтобы открыть свой топ.", show_alert=True)
        return

    user_id = call.from_user.id

    # Проверка регистрации пользователя
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT lose_amount, status FROM users WHERE user_id = ?", (user_id,))
        user_row = await cursor.fetchone()
        if not user_row:
            await call.message.edit_text("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            await call.answer()
            return
        user_losses, user_status = user_row

        cursor = await db.execute(
            "SELECT user_id, lose_amount, status FROM users WHERE hidden = 0 ORDER BY lose_amount DESC LIMIT 10")
        rows = await cursor.fetchall()

        cursor = await db.execute("SELECT user_id FROM users WHERE hidden = 0 ORDER BY lose_amount DESC")
        all_users = await cursor.fetchall()
        rank = next((i + 1 for i, (uid,) in enumerate(all_users) if uid == user_id), None)

    if not rows:
        await call.message.edit_text("Пока нет игроков в топе по проигрышам.", parse_mode="HTML")
        await call.answer()
        return

    text = "💸 <b>Топ игроков по проигрышам</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    diamond_medals = ["💎", "💎", "💎", "💎", "💎", "💎", "💎"]

    for i, (uid, losses, status) in enumerate(rows, start=1):
        try:
            user = await bot.get_chat(uid)
            name = user.first_name
            if len(name) > 10:
                name = name[:10] + "..."
        except Exception:
            name = f"Игрок {uid}"

        if i <= 3:
            medal = medals[i - 1]
        else:
            medal = diamond_medals[i - 4]
        text += f"{medal} <b>{name}</b> <code>[{emojis[status]}]</code> — <code>{format_balance(losses)}</code>\n"

    if rank and rank > 10:
        text += f"\n✨ Ваше место: <b>{rank}</b> — <code>{format_balance(user_losses)}</code>\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏆 Балансы", callback_data=f"top_balance_{user_id}"),
                InlineKeyboardButton(text="🏆 Выигрыши", callback_data=f"top_wins_{user_id}")
            ]
        ]
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("top_balance_"))
async def top_balance(call: types.CallbackQuery):
    parts = call.data.split("_")
    if len(parts) != 3:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return
    try:
        original_user_id = int(parts[2])
    except ValueError:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    if call.from_user.id != original_user_id:
        await call.answer("❌ Эта кнопка не для вас! Используйте /top, чтобы открыть свой топ.", show_alert=True)
        return

    user_id = call.from_user.id

    # Проверка регистрации пользователя
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins, status FROM users WHERE user_id = ?", (user_id,))
        user_row = await cursor.fetchone()
        if not user_row:
            await call.message.edit_text("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            await call.answer()
            return
        user_coins, user_status = user_row

        # Топ-10 игроков (исключая скрытых)
        cursor = await db.execute("SELECT user_id, coins, status FROM users WHERE hidden = 0 ORDER BY coins DESC LIMIT 10")
        rows = await cursor.fetchall()

        # Поиск места текущего игрока (если не скрыт)
        cursor = await db.execute("SELECT user_id FROM users WHERE hidden = 0 ORDER BY coins DESC")
        all_users = await cursor.fetchall()
        rank = next((i + 1 for i, (uid,) in enumerate(all_users) if uid == user_id), None)

    if not rows:
        await call.message.edit_text("Пока нет игроков в топе.", parse_mode="HTML")
        await call.answer()
        return

    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    diamond_medals = ["💎", "💎", "💎", "💎", "💎", "💎", "💎"]

    for i, (uid, coins, status) in enumerate(rows, start=1):
        try:
            user = await bot.get_chat(uid)
            name = user.first_name
            if len(name) > 10:
                name = name[:10] + "..."
        except Exception:
            name = f"Игрок {uid}"

        if i <= 3:
            medal = medals[i - 1]
        else:
            medal = diamond_medals[i - 4]
        text += f"{medal} <b>{name}</b> <code>[{emojis[status]}]</code> — <code>{format_balance(coins)}</code>\n"

    if rank and rank > 10:
        text += f"\n✨ Ваше место: <b>{rank}</b> — <code>{format_balance(user_coins)}</code>\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏆 Выигрыши", callback_data=f"top_wins_{user_id}"),
                InlineKeyboardButton(text="💸 Проигрыши", callback_data=f"top_losses_{user_id}")
            ]
        ]
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.message(Command("hide"))
async def cmd_hide(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT hidden FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("Вы не зарегистрированы. Введите /start.")
            return
        current_hidden = result[0]

        new_hidden = 0 if current_hidden else 1
        await db.execute("UPDATE users SET hidden = ? WHERE user_id = ?", (new_hidden, user_id))
        await db.commit()

        status = "скрыт" if new_hidden else "показан"
        await message.reply(f"✅ Вы {status} в топах. Для изменения используйте /hide снова.")



#=================================== МОНЕТА ===========================
def get_coin_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🦅 Орел (1,9x)", callback_data="coin_heads"),
                InlineKeyboardButton(text="🌑 Решка (1,9x)", callback_data="coin_tails"),
            ],
            [
                InlineKeyboardButton(text="❌ Отменить игру", callback_data="coin_cancel")
            ]
        ]
    )

@dp.message(Command("coin"))
async def cmd_coin(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply(
            "🎲 <b>Игра: Орел или Решка</b>\n"
            "Введите ставку: <code>/coin 10</code> (минимум 10 монет)",
            parse_mode="HTML"
        )
        return
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("Вы не зарегистрированы. Введите /start.")
            return
        user_money = result[0]
        bet = parse_bet_input(args[1], user_money)
        if bet < 10:
            await message.reply("❗ Минимальная ставка — <b>10</b> монет.", parse_mode="HTML")
            return
        if user_money < bet:
            await message.reply(f"Недостаточно монет для ставки. Ваш баланс: <code>{format_balance(user_money)}</code>", parse_mode="HTML")
            return
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.execute("INSERT OR REPLACE INTO coin_game (user_id, bet) VALUES (?, ?)", (user_id, bet))
        await db.commit()
    await message.reply(
        f"🎰 <b>Ставка принята:</b> <code>{format_balance(bet)}</code> монет\n"
        "Выберите сторону монеты:",
        reply_markup=get_coin_keyboard(),
        parse_mode="HTML"
    )

@dp.message(lambda m: m.text and m.text.lower().startswith("монета"))
async def txt_coin(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply(
            "🎲 <b>Игра: Орел или Решка</b>\n"
            "Введите ставку: <code>монета 10</code> (минимум 10 монет)",
            parse_mode="HTML"
        )
        return
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("Вы не зарегистрированы. Введите /start.")
            return
        user_money = result[0]
        bet = parse_bet_input(args[1], user_money)
        if bet < 10:
            await message.reply("❗ Минимальная ставка — <b>10</b> монет.", parse_mode="HTML")
            return
        if user_money < bet:
            await message.reply(f"Недостаточно монет для ставки. Ваш баланс: <code>{format_balance(user_money)}</code>", parse_mode="HTML")
            return
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.execute("INSERT OR REPLACE INTO coin_game (user_id, bet) VALUES (?, ?)", (user_id, bet))
        await db.commit()
    await message.reply(
        f"🎰 <b>Ставка принята:</b> <code>{format_balance(bet)}</code> монет\n"
        "Выберите сторону монеты:",
        reply_markup=get_coin_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data in ["coin_heads", "coin_tails"])
async def coin_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT bet FROM coin_game WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        cursor2 = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        user_exists = await cursor2.fetchone()
        if not user_exists:
            await call.answer("Вы не зарегистрированы. Введите /start.", show_alert=True)
            return
        if not result:
            await call.answer("Ошибка: ставка не найдена. Начните игру заново.")
            return
        bet = result[0]
        user_choice = call.data  # "coin_heads" или "coin_tails"
        # Шанс выигрыша 1/1.7
        win = random.random() < 0.4  # шанс 40%
        coin_result = user_choice if win else ("coin_tails" if user_choice == "coin_heads" else "coin_heads")
        if win:
            prize = int(bet * 1.9)
            await db.execute("UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?", (prize, prize, user_id))
            await call.message.edit_text(
                f"🎉 <b>Победа!</b>\n"
                f"Выпало: <b>{'🦅 Орел' if coin_result == 'coin_heads' else '🌑 Решка'}</b>\n"
                f"Вы выиграли <b>{format_balance(prize)}</b> монет!\n"
                "Попробуйте ещё раз — удача любит смелых!",
                parse_mode="HTML"
            )
        else:
            await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?", (bet, user_id))
            await call.message.edit_text(
                f"😢 <b>Проигрыш!</b>\n"
                f"Выпало: <b>{'🦅 Орел' if coin_result == 'coin_heads' else '🌑 Решка'}</b>\n"
                f"Вы ничего не выиграли.\n"
                "Не расстраивайтесь, попробуйте снова!",
                parse_mode="HTML"
            )
        await db.execute("DELETE FROM coin_game WHERE user_id = ?", (user_id,))
        await db.commit()
    await call.answer()

@dp.callback_query(lambda c: c.data == "coin_cancel")
async def coin_cancel_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT bet FROM coin_game WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if result:
            bet = result[0]
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (bet, user_id))
            await db.execute("DELETE FROM coin_game WHERE user_id = ?", (user_id,))
            await db.commit()
            await call.message.edit_text(
                f"🚫 Игра отменена.\nВаша ставка <b>{format_balance(bet)}</b> монет возвращена.",
                parse_mode="HTML"
            )
        else:
            await call.message.edit_text(
                "🚫 Игра отменена.",
                parse_mode="HTML"
            )
    await call.answer()

#=================================== БОСС ===========================


ADMIN_ID = 6492780518  # Example, replace with your admin ID
# Bot instance (assumed defined elsewhere in werere.py)
# bot = Bot(token=API_TOKEN)
# dp = Dispatcher()

# Utility functions for boss
last_message_state = {}  # Формат: {chat_id_message_id: {"hash": str}}

# Вспомогательные функции для босса
def format_balance_boss(value: Union[int, float]) -> str:
    """Форматирует здоровье босса с пробелами (например, 1234567 -> 1 234 567)."""
    return "{:,}".format(int(value)).replace(",", " ")

def form_balance(balance: Union[int, float, str, Decimal]) -> str:
    """Форматирует числа (кроме здоровья босса) с суффиксами 'к' (например, 1234567 -> 1.23кк)."""
    balance = float(balance)
    if balance == 0:
        return "0"
    exponent = int(math.log10(abs(balance)))
    group = exponent // 3
    scaled_balance = balance / (10 ** (group * 3))
    formatted_balance = f"{scaled_balance:.2f}"
    suffix = "к" * group
    return formatted_balance.rstrip('0').rstrip('.') + suffix

def _to_decimal_safe(value) -> Optional[Decimal]:
    """Safely converts a value to Decimal."""
    try:
        return Decimal(str(value))
    except Exception:
        return None

def parse_bet_input_boss(arg: str, user_resource: Optional[Union[int, float, str, Decimal]] = None) -> int:
    """Парсит входную строку в целое число для босса, поддерживая 'все', 'k' суффиксы и числа."""
    if arg is None:
        return -1

    s = str(arg).strip().lower()
    s = s.replace(" ", "").replace("_", "")

    if s in ("все", "всё", "all"):
        um = _to_decimal_safe(user_resource)
        if um is None:
            return -1
        try:
            return int(um)
        except Exception:
            return -1

    m = re.fullmatch(r'([0-9]+(?:[.,][0-9]{1,2})?)([kк]*)', s)
    if not m:
        return -1

    num_str, k_suffix = m.groups()
    num_str = num_str.replace(',', '.')
    try:
        num = Decimal(num_str)
    except Exception:
        return -1

    multiplier = Decimal(1000) ** len(k_suffix)
    result = num * multiplier

    try:
        return int(result)
    except Exception:
        return -1

# Состояния босса
class BossStates(StatesGroup):
    buy_quantity = State()
    attack_quantity = State()

# Получение текущего босса
async def fetch_current_boss():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT boss_id, name, hp_total, hp_current, start_time
            FROM bosses WHERE active = 1 ORDER BY boss_id DESC LIMIT 1
        """)
        row = await cursor.fetchone()
        if row:
            boss_id, name, hp_total, hp_current, start_time = row
            if hp_current == -1 and start_time:
                start_dt = datetime.fromisoformat(start_time).replace(tzinfo=ZoneInfo("UTC"))
                now = datetime.now(ZoneInfo("UTC"))
                if now >= start_dt:
                    await db.execute("UPDATE bosses SET hp_current = ? WHERE boss_id = ?", (hp_total, boss_id))
                    await db.commit()
                    hp_current = hp_total
            return {'boss_id': boss_id, 'name': name, 'hp_total': hp_total, 'hp_current': hp_current, 'start_time': start_time}
        return None

async def calculate_avg_fezcoin_price():
    async with aiosqlite.connect(DB_PATH) as db:
        # Топ-3 минимальных sell
        cursor = await db.execute("""
            SELECT price FROM fez_orders 
            WHERE status = 'open' AND order_type = 'sell' 
            ORDER BY price ASC LIMIT 3
        """)
        sell_prices = [row[0] for row in await cursor.fetchall()]

        # Топ-3 максимальных buy
        cursor = await db.execute("""
            SELECT price FROM fez_orders 
            WHERE status = 'open' AND order_type = 'buy' 
            ORDER BY price DESC LIMIT 3
        """)
        buy_prices = [row[0] for row in await cursor.fetchall()]

        if not sell_prices or not buy_prices:
            return 6073  # Fallback

        # Среднее от топ-3 sell и топ-3 buy
        avg_sell = sum(sell_prices) / len(sell_prices)
        avg_buy = sum(buy_prices) / len(buy_prices)
        return int((avg_sell + avg_buy) / 2)
        
# Функция для рендеринга главного меню босса
async def render_boss_menu(obj: Union[types.Message, types.CallbackQuery], state: FSMContext, is_callback: bool = False):
    await state.clear()  # Очищаем состояние FSM
    user_id = obj.from_user.id if is_callback else obj.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            text = (
                "❌ <b>Ошибка регистрации</b>\n\n"
                "Вы ещё не зарегистрированы в игре. Чтобы участвовать в битвах с боссами, вам нужно создать аккаунт.\n"
                "➡️ Используйте команду <code>/start</code>, чтобы зарегистрироваться и начать своё приключение!"
            )
            if is_callback:
                await obj.message.edit_text(text, parse_mode="HTML")
            else:
                await obj.answer(text, parse_mode="HTML")
            return

    boss = await fetch_current_boss()
    if not boss:
        text = (
            "<b>👹 Арена Боссов: Ожидание нового вызова</b>\n\n"
            "❌ На данный момент <i>активный босс отсутствует</i>.\n"
            "🌟 Это означает, что предыдущий сезон завершён, и скоро появится новый могущественный противник!\n"
            "⏳ Следите за обновлениями в чате, чтобы не пропустить начало нового сезона.\n"
            "💡 <i>Совет:</i> Пока можно накопить <b>Fezcoin</b>, приобрести мощное оружие или обменять накопленный опыт на <b>GG</b> монеты."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡️ Магазин оружия", callback_data="boss_weapons"),
             InlineKeyboardButton(text="🏆 Топ игроков", callback_data="boss_top")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="boss_main"),
             InlineKeyboardButton(text="🔄 Обменник опыта", callback_data="boss_exchange")]
        ])
    else:
        moscow_tz = pytz.timezone('Europe/Moscow')
        text = (
            f"<b>👹 Эпическая битва с боссом: {boss['name']}</b>\n\n"
            f"❤️ <b>Здоровье босса:</b> <code>{format_balance_boss(boss['hp_current'])}</code> из <code>{format_balance_boss(boss['hp_total'])}</code> HP\n\n"
            "🌌 <i>Описание:</i> Этот грозный враг обладает невероятной силой и хитростью. Только совместные усилия всех игроков смогут его победить!\n"
            "📢 <b>Цель:</b> Нанесите как можно больше урона, чтобы занять высокое место в топе и заработать <b>опыт</b> для обмена на <b>GG</b> монеты.\n\n"
        )
        if boss['hp_current'] == -1 and boss['start_time']:
            start_dt = datetime.fromisoformat(boss['start_time']).replace(tzinfo=ZoneInfo("UTC")).astimezone(moscow_tz)
            text += (
                f"⏳ <b>Время до начала сезона:</b> <code>{start_dt.strftime('%Y-%m-%d %H:%M:%S')} (МСК)</code>\n"
                "📢 Сезон начнётся скоро. Это время для подготовки!\n"
                "💡 <i>Совет:</i> Зайдите в <b>Магазин оружия</b>, чтобы купить подходящее снаряжение, или проверьте свой <b>инвентарь</b>.\n\n"
            )
        elif boss['hp_current'] == 0:
            text += (
                "🏆 <b>Босс побеждён!</b>\n"
                "🎉 Поздравляем всех участников с этой грандиозной победой!\n"
                "📊 Проверьте свою статистику: сколько <b>урона</b> вы нанесли и сколько <b>опыта</b> заработали.\n"
                "⏳ Новый сезон с ещё более сильным боссом скоро начнётся.\n"
                "💡 <i>Совет:</i> Используйте <b>обменник опыта</b>, чтобы конвертировать свой опыт в <b>GG</b> монеты.\n\n"
            )
        else:
            text += (
                "📈 <b>Прогресс:</b> Каждый ваш удар приближает команду к победе и поднимает вас в <b>топе</b> игроков.\n"
                "💡 <i>Совет:</i> Используйте мощное оружие с высоким <b>бонусным уроном</b> для максимального эффекта (шанс 10%).\n\n"
            )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚔️ Нанести урон", callback_data="boss_attack")] if boss['hp_current'] > 0 else [],
            [InlineKeyboardButton(text="🛡️ Магазин оружия", callback_data="boss_weapons"),
             InlineKeyboardButton(text="🏆 Топ игроков", callback_data="boss_top")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="boss_main"),
             InlineKeyboardButton(text="🔄 Обменник опыта", callback_data="boss_exchange")]
        ])
    text += "<b>🌟 Выберите действие:</b>"

    if is_callback:
        message_key = f"{obj.message.chat.id}_{obj.message.message_id}"
        reply_markup_str = str(kb.inline_keyboard) if kb else ""
        new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
        last_state = last_message_state.get(message_key, {"hash": None})

        if last_state["hash"] != new_state_hash:
            try:
                await obj.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
                last_message_state[message_key] = {"hash": new_state_hash}
            except Exception:
                await obj.message.delete()
                new_msg = await obj.message.reply(text, reply_markup=kb, parse_mode="HTML")
                last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
        await obj.answer()
    else:
        new_msg = await obj.answer(text, reply_markup=kb, parse_mode="HTML")
        message_key = f"{new_msg.chat.id}_{new_msg.message_id}"
        reply_markup_str = str(kb.inline_keyboard) if kb else ""
        new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
        last_message_state[message_key] = {"hash": new_state_hash}

# Основная команда босса
@dp.message(Command("boss"))
@dp.message(lambda m: m.text and m.text.lower() in ["босс"])
async def cmd_boss(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /boss доступна только в личных сообщениях с ботом!",
            parse_mode="HTML"
        )
        return
    await render_boss_menu(message, state)

# Обновление меню босса
@dp.callback_query(lambda c: c.data == "boss_main")
async def boss_main(call: types.CallbackQuery, state: FSMContext):
    await render_boss_menu(call, state, is_callback=True)

# Команда администратора для создания нового босса
@dp.message(Command("new_boss"))
async def cmd_new_boss(message: types.Message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.reply(
            "❌ <b>Доступ запрещён</b>\n\n"
            "Эта команда доступна только <b>администраторам</b> бота.\n"
            "➡️ Если у вас есть вопросы или вы считаете это ошибкой, свяжитесь с <i>поддержкой</i>.",
            parse_mode="HTML"
        )
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply(
            "<b>📋 Руководство по созданию босса</b>\n\n"
            "🔹 <b>Формат команды:</b> <code>/new_boss &lt;название&gt; &lt;HP&gt; [время_в_минутах]</code>\n"
            "📝 <b>Примеры:</b>\n"
            "• <code>/new_boss Огненный Дракон 1000000 1</code> — босс через 1 минуту\n"
            "• <code>/new_boss Теневой Властелин 50000000</code> — босс через 30 минут (по умолчанию)\n\n"
            "💡 <b>Подробности:</b>\n"
            "• <b>Название</b> может содержать несколько слов.\n"
            "• <b>HP</b> — целое число, минимум <code>1000</code>.\n"
            "• <b>Время</b> — целое число в минутах (по умолчанию <code>30</code>).\n"
            "Создание нового босса завершит текущий сезон, обнулив статистику урона, но <b>опыт</b> игроков сохранится.",
            parse_mode="HTML"
        )
        return

    try:
        last_arg = args[-1]
        if last_arg.isdigit() or re.match(r'([0-9]+(?:[.,][0-9]{1,2})?)([kк]*)', last_arg.lower()):
            minutes = parse_bet_input_boss(last_arg, None)
            if minutes < 1:
                raise ValueError("Время должно быть не менее 1 минуты")
            hp_str = args[-2]
            name_words = args[1:-2]
        else:
            minutes = 30
            hp_str = args[-1]
            name_words = args[1:-1]

        hp_total = parse_bet_input_boss(hp_str, None)
        if hp_total < 1000:
            raise ValueError("HP слишком мало")
        name = " ".join(name_words)
        if not name:
            raise ValueError("Название не может быть пустым")
    except ValueError as e:
        await message.reply(
            f"❌ <b>Ошибка ввода</b>\n\n"
            f"🚫 {str(e).replace('Invalid input: must be a positive integer', 'HP и время должны быть целыми числами.')}\n"
            "➡️ Проверьте формат: <code>/new_boss &lt;название&gt; &lt;HP&gt; [время_в_минутах]</code>\n"
            "📝 Пример: <code>/new_boss Огненный Дракон 1000000 1</code>",
            parse_mode="HTML"
        )
        return

    now = datetime.now(ZoneInfo("UTC"))
    start_time = (now + timedelta(minutes=minutes)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET active = 0 WHERE active = 1")
        await db.execute(
            "INSERT INTO bosses (name, hp_total, created_at, start_time, active) VALUES (?, ?, ?, ?, 1)",
            (name, hp_total, now.isoformat(), start_time)
        )
        cursor = await db.execute("SELECT boss_id FROM bosses WHERE active = 0")
        old_bosses = await cursor.fetchall()
        for old_id in old_bosses:
            await db.execute("DELETE FROM player_boss_damage WHERE boss_id = ?", (old_id[0],))
        await db.commit()

    moscow_tz = pytz.timezone('Europe/Moscow')
    start_dt = datetime.fromisoformat(start_time).replace(tzinfo=ZoneInfo("UTC")).astimezone(moscow_tz)
    await message.reply(
        "<b>🎉 Новый босс создан!</b>\n\n"
        f"👹 <b>Название:</b> {name}\n"
        f"❤️ <b>Здоровье:</b> <code>{format_balance_boss(hp_total)}</code> HP\n"
        f"⏳ <b>Начало сезона:</b> <code>{start_dt.strftime('%Y-%m-%d %H:%M:%S')} (МСК)</code>\n"
        f"📢 <b>Информация:</b> Босс активируется через <b>{minutes} мин.</b> Игроки смогут начать наносить урон, как только сезон откроется.",
        parse_mode="HTML"
    )

# Команда администратора для накрутки Fezcoin
@dp.message(Command("hh"))
async def cmd_hh(message: types.Message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.reply(
            "❌ <b>Доступ запрещён</b>\n\n"
            "Эта команда доступна только <b>администраторам</b> бота.\n"
            "➡️ Если у вас есть вопросы или вы считаете это ошибкой, свяжитесь с <i>поддержкой</i>.",
            parse_mode="HTML"
        )
        return

    args = message.text.split()
    if len(args) != 3:
        await message.reply(
            "<b>📋 Руководство по команде /hh</b>\n\n"
            "🔹 <b>Формат команды:</b> <code>/hh &lt;количество_Fezcoin&gt; &lt;user_id&gt;</code>\n"
            "📝 <b>Примеры:</b>\n"
            "• <code>/hh 1000 123456789</code> — начислить 1000 Fezcoin пользователю с ID 123456789\n"
            "💡 <b>Подробности:</b>\n"
            "• <b>Количество</b> — целое положительное число, можно использовать 'k' (например, 10k = 10000).\n"
            "• <b>User ID</b> — Telegram ID пользователя (целое число).\n"
            "Команда начисляет Fezcoin на баланс указанного пользователя.",
            parse_mode="HTML"
        )
        return

    try:
        amount = parse_bet_input_boss(args[1], None)
        target_user_id = parse_bet_input_boss(args[2], None)
        if amount < 0 or target_user_id < 0:
            raise ValueError("Количество и user_id должны быть положительными числами")
    except ValueError as e:
        await message.reply(
            f"❌ <b>Ошибка ввода</b>\n\n"
            f"🚫 {str(e)}\n"
            "➡️ Проверьте формат: <code>/hh &lt;количество_Fezcoin&gt; &lt;user_id&gt;</code>\n"
            "📝 Пример: <code>/hh 1000 123456789</code>",
            parse_mode="HTML"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (target_user_id,))
        user = await cursor.fetchone()
        if not user:
            await message.reply(
                f"❌ <b>Ошибка</b>\n\n"
                f"Пользователь с ID <code>{target_user_id}</code> не найден в базе данных.\n"
                "➡️ Убедитесь, что пользователь зарегистрирован (использовал <code>/start</code>).",
                parse_mode="HTML"
            )
            return

        current_fezcoin = user[0]
        new_fezcoin = current_fezcoin + amount
        await db.execute("UPDATE users SET fezcoin = ? WHERE user_id = ?", (new_fezcoin, target_user_id))
        await db.commit()

    await message.reply(
        f"<b>💎 Начисление Fezcoin успешно</b>\n\n"
        f"👤 <b>Пользователь ID:</b> <code>{target_user_id}</code>\n"
        f"💰 <b>Начислено:</b> <code>{form_balance(amount)}</code> Fezcoin\n"
        f"💎 <b>Новый баланс:</b> <code>{form_balance(new_fezcoin)}</code> Fezcoin\n"
        f"📢 Пользователь уведомлён о начислении.",
        parse_mode="HTML"
    )

    try:
        await bot.send_message(
            target_user_id,
            f"<b>💎 Пополнение баланса!</b>\n\n"
            f"Вам начислено <code>{form_balance(amount)}</code> Fezcoin.\n"
            f"💎 <b>Ваш баланс:</b> <code>{form_balance(new_fezcoin)}</code> Fezcoin\n"
            f"💡 Используйте их для покупки оружия или других бонусов в игре!",
            parse_mode="HTML"
        )
    except:
        await message.reply(
            f"⚠️ <b>Предупреждение</b>\n\n"
            f"Не удалось отправить уведомление пользователю <code>{target_user_id}</code> (возможно, бот заблокирован).\n"
            f"Fezcoin успешно начислены на баланс.",
            parse_mode="HTML"
        )
        
@dp.message(Command("uhh"))
async def cmd_hh(message: types.Message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.reply(
            "❌ <b>Доступ запрещён</b>\n\n"
            "Эта команда доступна только <b>администраторам</b> бота.\n"
            "➡️ Если у вас есть вопросы или вы считаете это ошибкой, свяжитесь с <i>поддержкой</i>.",
            parse_mode="HTML"
        )
        return

    args = message.text.split()
    if len(args) != 3:
        await message.reply(
            "<b>📋 Руководство по команде /hh</b>\n\n"
            "🔹 <b>Формат команды:</b> <code>/uhh &lt;количество_Fezcoin&gt; &lt;user_id&gt;</code>\n"
            "📝 <b>Примеры:</b>\n"
            "• <code>/uhh 1000 123456789</code> — начислить 1000 Fezcoin пользователю с ID 123456789\n"
            "💡 <b>Подробности:</b>\n"
            "• <b>Количество</b> — целое положительное число, можно использовать 'k' (например, 10k = 10000).\n"
            "• <b>User ID</b> — Telegram ID пользователя (целое число).\n"
            "Команда забирает Fezcoin с баланса указанного пользователя.",
            parse_mode="HTML"
        )
        return

    try:
        amount = parse_bet_input_boss(args[1], None)
        target_user_id = parse_bet_input_boss(args[2], None)
        if amount < 0 or target_user_id < 0:
            raise ValueError("Количество и user_id должны быть положительными числами")
    except ValueError as e:
        await message.reply(
            f"❌ <b>Ошибка ввода</b>\n\n"
            f"🚫 {str(e)}\n"
            "➡️ Проверьте формат: <code>/uhh &lt;количество_Fezcoin&gt; &lt;user_id&gt;</code>\n"
            "📝 Пример: <code>/uhh 1000 123456789</code>",
            parse_mode="HTML"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (target_user_id,))
        user = await cursor.fetchone()
        if not user:
            await message.reply(
                f"❌ <b>Ошибка</b>\n\n"
                f"Пользователь с ID <code>{target_user_id}</code> не найден в базе данных.\n"
                "➡️ Убедитесь, что пользователь зарегистрирован (использовал <code>/start</code>).",
                parse_mode="HTML"
            )
            return

        current_fezcoin = user[0]
        new_fezcoin = current_fezcoin - amount
        await db.execute("UPDATE users SET fezcoin = ? WHERE user_id = ?", (new_fezcoin, target_user_id))
        await db.commit()

    await message.reply(
        f"<b>💎 Снятие Fezcoin успешно</b>\n\n"
        f"👤 <b>Пользователь ID:</b> <code>{target_user_id}</code>\n"
        f"💰 <b>Снято:</b> <code>{form_balance(amount)}</code> Fezcoin\n"
        f"💎 <b>Новый баланс:</b> <code>{form_balance(new_fezcoin)}</code> Fezcoin\n"
        f"📢 Пользователь уведомлён о снятии.",
        parse_mode="HTML"
    )

    try:
        await bot.send_message(
            target_user_id,
            f"<b>💎 Снятие с баланса!</b>\n\n"
            f"У вас снято <code>{form_balance(amount)}</code> Fezcoin.\n"
            f"💎 <b>Ваш баланс:</b> <code>{form_balance(new_fezcoin)}</code> Fezcoin\n"
            f"💡 Используйте их для покупки оружия или других бонусов в игре!",
            parse_mode="HTML"
        )
    except:
        await message.reply(
            f"⚠️ <b>Предупреждение</b>\n\n"
            f"Не удалось отправить уведомление пользователю <code>{target_user_id}</code> (возможно, бот заблокирован).\n"
            f"Fezcoin успешно начислены на баланс.",
            parse_mode="HTML"
        )

# Топ игроков по урону
@dp.callback_query(lambda c: c.data == "boss_top")
async def handle_boss_top(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    boss = await fetch_current_boss()
    if not boss:
        text = (
            "<b>🏆 Топ игроков по урону</b>\n\n"
            "❌ <i>Нет активного босса.</i>\n"
            "⏳ Дождитесь начала нового сезона, чтобы увидеть рейтинг лучших бойцов."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("""
                SELECT p.user_id, p.damage_dealt, u.username, u.hidden
                FROM player_boss_damage p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.boss_id = ? AND u.hidden = 0
                ORDER BY p.damage_dealt DESC LIMIT 10
            """, (boss['boss_id'],))
            rows = await cursor.fetchall()

        text = (
            f"<b>🏆 Топ игроков по урону боссу: {boss['name']}</b>\n\n"
            "📊 Рейтинг лучших бойцов, нанёсших урон текущему боссу:\n\n"
        )
        if not rows:
            text += "❌ <i>Пока никто не нанёс урон боссу.</i>\n➡️ Станьте первым, атаковав босса!"
        else:
            for i, (user_id, damage, username, _) in enumerate(rows, 1):
                try:
                    user = await bot.get_chat(user_id)
                    display_name = user.first_name or username or f"ID {user_id}"
                except:
                    display_name = username or f"ID {user_id}"
                display_name = display_name[:12] + "..." if len(display_name) > 12 else display_name
                text += f"{'🥇' if i == 1 else '🥈' if i == 2 else '🥉' if i == 3 else f'{i}.'} <b>{display_name}</b> — <code>{form_balance(damage)}</code> HP\n"

        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Магазин оружия
@dp.callback_query(lambda c: c.data == "boss_weapons")
async def handle_boss_weapons(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    text = (
        "<b>🛡️ Магазин оружия</b>\n\n"
        "🔹 Добро пожаловать в арсенал! Здесь вы можете приобрести оружие для битвы с боссом.\n"
        "📋 <i>Категории:</i>\n"
        "• <b>Лёгкое оружие</b> — дешёвое, но с низким уроном.\n"
        "• <b>Тяжёлая артиллерия</b> — мощное, но более дорогое.\n"
        "• <b>Экзотическое оружие</b> — для элитных бойцов с максимальным уроном.\n"
        "💡 <i>Совет:</i> Проверьте свой <b>инвентарь</b>, чтобы увидеть, какое оружие у вас уже есть."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔫 Лёгкое оружие", callback_data="weapon_category_Light Arms")],
        [InlineKeyboardButton(text="💣 Тяжёлая артиллерия", callback_data="weapon_category_Heavy Artillery")],
        [InlineKeyboardButton(text="🪐 Экзотическое оружие", callback_data="weapon_category_Exotic Weapons")],
        [InlineKeyboardButton(text="🎒 Мой инвентарь", callback_data="weapon_inventory")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]
    ])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Выбор категории оружия
@dp.callback_query(lambda c: c.data.startswith("weapon_category_"))
async def handle_weapon_category(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    category = call.data.split("_", 2)[2]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT weapon_id, name FROM weapons WHERE category = ? ORDER BY cost_fez", (category,))
        weapons = await cursor.fetchall()

    text = (
        f"<b>🛡️ Категория: {category}</b>\n\n"
        "🔹 Выберите оружие, чтобы узнать подробности и приобрести его.\n"
        "💡 <i>Совет:</i> Обращайте внимание на <b>базовый урон</b> и <b>бонусный урон</b> (шанс 10%)."
    )
    kb_rows = [[InlineKeyboardButton(text=name, callback_data=f"weapon_details_{wid}")] for wid, name in weapons]
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_weapons")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Детали оружия
@dp.callback_query(lambda c: c.data.startswith("weapon_details_"))
async def handle_weapon_details(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    weapon_id = int(call.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT name, category, cost_fez, base_damage, bonus_damage, bonus_chance
            FROM weapons WHERE weapon_id = ?
        """, (weapon_id,))
        weapon = await cursor.fetchone()
        if not weapon:
            text = (
                "❌ <b>Ошибка</b>\n\nОружие не найдено."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        else:
            name, category, cost_fez, base_damage, bonus_damage, bonus_chance = weapon
            text = (
                f"<b>🛡️ Оружие: {name}</b>\n\n"
                f"📍 <b>Категория:</b> {category}\n"
                f"💰 <b>Цена:</b> <code>{form_balance(cost_fez)}</code> Fezcoin\n"
                f"⚔️ <b>Базовый урон:</b> <code>{form_balance(base_damage)}</code> HP\n"
                f"🔥 <b>Бонусный урон:</b> <code>{form_balance(bonus_damage)}</code> HP (<i>{bonus_chance*100}% шанс</i>)\n"
                "💡 <i>Совет:</i> Оружие с высоким бонусным уроном может нанести критический удар!"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛒 Купить", callback_data=f"weapon_buy_{weapon_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"weapon_category_{category}")]
            ])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Начало покупки оружия
@dp.callback_query(lambda c: c.data.startswith("weapon_buy_"))
async def handle_weapon_buy(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    weapon_id = int(call.data.split("_")[2])
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
        fezcoin = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT name, cost_fez FROM weapons WHERE weapon_id = ?", (weapon_id,))
        weapon = await cursor.fetchone()
        if not weapon:
            text = (
                "❌ <b>Ошибка</b>\n\nОружие не найдено."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        else:
            name, cost_fez = weapon
            await state.set_state(BossStates.buy_quantity)
            await state.update_data(weapon_id=weapon_id, weapon_name=name, cost_fez=cost_fez)
            text = (
                f"<b>🛒 Покупка оружия: {name}</b>\n\n"
                f"💰 <b>Цена за единицу:</b> <code>{form_balance(cost_fez)}</code> Fezcoin\n"
                f"💎 <b>Ваш баланс:</b> <code>{form_balance(fezcoin)}</code> Fezcoin\n\n"
                "➡️ Введите <b>количество</b> оружия для покупки (<i>целое число, минимум 1, или 'все' для макс. количества</i>):"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"weapon_details_{weapon_id}")]])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Обработка количества для покупки
@dp.message(BossStates.buy_quantity)
async def process_weapon_quantity(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Эта функция доступна только в личных сообщениях с ботом!",
            parse_mode="HTML"
        )
        await state.clear()
        return
    data = await state.get_data()
    weapon_id = data.get("weapon_id")
    weapon_name = data.get("weapon_name")
    cost_fez = data.get("cost_fez")
    user_id = message.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
        fezcoin = (await cursor.fetchone())[0]

    try:
        quantity = parse_bet_input_boss(message.text, fezcoin // cost_fez)
        if quantity < 1:
            raise ValueError("Количество должно быть больше 0")
    except ValueError:
        text = (
            "<b>🚫 Ошибка ввода</b>\n\n"
            "❌ Введите <b>целое число</b> (минимум <code>1</code>, или 'все' для максимума).\n"
            "➡️ Попробуйте снова или вернитесь назад."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"weapon_details_{weapon_id}")]])
        await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.clear()
        return

    total_cost = cost_fez * quantity
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
        fezcoin = (await cursor.fetchone())[0]
        if total_cost > fezcoin:
            text = (
                "<b>🚫 Недостаточно Fezcoin</b>\n\n"
                f"💰 <b>Требуется:</b> <code>{form_balance(total_cost)}</code> Fezcoin\n"
                f"💎 <b>Ваш баланс:</b> <code>{form_balance(fezcoin)}</code> Fezcoin\n"
                "➡️ Уменьшите количество или пополните баланс через <b>обменник</b>."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"weapon_details_{weapon_id}")]])
            await message.reply(text, reply_markup=kb, parse_mode="HTML")
            await state.clear()
            return

    callback_data = f"weapon_confirm_buy_{weapon_id}_{quantity}_{total_cost}"
    text = (
        f"<b>🛒 Подтверждение покупки: {weapon_name}</b>\n\n"
        f"🔢 <b>Количество:</b> <code>{quantity}</code>\n"
        f"💰 <b>Общая стоимость:</b> <code>{form_balance(total_cost)}</code> Fezcoin\n"
        "➡️ Подтвердите покупку, чтобы добавить оружие в ваш <b>инвентарь</b>:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=callback_data)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"weapon_details_{weapon_id}")]
    ])
    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()

# Подтверждение покупки оружия
@dp.callback_query(lambda c: c.data.startswith("weapon_confirm_buy_"))
async def confirm_weapon_buy(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    parts = call.data.split("_")
    try:
        # Если частей больше 5, берем последние три (weapon_id, quantity, total_cost)
        if len(parts) < 5:
            raise ValueError(f"Ожидалось как минимум 5 частей в callback data, получено {len(parts)}")
        # Берем последние три части, игнорируя возможное weapon_name
        weapon_id = int(parts[-3])
        quantity = int(parts[-2])
        total_cost = float(parts[-1])
    except ValueError as e:
        text = (
            f"<b>🚫 Ошибка</b>\n\n"
            f"❌ Неверный формат данных покупки: {str(e)}.\n"
            "➡️ Пожалуйста, выберите оружие заново в магазине."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
    else:
        user_id = call.from_user.id
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
            fezcoin = (await cursor.fetchone())[0]
            cursor = await db.execute("SELECT name FROM weapons WHERE weapon_id = ?", (weapon_id,))
            weapon = await cursor.fetchone()
            if not weapon:
                text = (
                    "<b>🚫 Ошибка</b>\n\n"
                    "❌ Оружие не найдено."
                )
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
            elif total_cost > fezcoin:
                text = (
                    "<b>🚫 Недостаточно Fezcoin</b>\n\n"
                    f"💎 <b>Ваш баланс:</b> <code>{form_balance(fezcoin)}</code> Fezcoin\n"
                    "➡️ Пополните баланс через <b>обменник</b> и попробуйте снова."
                )
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
            else:
                weapon_name = weapon[0]
                await db.execute("UPDATE users SET fezcoin = fezcoin - ? WHERE user_id = ?", (total_cost, user_id))
                await db.execute(
                    "INSERT OR REPLACE INTO player_weapons (user_id, weapon_id, quantity) "
                    "VALUES (?, ?, COALESCE((SELECT quantity FROM player_weapons WHERE user_id = ? AND weapon_id = ?), 0) + ?)",
                    (user_id, weapon_id, user_id, weapon_id, quantity)
                )
                await db.commit()
                text = (
                    f"<b>🛒 Покупка успешна: {weapon_name}</b>\n\n"
                    f"🔢 <b>Куплено:</b> <code>{quantity}</code> единиц\n"
                    f"💰 <b>Потрачено:</b> <code>{form_balance(total_cost)}</code> Fezcoin\n"
                    f"💎 <b>Остаток баланса:</b> <code>{form_balance(fezcoin - total_cost)}</code> Fezcoin\n"
                    "💡 <i>Совет:</i> Проверьте <b>инвентарь</b>, чтобы использовать новое оружие в бою!"
                )
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Инвентарь игрока
@dp.callback_query(lambda c: c.data == "weapon_inventory")
async def handle_weapon_inventory(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT w.name, p.quantity, w.cost_fez, w.base_damage, w.bonus_damage
            FROM player_weapons p
            JOIN weapons w ON p.weapon_id = w.weapon_id
            WHERE p.user_id = ? AND p.quantity > 0
        """, (user_id,))
        weapons = await cursor.fetchall()

    text = (
        "<b>🎒 Ваш инвентарь</b>\n\n"
        "🔹 Здесь отображается всё ваше оружие, готовое для битвы с боссом.\n\n"
    )
    if not weapons:
        text += "❌ <i>Ваш инвентарь пуст.</i>\n➡️ Перейдите в <b>Магазин оружия</b>, чтобы приобрести снаряжение!"
    else:
        for name, qty, cost_fez, base_dmg, bonus_dmg in weapons:
            text += (
                f"🛡️ <b>{name}</b>\n"
                f"🔢 Количество: <code>{qty}</code>\n"
                f"💰 Цена: <code>{form_balance(cost_fez)}</code> Fezcoin\n"
                f"⚔️ Урон: <code>{form_balance(base_dmg)}</code> HP (<i>10% шанс на</i> <code>{form_balance(bonus_dmg)}</code> HP)\n\n"
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Атака босса
@dp.callback_query(lambda c: c.data == "boss_attack")
async def handle_boss_attack(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    user_id = call.from_user.id
    boss = await fetch_current_boss()
    if not boss or boss['hp_current'] <= 0:
        text = (
            "<b>⚔️ Нанести урон</b>\n\n"
            "❌ <i>Босс уже побеждён или не активен!</i>\n"
            "➡️ Обновите меню или дождитесь нового сезона."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("""
                SELECT w.weapon_id, w.name, p.quantity
                FROM player_weapons p
                JOIN weapons w ON p.weapon_id = w.weapon_id
                WHERE p.user_id = ? AND p.quantity > 0
                ORDER BY w.base_damage
                LIMIT 8
            """, (user_id,))
            weapons = await cursor.fetchall()

        if not weapons:
            text = (
                "<b>⚔️ Нанести урон</b>\n\n"
                "❌ <i>У вас нет оружия!</i>\n"
                "➡️ Зайдите в <b>Магазин оружия</b>, чтобы приобрести снаряжение для битвы."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        else:
            text = (
                f"<b>⚔️ Атака босса: {boss['name']}</b>\n\n"
                f"❤️ <b>Здоровье босса:</b> <code>{format_balance_boss(boss['hp_current'])}</code> HP\n"
                "🔹 Выберите оружие для атаки:\n"
                "💡 <i>Совет:</i> Оружие с высоким <b>бонусным уроном</b> может нанести критический удар!"
            )
            kb_rows = [[InlineKeyboardButton(text=f"{name} ({qty})", callback_data=f"attack_weapon_{wid}")] for wid, name, qty in weapons]
            kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")])
            kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Выбор оружия для атаки
@dp.callback_query(lambda c: c.data.startswith("attack_weapon_"))
async def handle_attack_weapon(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    weapon_id = int(call.data.split("_")[2])
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT w.name, p.quantity
            FROM player_weapons p
            JOIN weapons w ON p.weapon_id = w.weapon_id
            WHERE p.user_id = ? AND p.weapon_id = ?
        """, (user_id, weapon_id))
        weapon = await cursor.fetchone()
        if not weapon:
            text = (
                "❌ <b>Ошибка</b>\n\nОружие не найдено."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        else:
            name, quantity = weapon
            await state.set_state(BossStates.attack_quantity)
            await state.update_data(weapon_id=weapon_id, weapon_name=name)
            text = (
                f"<b>⚔️ Использовать оружие: {name}</b>\n\n"
                f"🔢 <b>В наличии:</b> <code>{quantity}</code>\n"
                "➡️ Введите <b>количество</b> для атаки (<i>целое число от 1 до</i> <code>{quantity}</code>, или 'все' для максимума):"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_attack")]])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Обработка количества для атаки
@dp.message(BossStates.attack_quantity)
async def process_attack_quantity(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        await message.reply(
            "❌ <b>Ошибка:</b> Эта функция доступна только в личных сообщениях с ботом!",
            parse_mode="HTML"
        )
        await state.clear()
        return
    data = await state.get_data()
    weapon_id = data.get("weapon_id")
    weapon_name = data.get("weapon_name")
    user_id = message.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT quantity, w.base_damage, w.bonus_damage, w.bonus_chance
            FROM player_weapons p
            JOIN weapons w ON p.weapon_id = w.weapon_id
            WHERE p.user_id = ? AND p.weapon_id = ?
        """, (user_id, weapon_id))
        weapon = await cursor.fetchone()
        if not weapon:
            text = (
                "<b>🚫 Ошибка</b>\n\n"
                f"❌ У вас недостаточно <b>{weapon_name}</b> (в наличии: <code>0</code>).\n"
                "➡️ Проверьте <b>инвентарь</b> или купите больше оружия."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_attack")]])
            await message.reply(text, reply_markup=kb, parse_mode="HTML")
            await state.clear()
            return
        quantity_available, base_damage, bonus_damage, bonus_chance = weapon

    try:
        quantity = parse_bet_input_boss(message.text, quantity_available)
        if quantity < 1:
            raise ValueError
    except:
        text = (
            "<b>🚫 Ошибка ввода</b>\n\n"
            "❌ Введите <b>целое число</b> (минимум <code>1</code>, или 'все' для максимума).\n"
            "➡️ Попробуйте снова или вернитесь назад."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_attack")]])
        await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.clear()
        return

    boss = await fetch_current_boss()
    if not boss or boss['hp_current'] <= 0:
        text = (
            "<b>🚫 Ошибка</b>\n\n"
            "❌ <i>Босс уже побеждён или не активен!</i>\n"
            "➡️ Обновите меню, чтобы проверить статус."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        await message.reply(text, reply_markup=kb, parse_mode="HTML")
        await state.clear()
        return

    # Ограничение количества оружия, чтобы не превысить оставшееся HP босса (используем base_damage для расчета)
    max_quantity = min(quantity_available, boss['hp_current'] // base_damage)
    if quantity > max_quantity:
        if max_quantity == 0:
            text = (
                "<b>🚫 Ошибка</b>\n\n"
                "❌ <i>Урон от этого оружия слишком мал, или HP босса недостаточно для атаки.</i>\n"
                "➡️ Выберите другое оружие или дождитесь обновления."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_attack")]])
            await message.reply(text, reply_markup=kb, parse_mode="HTML")
            await state.clear()
            return
        quantity = max_quantity
        text_note = f"\n⚠️ <b>Внимание:</b> Количество уменьшено до <code>{quantity}</code>, чтобы не превысить оставшееся HP босса."
    else:
        text_note = ""

    total_damage = 0
    for _ in range(quantity):
        damage = bonus_damage if random.random() < bonus_chance else base_damage
        total_damage += damage

    # Кап урона, если превышает оставшееся HP
    if total_damage > boss['hp_current']:
        total_damage = boss['hp_current']

    new_hp = boss['hp_current'] - total_damage
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bosses SET hp_current = ? WHERE boss_id = ?", (new_hp, boss['boss_id']))
        await db.execute(
            "INSERT OR REPLACE INTO player_boss_damage (user_id, boss_id, damage_dealt) "
            "VALUES (?, ?, COALESCE((SELECT damage_dealt FROM player_boss_damage WHERE user_id = ? AND boss_id = ?), 0) + ?)",
            (user_id, boss['boss_id'], user_id, boss['boss_id'], total_damage)
        )
        await db.execute("UPDATE users SET boss_experience = boss_experience + ? WHERE user_id = ?", (total_damage, user_id))
        await db.execute("UPDATE player_weapons SET quantity = quantity - ? WHERE user_id = ? AND weapon_id = ?", (quantity, user_id, weapon_id))
        await db.commit()

        text = (
            f"<b>⚔️ Атака босса: {boss['name']}</b>\n\n"
            f"🛡️ <b>Использовано:</b> {weapon_name} (<code>{quantity}</code>)\n"
            f"💥 <b>Нанесено урона:</b> <code>{form_balance(total_damage)}</code> HP\n"
            f"❤️ <b>Остаток HP босса:</b> <code>{format_balance_boss(new_hp)}</code>/<code>{format_balance_boss(boss['hp_total'])}</code>\n"
            f"📈 <b>Получено опыта:</b> <code>{form_balance(total_damage)}</code>\n"
            "💡 <i>Совет:</i> Продолжайте атаковать, чтобы подняться в <b>топе</b>!" + text_note
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])

        if new_hp == 0:
            # Автоматический обмен опыта
            avg_price = await calculate_avg_fezcoin_price()
            cursor = await db.execute("""
                SELECT u.user_id, u.username, u.boss_experience, u.total_exchanged_exp, u.total_gg_from_exp
                FROM users u
                JOIN player_boss_damage p ON u.user_id = p.user_id
                WHERE p.boss_id = ?
            """, (boss['boss_id'],))
            players = await cursor.fetchall()
            for pid, username, exp, exchanged, gg_from_exp in players:
                gg_amount = exp * avg_price
                await db.execute(
                    "UPDATE users SET coins = coins + ?, boss_experience = 0, total_exchanged_exp = total_exchanged_exp + ?, "
                    "total_gg_from_exp = total_gg_from_exp + ? WHERE user_id = ?",
                    (gg_amount, exp, gg_amount, pid)
                )
                try:
                    cursor = await db.execute("SELECT damage_dealt FROM player_boss_damage WHERE user_id = ? AND boss_id = ?", (pid, boss['boss_id']))
                    damage = (await cursor.fetchone())[0]
                    await bot.send_message(
                        pid,
                        f"<b>🏆 Босс {boss['name']} побеждён!</b>\n\n"
                        f"👹 <b>Ваш вклад:</b> <code>{form_balance(damage)}</code> HP\n"
                        f"📈 <b>Ваш опыт:</b> <code>{form_balance(exp)}</code>\n"
                        f"💰 <b>Автоматически обменяно:</b> <code>{form_balance(gg_amount)}</code> GG\n"
                        f"🔄 <b>Всего обменяно опыта:</b> <code>{form_balance(exchanged + exp)}</code>\n"
                        f"💰 <b>Всего получено GG:</b> <code>{form_balance(gg_from_exp + gg_amount)}</code>\n"
                        f"⏳ <i>Ожидайте нового сезона!</i>",
                        parse_mode="HTML"
                    )
                except:
                    pass
            # Распределение наград по топу
            cursor = await db.execute("""
                SELECT p.user_id, p.damage_dealt, u.username
                FROM player_boss_damage p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.boss_id = ?
                ORDER BY p.damage_dealt DESC
            """, (boss['boss_id'],))
            all_players = await cursor.fetchall()
            top_rewards = [3000000000, 1500000000, 1000000000] + [300000000] * 7  # Top 1-3 + Top 4-10
            other_reward = 50000

            for rank, (pid, damage, username) in enumerate(all_players):
                if damage <= 1:
                    continue
                if rank < 10:
                    reward = top_rewards[rank]
                else:
                    reward = other_reward
                await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (reward, pid))
                try:
                    await bot.send_message(
                        pid,
                        f"<b>🏆 Награда за победу над боссом {boss['name']}!</b>\n\n"
                        f"📊 <b>Ваш ранг:</b> {rank + 1}\n"
                        f"👹 <b>Ваш урон:</b> <code>{form_balance(damage)}</code> HP\n"
                        f"💰 <b>Награда:</b> <code>{form_balance(reward)}</code> GG\n"
                        "🎉 Поздравляем! Продолжайте в том же духе в следующем сезоне.",
                        parse_mode="HTML"
                    )
                except:
                    pass
            await db.execute("UPDATE bosses SET active = 0 WHERE boss_id = ?", (boss['boss_id'],))
            await db.commit()

    await message.reply(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()

# Обменник опыта
@dp.callback_query(lambda c: c.data == "boss_exchange")
async def handle_boss_exchange(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT boss_experience FROM users WHERE user_id = ?", (user_id,))
        exp = (await cursor.fetchone())[0]
        if exp <= 0:
            text = (
                "<b>🔄 Обменник опыта</b>\n\n"
                "❌ <i>У вас нет опыта для обмена.</i>\n"
                "➡️ Нанесите урон боссу, чтобы заработать <b>опыт</b>! Каждый удар приносит опыт, равный нанесённому урону."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        else:
            avg_price = await calculate_avg_fezcoin_price()
            gg_amount = exp * avg_price
            text = (
                "<b>🔄 Обменник опыта</b>\n\n"
                f"📈 <b>Ваш опыт:</b> <code>{form_balance(exp)}</code>\n"
                f"💰 <b>Текущий курс:</b> <code>{form_balance(avg_price)}</code> GG за 1 опыт\n"
                f"➡️ <b>Получите:</b> <code>{form_balance(gg_amount)}</code> GG\n"
                "🔹 Подтвердите, чтобы обменять весь опыт на <b>GG</b> монеты."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обменять", callback_data="confirm_exchange")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]
            ])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()

# Подтверждение обмена опыта
@dp.callback_query(lambda c: c.data == "confirm_exchange")
async def confirm_exchange(call: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние FSM
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT boss_experience FROM users WHERE user_id = ?", (user_id,))
        exp = (await cursor.fetchone())[0]
        if exp <= 0:
            text = (
                "<b>🔄 Обменник опыта</b>\n\n"
                "❌ <i>У вас нет опыта для обмена.</i>\n"
                "➡️ Нанесите урон боссу, чтобы заработать <b>опыт</b>! Каждый удар приносит опыт, равный нанесённому урону."
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])
        else:
            avg_price = await calculate_avg_fezcoin_price()
            gg_amount = exp * avg_price
            cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
            current_coins = (await cursor.fetchone())[0]
            await db.execute(
                "UPDATE users SET coins = coins + ?, boss_experience = 0, total_exchanged_exp = total_exchanged_exp + ?, "
                "total_gg_from_exp = total_gg_from_exp + ? WHERE user_id = ?",
                (gg_amount, exp, gg_amount, user_id)
            )
            await db.commit()
            text = (
                "<b>🔄 Обмен успешен</b>\n\n"
                f"📈 <b>Обменяно:</b> <code>{form_balance(exp)}</code> опыта\n"
                f"💰 <b>Получено:</b> <code>{form_balance(gg_amount)}</code> GG\n"
                f"💸 <b>Ваш баланс GG:</b> <code>{form_balance(current_coins + gg_amount)}</code> GG\n"
                "💡 <i>Совет:</i> Используйте <b>GG</b> для покупки <b>Fezcoin</b> или других бонусов в игре!"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="boss_main")]])

    message_key = f"{call.message.chat.id}_{call.message.message_id}"
    reply_markup_str = str(kb.inline_keyboard) if kb else ""
    new_state_hash = md5((text + reply_markup_str).encode()).hexdigest()
    last_state = last_message_state.get(message_key, {"hash": None})

    if last_state["hash"] != new_state_hash:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[message_key] = {"hash": new_state_hash}
        except Exception:
            await call.message.delete()
            new_msg = await call.message.reply(text, reply_markup=kb, parse_mode="HTML")
            last_message_state[f"{new_msg.chat.id}_{new_msg.message_id}"] = {"hash": new_state_hash}
    await call.answer()


#=================================== БОКС ===========================

BOX_PRIZES = {
    0: [150, 300, 450, 600, 750, 900, 1050, 1200, 1350, 1500],
    1: [300, 600, 9000, 1200, 1500, 1800, 2100, 2400, 2700, 3000],
    2: [600, 1200, 1800, 2400, 3000, 3600, 4200, 4800, 5400, 6000],
    3: [1200, 2400, 3600, 4800, 6000, 7200, 8400, 9600, 10800, 12000],
    4: [2400, 4800, 7200, 9600, 12000, 14400, 16800, 19200, 21600, 24000],
    5: [3000, 6000, 9000, 12000, 15000, 18000, 21000, 24000, 27000, 30000],
    6: [9000, 18000, 27000, 36000, 45000, 54000, 63000, 72000, 81000, 90000],
    7: [21000, 42000, 63000, 84000, 105000, 126000, 147000, 168000, 189000, 210000],
    8: [30000, 60000, 90000, 120000, 150000, 180000, 210000, 240000, 270000, 300000],
    9: [150000, 300000, 450000, 600000, 750000, 900000, 1050000, 1200000, 1350000, 1500000],
    10: [300000, 600000, 900000, 1200000, 1500000, 1800000, 2100000, 2400000, 2700000, 3000000],
    11: [900000, 1800000, 2700000, 3600000, 4500000, 5400000, 6300000, 7200000, 8100000, 9000000],
    12: [1500000, 3000000, 4500000, 6000000, 7500000, 9000000, 10500000, 12000000, 13500000, 15000000],
    13: [2100000, 4200000, 6300000, 8400000, 10500000, 12600000, 14700000, 16800000, 18900000, 21000000]
}

class BoxStates(StatesGroup):
    choosing = State()

@dp.message(Command("box"))
@dp.message(lambda m: m.text and m.text.lower() in ["бокс", "коробка"])
async def cmd_box(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    now = datetime.now(UTC)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT status, last_box FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return

        user_status, last_box = result
        if last_box:
            last_box_time = datetime.fromisoformat(last_box)
            time_diff = now - last_box_time
            if time_diff < timedelta(hours=6):
                remaining = timedelta(hours=6) - time_diff
                hours, remainder = divmod(remaining.seconds, 3600)
                minutes = remainder // 60
                await message.reply(
                    f"❌ Коробку можно открывать раз в 6 часов. Попробуйте снова через {hours} ч {minutes} мин.",
                    parse_mode="HTML"
                )
                return

        # Get prizes based on user status
        prizes = BOX_PRIZES.get(user_status, BOX_PRIZES[0])
        selected_prizes = random.sample(prizes, 5) + [0]  # 5 random prizes + 1 empty
        random.shuffle(selected_prizes)

        # Create inline keyboard with 6 buttons
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🎁", callback_data=f"box_open:{user_id}:0"),
                    InlineKeyboardButton(text="🎁", callback_data=f"box_open:{user_id}:1"),
                    InlineKeyboardButton(text="🎁", callback_data=f"box_open:{user_id}:2")
                ],
                [
                    InlineKeyboardButton(text="🎁", callback_data=f"box_open:{user_id}:3"),
                    InlineKeyboardButton(text="🎁", callback_data=f"box_open:{user_id}:4"),
                    InlineKeyboardButton(text="🎁", callback_data=f"box_open:{user_id}:5")
                ]
            ]
        )

        # Store prizes in state
        state_key = StorageKey(bot_id=bot.id, chat_id=message.chat.id, user_id=user_id)
        await state.set_state(BoxStates.choosing)
        await state.update_data(prizes=selected_prizes)

        await message.reply(
            "🎁 <b>Открытие коробки</b> 🎁\n\n"
            "<b>Выберите одну из шести коробок! В пяти из них спрятаны монеты, а одна пуста.</b>",
            reply_markup=kb,
            parse_mode="HTML"
        )


@dp.callback_query(lambda c: c.data.startswith("box_open:"))
async def box_open_callback(call: types.CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    if len(parts) != 3:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    try:
        original_user_id = int(parts[1])
        prize_index = int(parts[2])
    except ValueError:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    if call.from_user.id != original_user_id:
        await call.answer("❌ Эта коробка не для вас! Используйте /box, чтобы открыть свою.", show_alert=True)
        return

    data = await state.get_data()
    prizes = data.get("prizes", [])
    if not prizes or prize_index < 0 or prize_index >= len(prizes):
        await call.answer("❌ Ошибка: Неверные данные коробки.", show_alert=True)
        return

    prize = prizes[prize_index]
    now = datetime.now(UTC).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        if prize > 0:
            await db.execute("UPDATE users SET coins = coins + ?, last_box = ? WHERE user_id = ?",
                             (prize, now, call.from_user.id))
        else:
            await db.execute("UPDATE users SET last_box = ? WHERE user_id = ?",
                             (now, call.from_user.id))
        await db.commit()

    await call.message.edit_text(
        f"🎁 <b>Вы открыли коробку!</b>\n\n"
        f"<b>{'💰 Вы получили: <code>' + format_balance(prize) + '</code> GG!' if prize > 0 else '😔 Коробка оказалась пуста.'}</b>\n"
        f"<b>Попробуйте снова через 6 часов!</b>",
        parse_mode="HTML"
    )
    await state.clear()
    await call.answer()

#=================================== КУБИК ===========================

async def dice_usage(name: str) -> str:
    return (
        f"🎲 <b>Игра: Кубик</b> 🎲\n"
        f"───────────────────────\n"
        f"👤 <b>{name}</b>, выберите режим игры:\n\n"
        f"🎯 <b>Форматы игры:</b>\n"
        f"  • /dice [ставка] [1-6] — Угадать точное число (x5.5)\n"
        f"  • /dice [ставка] [б/м] — Больше (4–6) или Меньше (1–3) (x1.9)\n"
        f"  • /dice [ставка] [ч/н] — Чётное или Нечётное (x1.9)\n\n"
        f"💡 <b>Примеры:</b>\n"
        f"  • /dice 100 4\n"
        f"  • кубик 200 м\n"
        f"  • /dice 300 ч\n\n"
        f"📊 <b>Минимальная ставка:</b> 10 GG\n"
        f"✨ Попробуйте угадать и сорвать куш!"
    )

def parse_dice_mode(arg: str):
    t = arg.lower()
    if t.isdigit() and 1 <= int(t) <= 6:
        return "num", int(t)
    if t in ("б", "больше", ">", "high", "h"):
        return "hi", None
    if t in ("м", "меньше", "<", "low", "l"):
        return "lo", None
    if t in ("ч", "чет", "четное", "четн", "even", "e"):
        return "even", None
    if t in ("н", "нч", "нечет", "odd", "o"):
        return "odd", None
    return None, None

def weighted_dice_roll(mode: str, param: int) -> int:
    """Generate dice roll with ~42% win probability for hi/lo/even/odd."""
    outcomes = [1, 2, 3, 4, 5, 6]
    base_weights = [0.18, 0.18, 0.18, 0.15, 0.15, 0.15]  # Default weights
    if mode == "num" and param in outcomes:
        weights = [0.167 if i == param else 0.167 for i in outcomes]  # ~16.7% for num
    elif mode == "hi":
        weights = [0.193, 0.193, 0.193, 0.14, 0.14, 0.14]  # ~42% for 4–6
    elif mode == "lo":
        weights = [0.14, 0.14, 0.14, 0.193, 0.193, 0.193]  # ~42% for 1–3
    elif mode == "even":
        weights = [0.193, 0.14, 0.193, 0.14, 0.193, 0.14]  # ~42% for 2,4,6
    elif mode == "odd":
        weights = [0.14, 0.193, 0.14, 0.193, 0.14, 0.193]  # ~42% for 1,3,5
    else:
        weights = base_weights
    return random.choices(outcomes, weights=weights, k=1)[0]

@dp.message(Command("dice"))
async def cmd_dice(message: types.Message):
    args = message.text.split()
    name = message.from_user.first_name or "Игрок"

    if len(args) < 3:
        return await message.reply(await dice_usage(name), parse_mode="HTML")

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
    if not row:
        return await message.reply("❌ <b>Вы не зарегистрированы. Введите /start.</b>", parse_mode="HTML")
    coins = row[0]

    bet = parse_bet_input(args[1], coins)
    if bet < 10:
        return await message.reply("❗ <b>Минимальная ставка — 10 GG.</b>", parse_mode="HTML")
    if coins < bet:
        return await message.reply(f"❌ <b>Недостаточно GG. Ваш баланс: {format_balance(coins)}.</b>", parse_mode="HTML")

    mode, param = parse_dice_mode(args[2])
    if not mode:
        return await message.reply(await dice_usage(name), parse_mode="HTML")

    # Roll dice with weighted probability
    roll = weighted_dice_roll(mode, param)

    # Determine win
    if mode == "num":
        win = bet * 5.5 if roll == param else 0
        cond = f"угадать число <b>{param}</b> (x6)"
    elif mode == "hi":
        win = bet * 1.9 if roll >= 4 else 0
        cond = "выпадет <b>больше</b> (4–6, x2)"
    elif mode == "lo":
        win = bet * 1.9 if roll <= 3 else 0
        cond = "выпадет <b>меньше</b> (1–3, x2)"
    elif mode == "even":
        win = bet * 1.9 if roll % 2 == 0 else 0
        cond = "выпадет <b>чётное</b> (x2)"
    else:
        win = bet * 1.9 if roll % 2 == 1 else 0
        cond = "выпадет <b>нечётное</b> (x2)"

    win = int(win)

    # Update database
    async with aiosqlite.connect(DB_PATH) as db:
        if win > 0:
            await db.execute(
                "UPDATE users SET coins = coins - ? + ?, win_amount = win_amount + ? WHERE user_id = ?",
                (bet, win, win - bet, user_id)
            )
        else:
            await db.execute(
                "UPDATE users SET coins = coins - ?, lose_amount = lose_amount + ? WHERE user_id = ?",
                (bet, bet, user_id)
            )
        await db.commit()

    # Get new balance
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        new_balance = (await cur.fetchone())[0]

    # Format result
    emoji = "🎉" if win > 0 else "😔"
    result_text = (
        f"🎲 <b>Кубик</b> {emoji}\n"
        f"───────────────────────\n"
        f"🎯 <b>Условие:</b> {cond}\n"
        f"🎲 <b>Выпало:</b> {roll}\n"
        
        f"{'✅ <b>Выигрыш:</b> ' + format_balance(win) + ' GG' if win > 0 else '💸 <b>Проигрыш:</b> ' + format_balance(bet) + ' GG'}\n"
        f"💎 <b>Баланс:</b> {format_balance(new_balance)} GG"
    )
    await message.reply(result_text, parse_mode="HTML")

@dp.message(lambda m: m.text and m.text.lower().startswith("кубик"))
async def txt_dice(message: types.Message):
    await cmd_dice(message)

#=================================== КОСТИ ===========================

async def cmd_cubes(message: types.Message):
    user_id = message.from_user.id
    args = message.text.split()

    # Проверка регистрации пользователя
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_coins = result[0]

    # Проверка формата команды
    if len(args) < 3:
        await message.reply(
            "🎲 <b>Игра в кости</b> 🎲\n\n"
            "📋 <b>Формат:</b> /cubes <code>сумма</code> <code>[тип ставки]</code>\n"
            "📝 <b>Примеры:</b>\n"
            "  • /cubes 1000 7\n"
            "  • /cubes 1k больше\n"
            "  • /cubes 500к чет\n\n"
            "🔹 <b>Типы ставок:</b>\n"
            "  • Число от 2 до 12 (x6)\n"
            "  • больше, бол, б, 8-12 (x1.9)\n"
            "  • меньше, мал, м, 2-6 (x1.9)\n"
            "  • чет, чёт, четное, ч (x1.9)\n"
            "  • нечет, нечёт, нечетное, нч (x1.9)\n\n"
            "<i>💡 Минимальная ставка: 10 GG</i>",
            parse_mode="HTML"
        )
        return

    # Парсинг ставки
    bet_input = args[1]
    bet = parse_bet_input(bet_input, user_coins)
    if bet < 10:
        await message.reply("❗ Минимальная ставка — <b>10</b> GG.", parse_mode="HTML")
        return
    if bet > user_coins:
        await message.reply(
            f"❌ Недостаточно GG. Ваш баланс: <code>{format_balance(user_coins)}</code>",
            parse_mode="HTML"
        )
        return

    # Парсинг типа ставки
    pred_str = args[2].lower()
    valid_numbers = [str(i) for i in range(2, 13)]
    valid_high = ("больше", "бол", "б", "8-12")
    valid_low = ("меньше", "мал", "м", "2-6")
    valid_even = ("чет", "чёт", "четное", "ч")
    valid_odd = ("нечет", "нечёт", "нечетное", "нч")

    pred_type = None
    pred_num = None
    if pred_str in valid_numbers:
        pred_type = "number"
        pred_num = int(pred_str)
    elif pred_str in valid_high:
        pred_type = "high"
    elif pred_str in valid_low:
        pred_type = "low"
    elif pred_str in valid_even:
        pred_type = "even"
    elif pred_str in valid_odd:
        pred_type = "odd"
    else:
        await message.reply("❌ Неверный тип ставки. Используйте число (2-12), больше, меньше, чет или нечет.",
                           parse_mode="HTML")
        return

    # Определение шанса выигрыша
    if pred_type == "number":
        win_chance = 0.05  # 5% для конкретного числа
        multiplier = 6
    elif pred_type in ("high", "low", "even", "odd"):
        win_chance = 0.4  # 40% для групп
        multiplier = 1.9
    else:
        # Резервная проверка для безопасности
        await message.reply("❌ Ошибка: неверный тип ставки.", parse_mode="HTML")
        return

    # Решение о выигрыше
    is_win = random.random() < win_chance

    # Списки возможных сумм
    all_sums = list(range(2, 13))
    high_sums = [8, 9, 10, 11, 12]
    low_sums = [2, 3, 4, 5, 6]
    even_sums = [2, 4, 6, 8, 10, 12]
    odd_sums = [3, 5, 7, 9, 11]

    # Выбор суммы в зависимости от типа ставки и исхода
    total = None  # Инициализируем total явно
    if pred_type == "number":
        if is_win:
            total = pred_num
        else:
            total = random.choice([s for s in all_sums if s != pred_num])
    elif pred_type == "high":
        if is_win:
            total = random.choice(high_sums)
        else:
            total = random.choice([s for s in all_sums if s not in high_sums])
    elif pred_type == "low":
        if is_win:
            total = random.choice(low_sums)
        else:
            total = random.choice([s for s in all_sums if s not in low_sums])
    elif pred_type == "even":
        if is_win:
            total = random.choice(even_sums)
        else:
            total = random.choice([s for s in all_sums if s not in even_sums])
    elif pred_type == "odd":
        if is_win:
            total = random.choice(odd_sums)
        else:
            total = random.choice([s for s in all_sums if s not in odd_sums])

    # Проверка, что total определена
    if total is None:
        total = random.randint(2, 12)  # Резервное значение

    # Генерация кубиков для выбранной суммы
    def generate_dice_for_sum(target_sum):
        possible_pairs = [(i, target_sum - i) for i in range(1, 7) if 1 <= target_sum - i <= 6]
        if possible_pairs:
            return random.choice(possible_pairs)
        return None

    dice_pair = generate_dice_for_sum(total)
    if dice_pair:
        dice1, dice2 = dice_pair
    else:
        # Fallback: если generate_dice_for_sum вернул None
        dice1 = random.randint(1, 6)
        dice2 = random.randint(1, 6)
        total = dice1 + dice2

    # Расчет выплаты
    payout = int(bet * multiplier) if is_win else 0

    # Обновление баланса и статистики
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        if payout > 0:
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (payout, user_id))
            await db.execute("UPDATE users SET win_amount = win_amount + ? WHERE user_id = ?", (payout - bet, user_id))
        else:
            await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?", (bet, user_id))
        await db.commit()

        # Получение нового баланса
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        new_balance = (await cursor.fetchone())[0]

    # Формирование результата
    result_text = (
        f"<b>🎲 Игра в кости</b>\n"
        f"<blockquote>🎲 <b>Кубики:</b> {dice1} + {dice2} = <code>{total}</code></blockquote>\n"
        f"{'<i>🎉 Выигрыш:</i>' if payout > 0 else '<i>😔 Проигрыш:</i>'} <i><b>{format_balance(payout if payout > 0 else bet)} GG</b></i>\n"
        f"<i>💰 Баланс:</i> <i><b>{format_balance(new_balance)} GG</b></i>"
    )
    await message.reply(result_text, parse_mode="HTML")


@dp.message(lambda m: m.text and m.text.lower().startswith("кости"))
async def txt_cubes(message: types.Message):
    await cmd_cubes(message)

#=================================== ВЫДАЧА ===========================


@dp.message(Command("hhh"))
async def cmd_hhh(message: types.Message):
    admin_id = 6492780518
    if message.from_user.id != admin_id:
        await message.reply("⛔ Только для админов.")
        return
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /hhh <сумма> <айди>")
        return
    amount = parse_bet_input(args[1])
    target_id = args[2]
    if amount <= 0 or not target_id.isdigit():
        await message.reply("Некорректные данные.")
        return
    target_id = int(target_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (amount, target_id))
        await db.commit()
    await message.reply(f"✅ Пользователю <code>{target_id}</code> начислено <b>{format_balance(amount)}</b> монет.", parse_mode="HTML")

@dp.message(Command("uhhh"))
async def cmd_uhhh(message: types.Message):
    admin_id = 6492780518
    if message.from_user.id != admin_id:
        await message.reply("⛔ Только для админов.")
        return
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /uhhh <сумма> <айди>")
        return
    amount = parse_bet_input(args[1])
    target_id = args[2]
    if amount <= 0 or not target_id.isdigit():
        await message.reply("Некорректные данные.")
        return
    target_id = int(target_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (amount, target_id))
        await db.commit()
    await message.reply(f"❌ У пользователя <code>{target_id}</code> снято <b>{format_balance(amount)}</b> монет.", parse_mode="HTML")



#=================================== ДУЭЛЬ ===========================

active_duel_tasks = set()

class DuelStates(StatesGroup):
    waiting_challenger_choice = State()
    waiting_opponent_choice = State()

@dp.message(lambda m: m.text and m.text.lower().startswith(("/duel", "дуэль", "дуель")))
async def txt_duel(message: types.Message, state: FSMContext):
    await cmd_duel(message, state)
def determine_winner(challenger_choice: str, opponent_choice: str) -> str:
    """Определяет победителя в игре 'камень-ножницы-бумага'."""
    if challenger_choice == opponent_choice:
        return "tie"
    winning_combinations = {
        "rock": "scissors",
        "paper": "rock",
        "scissors": "paper"
    }
    if winning_combinations.get(challenger_choice) == opponent_choice:
        return "win_challenger"
    return "win_opponent"

async def duel_timeout(duel_id: int, challenger_id: int, opponent_id: int, stake: int, message_id: int, chat_id: int, state: FSMContext, opponent_state: FSMContext):
    """Обработчик таймаута дуэли (5 минут)."""
    await asyncio.sleep(300)  # 5 минут
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT status, challenger_choice, opponent_choice FROM duels WHERE duel_id = ?",
            (duel_id,)
        )
        result = await cursor.fetchone()
        if not result or result[0] != "accepted":
            return
        status, challenger_choice, opponent_choice = result
        if challenger_choice and opponent_choice:
            return  # Оба игрока сделали выбор, таймаут не нужен

        # Возврат ставок
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, challenger_id))
        if opponent_id:
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, opponent_id))
        await db.execute("UPDATE duels SET status = 'cancelled', result = 'timeout' WHERE duel_id = ?", (duel_id,))
        await db.commit()

    # Обновление сообщения
    try:
        challenger = await bot.get_chat(challenger_id)
        challenger_name = challenger.first_name or f"ID {challenger_id}"
    except Exception:
        challenger_name = f"ID {challenger_id}"
    opponent_name = "Не выбран"
    if opponent_id:
        try:
            opponent = await bot.get_chat(opponent_id)
            opponent_name = opponent.first_name or f"ID {opponent_id}"
        except Exception:
            opponent_name = f"ID {opponent_id}"

    text = (
        "🎯 <b>Дуэль отменена</b> 🎯\n\n"
        f"👤 <b>Инициатор:</b> {challenger_name}\n"
        f"👤 <b>Противник:</b> {opponent_name}\n"
        f"💰 <b>Ставка:</b> <code>{format_balance(stake)}</code> GG\n"
        "❌ Дуэль отменена из-за превышения времени ожидания (5 минут)."
    )
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=None
        )
    except Exception as e:
        print(f"Ошибка при редактировании сообщения: {e}")
    await state.clear()
    if opponent_id:
        await opponent_state.clear()





@dp.message(lambda m: m.text and m.text.lower().startswith(("/duel")))
async def cmd_duel(message: types.Message, state: FSMContext):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply(
            "❌ <b>Ошибка:</b> Команда /duel доступна только в групповых чатах!",
            parse_mode="HTML"
        )
        return

    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверка регистрации и баланса
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply(
                "❌ <b>Ошибка:</b> Вы не зарегистрированы. Используйте <code>/start</code>.",
                parse_mode="HTML"
            )
            return
        user_coins = result[0]

    # Парсинг ставки
    args = message.text.split()
    if len(args) < 2:
        await message.reply(
            "🎯 <b>Дуэль</b> 🎯\n\n"
            "📋 <b>Использование:</b>\n"
            "  • /duel &lt;ставка&gt;\n"
            "  • Пример: <code>/duel 1000</code>\n"
            "💰 Минимальная ставка: 10 GG\n"
            "📢 Дуэль доступна только в чатах!",
            parse_mode="HTML"
        )
        return

    stake = parse_bet_input(args[1], user_coins)
    if stake < 10:
        await message.reply(
            "❌ <b>Ошибка:</b> Минимальная ставка — <code>10</code> GG.",
            parse_mode="HTML"
        )
        return
    if stake > user_coins:
        await message.reply(
            f"❌ <b>Ошибка:</b> Недостаточно монет. Ваш баланс: <code>{format_balance(user_coins)}</code> GG.",
            parse_mode="HTML"
        )
        return

    # Проверка на существующую активную дуэль
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT duel_id FROM duels WHERE challenger_id = ? AND status = 'pending'",
            (user_id,)
        )
        if await cursor.fetchone():
            await message.reply(
                "❌ <b>Ошибка:</b> У вас уже есть активная дуэль. Отмените её или дождитесь завершения.",
                parse_mode="HTML"
            )
            return

        # Списываем ставку и создаем дуэль
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (stake, user_id))
        now = datetime.now(UTC).isoformat()
        cursor = await db.execute(
            "INSERT INTO duels (challenger_id, stake, status, chat_id, created_at) VALUES (?, ?, 'pending', ?, ?)",
            (user_id, stake, chat_id, now)
        )
        await db.commit()
        duel_id = cursor.lastrowid

    # Создаем клавиатуру
    try:
        challenger = await bot.get_chat(user_id)
        challenger_name = challenger.first_name or f"ID {user_id}"
    except Exception:
        challenger_name = f"ID {user_id}"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять вызов", callback_data=f"duel_accept:{duel_id}"),
                InlineKeyboardButton(text="❌ Отменить", callback_data=f"duel_cancel:{duel_id}")
            ]
        ]
    )

    # Отправляем начальное сообщение дуэли
    text = (
        "🎯 <b>Дуэль создана!</b> 🎯\n\n"
        f"👤 <b>Инициатор:</b> {challenger_name}\n"
        f"💰 <b>Ставка:</b> <code>{format_balance(stake)}</code> GG\n"
        "➡️ Нажмите 'Принять вызов', чтобы участвовать!"
    )
    sent_message = await message.reply(text, parse_mode="HTML", reply_markup=keyboard)

    # Обновляем duel с message_id
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE duels SET message_id = ? WHERE duel_id = ?",
            (sent_message.message_id, duel_id)
        )
        await db.commit()



@dp.callback_query(lambda c: c.data.startswith("duel_accept:"))
async def duel_accept_callback(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    duel_id = int(call.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT challenger_id, stake, status, message_id, chat_id FROM duels WHERE duel_id = ?",
            (duel_id,)
        )
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Дуэль не найдена.", show_alert=True)
            return
        challenger_id, stake, status, message_id, chat_id = result

        if status != "pending":
            await call.answer("❌ Дуэль уже принята или завершена.", show_alert=True)
            return
        if user_id == challenger_id:
            await call.answer("❌ Вы не можете принять свою собственную дуэль.", show_alert=True)
            return

        # Проверка баланса противника
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Вы не зарегистрированы. Используйте /start.", show_alert=True)
            return
        opponent_coins = result[0]
        if opponent_coins < stake:
            await call.answer(
                f"❌ Недостаточно монет. Нужно: <code>{format_balance(stake)}</code> GG.",
                show_alert=True
            )
            return

        # Списываем ставку противника
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (stake, user_id))
        await db.execute(
            "UPDATE duels SET opponent_id = ?, status = 'accepted' WHERE duel_id = ?",
            (user_id, duel_id)
        )
        await db.commit()

    # Обновляем сообщение с клавиатурой для выбора инициатора
    try:
        challenger = await bot.get_chat(challenger_id)
        challenger_name = challenger.first_name or f"ID {challenger_id}"
    except Exception:
        challenger_name = f"ID {challenger_id}"
    try:
        opponent = await bot.get_chat(user_id)
        opponent_name = opponent.first_name or f"ID {user_id}"
    except Exception:
        opponent_name = f"ID {user_id}"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✊ Камень", callback_data=f"duel_choice:{duel_id}:rock:{challenger_id}")],
            [InlineKeyboardButton(text="✌️ Ножницы", callback_data=f"duel_choice:{duel_id}:scissors:{challenger_id}")],
            [InlineKeyboardButton(text="🖐 Бумага", callback_data=f"duel_choice:{duel_id}:paper:{challenger_id}")]
        ]
    )

    text = (
        "🎯 <b>Дуэль началась!</b> 🎯\n\n"
        f"👤 <b>Инициатор:</b> {challenger_name}\n"
        f"👤 <b>Противник:</b> {opponent_name}\n"
        f"💰 <b>Ставка:</b> <code>{format_balance(stake)}</code> GG\n\n"
        f"➡️ Инициатор <b>{challenger_name}</b>, выберите ваш ход:"
    )
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=keyboard
        )
    except Exception as e:
        print(f"Ошибка при редактировании сообщения: {e}")
        await call.answer("❌ Ошибка при обновлении дуэли.", show_alert=True)
        return
    await call.answer()

    # Устанавливаем состояние для инициатора
    state_key = StorageKey(bot_id=bot.id, chat_id=chat_id, user_id=challenger_id)
    state = FSMContext(dp.storage, key=state_key)
    await state.set_state(DuelStates.waiting_challenger_choice)
    await state.update_data(duel_id=duel_id, message_id=message_id, chat_id=chat_id)

    # Запускаем таймер
    opponent_state_key = StorageKey(bot_id=bot.id, chat_id=chat_id, user_id=user_id)
    opponent_state = FSMContext(dp.storage, key=opponent_state_key)
    asyncio.create_task(duel_timeout(duel_id, challenger_id, user_id, stake, message_id, chat_id, state, opponent_state))

@dp.callback_query(lambda c: c.data.startswith("duel_cancel:"))
async def duel_cancel_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    duel_id = int(call.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT challenger_id, stake, status, message_id, chat_id FROM duels WHERE duel_id = ?",
            (duel_id,)
        )
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Дуэль не найдена.", show_alert=True)
            return
        challenger_id, stake, status, message_id, chat_id = result

        if status != "pending":
            await call.answer("❌ Дуэль уже принята или завершена.", show_alert=True)
            return
        if user_id != challenger_id:
            await call.answer("❌ Вы не можете отменить чужую дуэль.", show_alert=True)
            return

        # Возвращаем ставку и отменяем дуэль
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, challenger_id))
        await db.execute("UPDATE duels SET status = 'cancelled', result = 'cancelled' WHERE duel_id = ?", (duel_id,))
        await db.commit()

    # Обновляем сообщение
    try:
        challenger = await bot.get_chat(challenger_id)
        challenger_name = challenger.first_name or f"ID {challenger_id}"
    except Exception:
        challenger_name = f"ID {challenger_id}"

    text = (
        "🎯 <b>Дуэль отменена</b> 🎯\n\n"
        f"👤 <b>Инициатор:</b> {challenger_name}\n"
        f"💰 <b>Ставка:</b> <code>{format_balance(stake)}</code> GG\n"
        "❌ Дуэль была отменена создателем."
    )
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=None
        )
    except Exception as e:
        print(f"Ошибка при редактировании сообщения: {e}")
        await call.answer("❌ Ошибка при отмене дуэли.", show_alert=True)
        return
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("duel_choice:"))
async def duel_choice_callback(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    data = call.data.split(":")
    duel_id = int(data[1])
    choice = data[2]
    expected_user_id = int(data[3])

    if user_id != expected_user_id:
        await call.answer("❌ Это не ваш ход!", show_alert=True)
        return

    # Открываем новое соединение для этого запроса
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT challenger_id, opponent_id, stake, status, message_id, chat_id, challenger_choice, opponent_choice FROM duels WHERE duel_id = ?",
            (duel_id,)
        )
        result = await cursor.fetchone()
        if not result or result[3] != "accepted":
            await call.answer("❌ Дуэль не найдена или завершена.", show_alert=True)
            return
        challenger_id, opponent_id, stake, status, message_id, chat_id, challenger_choice, opponent_choice = result

    try:
        challenger = await bot.get_chat(challenger_id)
        challenger_name = challenger.first_name or f"ID {challenger_id}"
    except Exception:
        challenger_name = f"ID {challenger_id}"
    try:
        opponent = await bot.get_chat(opponent_id)
        opponent_name = opponent.first_name or f"ID {opponent_id}"
    except Exception:
        opponent_name = f"ID {opponent_id}"

    async with aiosqlite.connect(DB_PATH) as db:
        if user_id == challenger_id and not challenger_choice:
            # Инициатор сделал выбор
            await db.execute(
                "UPDATE duels SET challenger_choice = ? WHERE duel_id = ?",
                (choice, duel_id)
            )
            await db.commit()

            # Обновляем сообщение для выбора противника
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✊ Камень", callback_data=f"duel_choice:{duel_id}:rock:{opponent_id}")],
                    [InlineKeyboardButton(text="✌️ Ножницы",
                                          callback_data=f"duel_choice:{duel_id}:scissors:{opponent_id}")],
                    [InlineKeyboardButton(text="🖐 Бумага", callback_data=f"duel_choice:{duel_id}:paper:{opponent_id}")]
                ]
            )

            text = (
                "🎯 <b>Дуэль продолжается!</b> 🎯\n\n"
                f"👤 <b>Инициатор:</b> {challenger_name} (выбрал)\n"
                f"👤 <b>Противник:</b> {opponent_name}\n"
                f"💰 <b>Ставка:</b> <code>{format_balance(stake)}</code> GG\n\n"
                f"➡️ Противник <b>{opponent_name}</b>, выберите ваш ход:"
            )
            try:
                await bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=message_id,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            except Exception as e:
                print(f"Ошибка при редактировании сообщения: {e}")
                await call.answer("❌ Ошибка при обновлении дуэли.", show_alert=True)
                return

            # Устанавливаем состояние для противника
            opponent_state_key = StorageKey(bot_id=bot.id, chat_id=chat_id, user_id=opponent_id)
            opponent_state = FSMContext(dp.storage, key=opponent_state_key)
            await opponent_state.set_state(DuelStates.waiting_opponent_choice)
            await opponent_state.update_data(duel_id=duel_id, message_id=message_id, chat_id=chat_id)

            # Запускаем таймер
            asyncio.create_task(duel_timeout(duel_id, challenger_id, opponent_id, stake, message_id, chat_id, state, opponent_state))
            await call.answer()
            return

        if user_id == opponent_id and not opponent_choice:
            # Противник сделал выбор
            await db.execute(
                "UPDATE duels SET opponent_choice = ? WHERE duel_id = ?",
                (choice, duel_id)
            )
            await db.commit()

            # Получаем выбор инициатора
            cursor = await db.execute("SELECT challenger_choice FROM duels WHERE duel_id = ?", (duel_id,))
            result = await cursor.fetchone()
            if not result:
                await call.answer("❌ Ошибка получения данных дуэли.", show_alert=True)
                return
            challenger_choice = result[0]

            # Определяем победителя
            result = determine_winner(challenger_choice, choice)
            total_stake = stake * 2
            winner_id = None
            result_text = "tie"

            if result == "win_challenger":
                winner_id = challenger_id
                result_text = f"Победил {challenger_name}!"
                await db.execute("UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?",
                                (total_stake, total_stake - stake, challenger_id))
                await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?",
                                (stake, opponent_id))
            elif result == "win_opponent":
                winner_id = opponent_id
                result_text = f"Победил {opponent_name}!"
                await db.execute("UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?",
                                (total_stake, total_stake - stake, opponent_id))
                await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?",
                                (stake, challenger_id))
            else:
                # Ничья, возвращаем ставки
                await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, challenger_id))
                await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, opponent_id))
                result_text = "Ничья!"
            await db.commit()

            # Обновляем статус дуэли
            await db.execute(
                "UPDATE duels SET status = 'completed', result = ? WHERE duel_id = ?",
                (result, duel_id)
            )
            await db.commit()

            # Отображаем результат
            choice_map = {"rock": "✊ Камень", "scissors": "✌️ Ножницы", "paper": "🖐 Бумага"}
            text = (
                "🎯 <b>Дуэль завершена!</b> 🎯\n\n"
                f"👤 <b>Инициатор:</b> {challenger_name} ({choice_map[challenger_choice]})\n"
                f"👤 <b>Противник:</b> {opponent_name} ({choice_map[choice]})\n"
                f"💰 <b>Ставка:</b> <code>{format_balance(stake)}</code> GG\n"
                f"🏆 <b>Результат:</b> {result_text}"
            )
            try:
                await bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=message_id,
                    parse_mode="HTML",
                    reply_markup=None
                )
            except Exception as e:
                print(f"Ошибка при редактировании сообщения: {e}")
                await call.answer("❌ Ошибка при завершении дуэли.", show_alert=True)
                return
            await state.clear()
            opponent_state_key = StorageKey(bot_id=bot.id, chat_id=chat_id, user_id=opponent_id)
            opponent_state = FSMContext(dp.storage, key=opponent_state_key)
            await opponent_state.clear()
            await call.answer()
            return

    await call.answer("❌ Вы уже сделали выбор или это не ваш ход.", show_alert=True)




#=================================== РЫБАЛКА ===========================

# 🎣 Рыбалка
FISH_PLACES = [
    ("🌊 Морская бухта", "fish_place_sea"),
    ("🏞 Горное озеро", "fish_place_lake"),
    ("🌅 Речка на закате", "fish_place_river"),
    ("🌌 Таинственный пруд", "fish_place_pond"),
]

FISH_RESULTS = [
    ("❌ Сорвалась! x0", 0, 4),        # (название, множитель, вес)
    ("🐟 Карасик x0.5", 0.5, 4),
    ("🐠 Форель x1", 1, 4),
    ("🐡 Сом x2", 2, 1),
    ("🐉 Золотой карп x5", 5, 1),
]


def get_fish_keyboard():
    buttons = [[InlineKeyboardButton(text=text, callback_data=data)] for text, data in FISH_PLACES]
    buttons.append([InlineKeyboardButton(text="❌ Отменить игру", callback_data="fish_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(Command("fish"))
async def cmd_fish(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        text = (
            "🎣 <b>Использование:</b>\n"
            "  • <code>/fish ставка</code>\n"
            "  • <code>рыбалка ставка</code>\n\n"
            "🐟 <b>Виды рыб и множители:</b>\n"
            "  • ❌ Сорвалась — <i>0x</i>\n"
            "  • 🐟 Карасик — <i>0.5x</i>\n"
            "  • 🐠 Форель — <i>1x</i>\n"
            "  • 🐡 Сом — <i>2x</i>\n"
            "  • 🐉 Золотой карп — <i>5x</i>\n"
        )
        await message.reply(text, parse_mode="HTML")
        return

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row:
            await message.reply("Вы не зарегистрированы. Введите /start.")
            return
        coins = row[0]

    bet = parse_bet_input(args[1], coins)
    if bet < 10:
        await message.reply("❗ Минимальная ставка — <b>10</b> монет.", parse_mode="HTML")
        return
    if coins < bet:
        await message.reply("❌ Недостаточно монет для рыбалки.", parse_mode="HTML")
        return

    # Списываем ставку
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.execute("INSERT OR REPLACE INTO coin_game (user_id, bet) VALUES (?, ?)", (user_id, bet))
        await db.commit()

    await message.reply(
        f"🎣 <b>Рыбалка</b>\n"
        f"Ставка: <code>{format_balance(bet)}</code>\n\n"
        f"Выберите место для рыбалки:",
        reply_markup=get_fish_keyboard(),
        parse_mode="HTML"
    )


# альтернатива — "рыбалка ставка"
@dp.message(lambda m: m.text and m.text.lower().startswith("рыбалка"))
async def txt_fish(message: types.Message):
    await cmd_fish(message)


@dp.callback_query(lambda c: c.data.startswith("fish_place_"))
async def fish_place_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT bet FROM coin_game WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row:
            await call.answer("❌ У вас нет активной рыбалки.", show_alert=True)
            return
        bet = row[0]

    await call.message.edit_text(f"<i>🪝 Забрасываете удочку...</i>\n<i>🌊 Ждём улова...</i>", parse_mode="HTML")
    await asyncio.sleep(2)

    fishes, weights = zip(*[((name, mult), w) for name, mult, w in FISH_RESULTS])
    fish, mult = random.choices(fishes, weights=weights, k=1)[0]
    win = int(bet * mult)

    async with aiosqlite.connect(DB_PATH) as db:
        if win > 0:
            await db.execute(
                "UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?",
                (win, win, user_id)
            )
            text = f"<b>🎣 Вы поймали:</b> <code>{fish}</code>\n💰 <b>Выигрыш:</b> <code>{format_balance(win)}</code>"
        else:
            await db.execute(
                "UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?",
                (bet, user_id)
            )
            text = f"<b>🎣 Вы поймали:</b> <code>{fish}</code>\n💸 <b>Увы, ставка проиграна.</b>"
        await db.execute("DELETE FROM coin_game WHERE user_id = ?", (user_id,))
        await db.commit()

    await call.message.edit_text(text, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data == "fish_cancel")
async def fish_cancel_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT bet FROM coin_game WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if row:
            bet = row[0]
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (bet, user_id))
            await db.execute("DELETE FROM coin_game WHERE user_id = ?", (user_id,))
            await db.commit()
            await call.message.edit_text(
                f"❌ Игра отменена.\nВаша ставка <b>{format_balance(bet)}</b> возвращена.",
                parse_mode="HTML"
            )
        else:
            await call.message.edit_text("❌ Игра отменена.", parse_mode="HTML")
    await call.answer()

# ============================== MINER GAME ==============================

# ============================== MINER GAME ==============================
# Configuration and constants for the Miner game
RIGGED_LOSE_CHANCE_BASE = 11  # Base rigged loss chance (~9% for 3 mines)
MINER_MULTIPLIERS = {
    3: [1.00, 1.07, 1.22, 1.4, 1.63, 1.89, 2.25, 2.63, 3.15, 3.82, 4.7, 5.87, 7.47, 9.71, 12.94, 17.79, 25.41, 38.11, 60.91, 106.69, 213.38, 533.45, 2133.8],
    4: [1.00, 1.13, 1.35, 1.63, 1.99, 2.45, 3.06, 3.87, 4.97, 6.49, 8.65, 11.79, 16.5, 23.83, 35.74, 56.16, 93.6, 168.48, 336.96, 786.24, 2358.72, 11793.6],
    5: [1.00, 1.18, 1.49, 1.9, 2.45, 3.21, 4.28, 5.8, 8.03, 11.37, 16.53, 24.79, 38.56, 62.66, 107.41, 196.91, 393.82, 886.09, 2362.9, 8270.15, 49620.9],
    6: [1.00, 1.23, 1.59, 2.07, 2.73, 3.65, 4.96, 6.84, 9.6, 13.83, 20.37, 30.96, 48.85, 80.6, 139.03, 258.77, 553.08, 1327.39, 3716.69, 14866.76, 89199.2],
    7: [1.00, 1.28, 1.69, 2.25, 3.02, 4.13, 5.73, 8.06, 11.53, 16.93, 25.56, 39.61, 63.38, 106.47, 187.39, 360.24, 802.76, 2006.9, 6019.5, 24077.99],
    8: [1.00, 1.33, 1.79, 2.44, 3.33, 4.63, 6.56, 9.45, 13.85, 20.79, 31.98, 50.63, 82.78, 142.17, 260.98, 532.39, 1277.74, 3833.22, 15332.88],
    9: [1.00, 1.38, 1.9, 2.64, 3.66, 5.16, 7.42, 10.92, 16.38, 25.07, 39.36, 63.78, 106.3, 185.83, 351.82, 760.54, 1901.34, 5704.02, 22816.08]
}

active_miner_games = {}  # Store active Miner game states

def get_miner_keyboard(game_id: str, opened: list[int], mines: list[int], exploded=False, last_index=None, finished=False):
    """Generate the Miner game keyboard (5x5 grid)."""
    buttons = []
    mines_set = set(mines)
    for i in range(25):
        if i == last_index and exploded:
            face = "💥"  # Exploded mine
        elif i in opened:
            face = "🌀"  # Opened safe cell
        elif (exploded or finished) and i in mines_set:
            face = "💣"  # Revealed mine
        else:
            face = "❔"  # Unopened cell
        buttons.append(InlineKeyboardButton(text=face, callback_data=f"miner_cell:{game_id}:{i}"))

    # Create 5x5 grid
    kb = [buttons[i:i+5] for i in range(0, 25, 5)]
    if not exploded and not finished:
        if opened:
            kb.append([InlineKeyboardButton(text="💰 Забрать приз", callback_data=f"miner_take:{game_id}")])
        else:
            kb.append([InlineKeyboardButton(text="🚫 Отменить", callback_data=f"miner_cancel:{game_id}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

@dp.message(Command("miner"))
async def cmd_miner(message: types.Message):
    """Start a new Miner game with specified bet and optional mine count."""
    args = message.text.split()
    if len(args) < 2:
        await message.reply(
            "💣 <b>Минёр: Испытай удачу!</b> 💣\n\n"
            "<b>📜 Как играть:</b>\n"
            "  • Используйте: <code>/miner ставка [кол-во мин]</code> или <code>минер ставка [кол-во мин]</code>\n"
            "  • Кол-во мин: <b>3–9</b> (по умолчанию <b>3</b>)\n"
            "  • Минимальная ставка: <code>10 монет</code>\n\n"
            "<b>🎮 Правила:</b>\n"
            "  • Открывайте клетки, избегая 💣 <i>мин</i>.\n"
            "  • Каждая безопасная клетка увеличивает <b>множитель выигрыша</b>.\n"
            "  • Забирайте приз в любой момент или продолжайте рисковать! 🎰\n"
            "  • Больше мин — <i>выше множители, но и риск больше</i>!\n\n"
            ,
            parse_mode="HTML"
        )
        return

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row:
            await message.reply("❌ <b>Ошибка:</b> Вы не зарегистрированы! 😕 Введите <code>/start</code>.", parse_mode="HTML")
            return
        coins = row[0]

    # Validate bet
    bet = parse_bet_input(args[1], coins)
    if bet < 10:
        await message.reply("❗ <b>Минимальная ставка</b> — <code>10 монет</code>! 💸", parse_mode="HTML")
        return
    if coins < bet:
        await message.reply("❌ <b>Недостаточно монет!</b> 😢 Проверьте баланс и попробуйте снова.", parse_mode="HTML")
        return

    # Validate number of mines (default to 3)
    num_mines = 3
    if len(args) >= 3:
        try:
            num_mines = int(args[2])
            if num_mines not in MINER_MULTIPLIERS or num_mines < 3 or num_mines > 9:
                await message.reply("❌ <b>Ошибка:</b> Количество мин должно быть <b>от 3 до 9</b>! ⚠️", parse_mode="HTML")
                return
        except ValueError:
            await message.reply("❌ <b>Ошибка:</b> Укажите <i>число мин</i> от 3 до 9! 🔢", parse_mode="HTML")
            return

    # Fixed rigged loss chance for all mine counts
    rigged_lose_chance = RIGGED_LOSE_CHANCE_BASE

    # Initialize game
    mines = random.sample(range(25), num_mines)  # Random mine positions
    game_id = str(random.randint(100000, 999999))
    game = {
        "user_id": user_id,
        "bet": bet,
        "opened": [],
        "mines": mines,
        "num_mines": num_mines,
        "mult": 1.0,
        "rigged_lose_chance": rigged_lose_chance,
        "exploded": False,
        "finished": False
    }
    active_miner_games[game_id] = game

    # Deduct bet from user
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.commit()

    # Send game start message
    kb = get_miner_keyboard(game_id, [], mines, False, None, False)
    await message.reply(
        f"💣 <b>Минёр: Игра началась!</b> 💣\n\n"
        f"💸 <b>Ставка:</b> <code>{format_balance(bet)}</code>\n"
        f"💥 <b>Мин:</b> <b>{num_mines}</b>\n"
        f"🎯 <i>Открывайте клетки и избегайте мин!</i> 🚀\n"
        f"<b>Текущий множитель:</b> <code>1.00x</code>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(lambda m: m.text and m.text.lower().startswith("минер"))
async def txt_miner(message: types.Message):
    """Alternative command to start Miner game."""
    await cmd_miner(message)

@dp.callback_query(lambda c: c.data.startswith("miner_cell"))
async def miner_cell(call: types.CallbackQuery):
    """Handle cell opening in Miner game."""
    _, game_id, idx = call.data.split(":")
    idx = int(idx)

    # Validate game state
    game = active_miner_games.get(game_id)
    if not game or game["user_id"] != call.from_user.id:
        await call.answer("❌ Игра не найдена! 😕", show_alert=True)
        return
    if game["exploded"] or game["finished"]:
        await call.answer("❌ Игра завершена! 🎮", show_alert=True)
        return
    if idx in game["opened"]:
        await call.answer("🌀 Эта клетка уже открыта! 🔄")
        return

    bet = game["bet"]
    num_mines = game.get("num_mines", 3)  # Fallback to 3 if num_mines is missing

    # Validate number of mines
    if num_mines not in MINER_MULTIPLIERS:
        await call.answer("❌ Ошибка: Неверное количество мин! ⚠️", show_alert=True)
        game["exploded"] = True
        game["finished"] = True
        kb = get_miner_keyboard(game_id, game["opened"], game["mines"], True, idx, False)
        await call.message.edit_text(
            f"❌ <b>Критическая ошибка!</b> 😱\n"
            f"<i>Неверное количество мин: {num_mines}.</i> Игра завершена. 🚫",
            reply_markup=kb,
            parse_mode="HTML"
        )
        del active_miner_games[game_id]
        return

    # Check for real mine
    if idx in game["mines"]:
        game["exploded"] = True
        kb = get_miner_keyboard(game_id, game["opened"], game["mines"], True, idx, False)
        await call.message.edit_text(
            f"💥 <b>БАМ! Вы попали на мину!</b> 😢\n"
            f"💸 <b>Ставка:</b> <code>{format_balance(bet)}</code>\n"
            f"<i>Игра окончена. Попробуйте снова!</i> 🔄",
            reply_markup=kb,
            parse_mode="HTML"
        )
        del active_miner_games[game_id]
        await call.answer()
        return

    # Check for rigged loss
    if random.randint(1, game["rigged_lose_chance"]) == 1:
        game["exploded"] = True
        kb = get_miner_keyboard(game_id, game["opened"], game["mines"], True, idx, False)
        await call.message.edit_text(
            f"💥 <b>Ох, не повезло!</b> 😈 Клетка оказалась миной!\n"
            f"💸 <b>Ставка:</b> <code>{format_balance(bet)}</code>\n"
            f"<i>Игра завершена. Удача отвернулась!</i> 😣",
            reply_markup=kb,
            parse_mode="HTML"
        )
        del active_miner_games[game_id]
        await call.answer()
        return

    # Safe cell opened
    game["opened"].append(idx)
    game["mult"] = MINER_MULTIPLIERS[num_mines][min(len(game["opened"]), len(MINER_MULTIPLIERS[num_mines])-1)]
    possible = int(bet * game["mult"])

    # Update game message
    kb = get_miner_keyboard(game_id, game["opened"], game["mines"], False, None, False)
    await call.message.edit_text(
        f"🟢 <b>Минёр: Успех!</b> 🎉\n\n"
        f"🌀 <b>Открыто клеток:</b> <b>{len(game['opened'])}</b>\n"
        f"💥 <b>Мин:</b> <b>{num_mines}</b>\n"
        f"📈 <b>Множитель:</b> <code>{game['mult']:.2f}x</code>\n"
        f"💰 <b>Возможный выигрыш:</b> <code>{format_balance(possible)}</code>\n"
        f"<i>Продолжайте или забирайте приз!</i> 🚀",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("miner_take"))
async def miner_take(call: types.CallbackQuery):
    """Handle taking the prize in Miner game."""
    _, game_id = call.data.split(":")
    game = active_miner_games.get(game_id)
    if not game or game["user_id"] != call.from_user.id:
        await call.answer("❌ Игра не найдена! 😕", show_alert=True)
        return
    if game["exploded"] or game["finished"]:
        await call.answer("❌ Игра завершена! 🎮", show_alert=True)
        return

    bet = game["bet"]
    win = int(bet * game["mult"])

    # Award winnings
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?",
                        (win, win, call.from_user.id))
        await db.commit()

    # Update game message
    kb = get_miner_keyboard(game_id, game["opened"], game["mines"], False, None, True)
    await call.message.edit_text(
        f"🎉 <b>Победа! Вы забрали приз!</b> 🏆\n"
        f"💰 <b>Выигрыш:</b> <code>{format_balance(win)}</code>\n"
        f"<i>Отличная игра! Попробуйте снова! 😎</i>",
        reply_markup=kb,
        parse_mode="HTML"
    )
    game["finished"] = True
    del active_miner_games[game_id]
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("miner_cancel"))
async def miner_cancel(call: types.CallbackQuery):
    """Cancel the Miner game and refund the bet."""
    _, game_id = call.data.split(":")
    game = active_miner_games.get(game_id)
    if not game or game["user_id"] != call.from_user.id:
        await call.answer("❌ Игра не найдена! 😕", show_alert=True)
        return
    if game["exploded"] or game["finished"]:
        await call.answer("❌ Игра завершена! 🎮", show_alert=True)
        return

    bet = game["bet"]

    # Refund bet
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (bet, call.from_user.id))
        await db.commit()

    # Update game message
    kb = get_miner_keyboard(game_id, game["opened"], game["mines"], False, None, True)
    await call.message.edit_text(
        f"🚫 <b>Игра отменена!</b> 😔\n"
        f"💸 <b>Ставка возвращена:</b> <code>{format_balance(bet)}</code>\n"
        f"<i>Попробуйте снова с новыми силами!</i> 💪",
        reply_markup=kb,
        parse_mode="HTML"
    )
    game["finished"] = True
    del active_miner_games[game_id]
    await call.answer()

# =================================== БАШНЯ ===========================



# Предполагается, что DB_PATH и format_balance определены где-то в коде


# Множители для игры "Башня"
TOWER_MULTIPLIERS = {
    1: [1.1, 1.3, 1.6, 2.0, 2.4, 2.9, 3.5, 5.0, 7.1],
    2: [1.4, 1.8, 2.4, 3.2, 4.5, 6.0, 8.0, 11.0, 15.0],
    3: [2.0, 3.0, 4.5, 6.5, 9.5, 14.0, 20.0, 30.0, 45.0],
    4: [3.0, 5.0, 8.0, 13.0, 20.0, 30.0, 45.0, 70.0, 100.0]
}

# In-memory game state and cooldowns
active_tower_games = {}  # game_id -> state
tower_cooldowns = {}     # user_id -> timestamp



def build_tower_keyboard(game_id: str, state: dict) -> InlineKeyboardMarkup:
    """Generate the Tower game keyboard (1x5 for current level, past levels below)."""
    level = state["level"]
    bombs = state["bombs"]
    selected = state["selected"]
    kb = []
    # Current row buttons (active level)
    buttons = [
        InlineKeyboardButton(text="❔", callback_data=f"tower_choose:{game_id}:{i}")
        for i in range(5)
    ]
    kb.append(buttons)
    # Completed rows (bottom-up)
    for i in range(level - 1, -1, -1):
        row = bombs[i]
        choice = selected[i]
        row_btns = []
        for j in range(5):
            emoji = "🌀" if j == choice else "❔"
            row_btns.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        kb.append(row_btns)
    # Control buttons
    control_buttons = []
    if level == 0:
        control_buttons.append(InlineKeyboardButton(text="🚫 Отменить", callback_data=f"tower_cancel:{game_id}"))
    else:
        control_buttons.append(InlineKeyboardButton(text="💰 Забрать приз", callback_data=f"tower_collect:{game_id}"))
    kb.append(control_buttons)
    return InlineKeyboardMarkup(inline_keyboard=kb)

def build_final_tower_keyboard(game_id: str, state: dict) -> InlineKeyboardMarkup:
    """Generate the final Tower game keyboard showing limited bombs for levels 1, 4, 6, 9."""
    bombs = state["bombs"]
    selected = state["selected"]
    lost = state["lost"]
    bombs_count = state["bombs_count"]
    last = len(selected) - 1 if lost else min(len(selected) - 1, 8)
    special_levels = [0, 3, 5, 8]  # Levels 1, 4, 6, 9 (0-based indexing)
    kb = []
    for i in range(last, -1, -1):
        row = bombs[i]
        choice = selected[i]
        row_btns = []
        bomb_indices = [j for j, bomb in enumerate(row) if bomb == 1]
        if i in special_levels and bombs_count in (1, 2):
            bombs_to_show = bombs_count
            if lost and choice in bomb_indices:
                # Для проигрыша: включить choice как 💥, и добавить (bombs_to_show - 1) других бомб
                shown_bombs = [choice] + rnd.sample([j for j in bomb_indices if j != choice], bombs_to_show - 1) if len(bomb_indices) > bombs_to_show else bomb_indices
            else:
                # Для выигрыша: выбрать bombs_to_show случайно из bomb_indices
                shown_bombs = rnd.sample(bomb_indices, min(bombs_to_show, len(bomb_indices)))
            for j in range(5):
                if lost and j == choice and row[j] == 1:
                    emoji = "💥"  # Exploded bomb
                elif j in shown_bombs:
                    emoji = "💣"  # Shown bomb
                elif j == choice:
                    emoji = "🌀"  # Safe selection
                else:
                    emoji = "❔"  # Unopened cell
                row_btns.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        else:
            # Normal display for other levels
            for j in range(5):
                if row[j] == 1 and j == choice:
                    emoji = "💥"  # Exploded bomb
                elif row[j] == 1:
                    emoji = "💣"  # Unselected bomb
                elif j == choice:
                    emoji = "🌀"  # Safe selection
                else:
                    emoji = "❔"  # Unopened cell
                row_btns.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        kb.append(row_btns)
    return InlineKeyboardMarkup(inline_keyboard=kb)

@dp.message(lambda m: m.text and m.text.lower().startswith("башня"))
async def txt_tower(message: types.Message):
    """Alternative command to start Tower game."""
    await cmd_tower(message)

@dp.message(Command("tower"))
async def cmd_tower(message: types.Message):
    """Start a new Tower game with specified bet and optional bomb count."""
    args = message.text.split()
    if len(args) < 2:
        text = (
            "🗼 <b>Башня: Испытай удачу!</b> 🗼\n\n"
            "<b>📜 Как играть:</b>\n"
            "  • Используйте: <code>/tower ставка [бомбы]</code> или <code>башня ставка [бомбы]</code>\n"
            "  • Минимальная ставка: <code>10 GG</code>\n"
            "  • Бомбы: от 1 до 4 (по умолчанию 1)\n\n"
            "<b>🎮 Правила:</b>\n"
            "  • Пройдите 9 уровней, выбирая одну из 5 клеток на каждом.\n"
            "  • На каждом уровне есть от 1 до 4 бомб 💣 — избегайте их!\n"
            "  • Больше бомб — выше множитель (1.1x–100.0x).\n"
            "  • Забирайте приз в любой момент или продолжайте рисковать! 🎰\n"
            "  • Можно отменить игру на первом уровне.\n\n"
            "<b>💡 Пример:</b>\n"
            "  • <code>/tower 100</code> (1 бомба)\n"
            "  • <code>башня 1к 3</code> (3 бомбы)"
        )
        await message.reply(text, parse_mode="HTML")
        return

    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row:
            await message.reply("❌ <b>Ошибка:</b> Вы не зарегистрированы! 😕 Введите <code>/start</code>.", parse_mode="HTML")
            return
        coins = row[0]

    # Validate bet
    bet = parse_bet_input(args[1], coins)
    if bet < 10:
        await message.reply("❗ <b>Минимальная ставка</b> — <code>10 GG</code>! 💸", parse_mode="HTML")
        return
    if coins < bet:
        await message.reply("❌ <b>Недостаточно GG!</b> 😢 Проверьте баланс и попробуйте снова.", parse_mode="HTML")
        return

    # Parse bomb count (default to 1)
    bombs_count = 1
    if len(args) >= 3:
        try:
            bombs_count = int(args[2])
            if bombs_count < 1 or bombs_count > 4:
                await message.reply("❗ <b>Количество бомб должно быть от 1 до 4!</b> 💣", parse_mode="HTML")
                return
        except ValueError:
            await message.reply("❗ <b>Укажите число бомб (1–4)!</b> 💣", parse_mode="HTML")
            return

    # Check cooldown
    now = datetime.now(timezone.utc).timestamp()
    if user_id in tower_cooldowns and now - tower_cooldowns[user_id] < 5:
        await message.reply("⏳ <b>Подождите пару секунд перед новой игрой!</b>", parse_mode="HTML")
        return
    tower_cooldowns[user_id] = now

    # Create game
    game_id = uuid.uuid4().hex
    bombs = [[0] * 5 for _ in range(9)]
    special_levels = [0, 3, 5, 8]  # Levels 1, 4, 6, 9 (0-based indexing)
    for i, row in enumerate(bombs):
        if i in special_levels and bombs_count == 1:
            bomb_positions = rnd.sample(range(5), 2)  # 2 bombs for bombs_count=1
        elif i in special_levels and bombs_count == 2:
            bomb_positions = rnd.sample(range(5), 3)  # 3 bombs for bombs_count=2
        else:
            bomb_positions = rnd.sample(range(5), bombs_count)  # Normal bomb count
        for pos in bomb_positions:
            row[pos] = 1
    state = {
        "user_id": user_id,
        "bet": bet,
        "level": 0,
        "bombs": bombs,
        "selected": [],
        "lost": False,
        "game_id": game_id,
        "bombs_count": bombs_count
    }
    active_tower_games[game_id] = state

    # Deduct bet
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.commit()

    # Send game start message
    kb = build_tower_keyboard(game_id, state)
    await message.reply(
        f"🗼 <b>Башня: Игра началась!</b> 🗼\n\n"
        f"💸 <b>Ставка:</b> <code>{format_balance(bet)}</code> GG\n"
        f"💣 <b>Бомб на уровень:</b> {bombs_count}\n"
        f"🏆 <b>Уровень:</b> 1/9\n"
        f"📈 <b>Множитель:</b> <code>{TOWER_MULTIPLIERS[bombs_count][0]:.1f}x</code>\n"
        f"🎯 <i>Выберите клетку, избегая бомб!</i> 🚀",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data.startswith("tower_choose:"))
async def tower_choose(call: types.CallbackQuery):
    """Handle cell selection in Tower game."""
    _, game_id, idx = call.data.split(":")
    idx = int(idx)
    state = active_tower_games.get(game_id)
    if not state:
        await call.answer("❌ Игра не найдена или уже завершена! 😕", show_alert=True)
        return
    if state["user_id"] != call.from_user.id:
        await call.answer("❌ Это не ваша игра! 🚫", show_alert=True)
        return
    if state["lost"] or state["level"] >= 9:
        await call.answer("❌ Игра завершена! 🎮", show_alert=True)
        return
    if idx < 0 or idx > 4:
        await call.answer("❌ Неверный выбор клетки! ⚠️", show_alert=True)
        return

    level = state["level"]
    state["selected"].append(idx)

    # Check for bomb
    if state["bombs"][level][idx] == 1:
        state["lost"] = True
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?",
                            (state["bet"], state["user_id"]))
            await db.commit()
        kb = build_final_tower_keyboard(game_id, state)
        await call.message.edit_text(
            f"💥 <b>БАМ! Вы попали на бомбу!</b> 😢\n"
            f"💸 <b>Ставка:</b> <code>{format_balance(state['bet'])}</code> GG\n"
            f"💣 <b>Бомб на уровень:</b> {state['bombs_count']}\n"
            f"<i>Игра окончена. Попробуйте снова!</i> 🔄",
            reply_markup=kb,
            parse_mode="HTML"
        )
        active_tower_games.pop(game_id, None)
        await call.answer()
        return

    # Move to next level
    state["level"] += 1
    if state["level"] >= 9:
        # Player completed the tower
        win = int(state["bet"] * TOWER_MULTIPLIERS[state["bombs_count"]][8])
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?",
                            (win, win, state["user_id"]))
            await db.commit()
        kb = build_final_tower_keyboard(game_id, state)
        await call.message.edit_text(
            f"🎉 <b>Поздравляем! Вы покорили Башню!</b> 🏆\n"
            f"💰 <b>Выигрыш:</b> <code>{format_balance(win)}</code> GG\n"
            f"💣 <b>Бомб на уровень:</b> {state['bombs_count']}\n"
            f"<i>Отличная игра! Попробуйте снова! 😎</i>",
            reply_markup=kb,
            parse_mode="HTML"
        )
        active_tower_games.pop(game_id, None)
        await call.answer()
        return

    # Update game message
    mult = TOWER_MULTIPLIERS[state["bombs_count"]][state["level"]]
    possible_win = int(state["bet"] * mult)
    kb = build_tower_keyboard(game_id, state)
    await call.message.edit_text(
        f"🟢 <b>Башня: Успех!</b> 🎉\n\n"
        f"🏆 <b>Уровень:</b> {state['level'] + 1}/9\n"
        f"💸 <b>Ставка:</b> <code>{format_balance(state['bet'])}</code> GG\n"
        f"💣 <b>Бомб на уровень:</b> {state['bombs_count']}\n"
        f"📈 <b>Множитель:</b> <code>{mult:.1f}x</code>\n"
        f"💰 <b>Возможный выигрыш:</b> <code>{format_balance(possible_win)}</code> GG\n"
        f"<i>Продолжайте или забирайте приз!</i> 🚀",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("tower_cancel:"))
async def tower_cancel(call: types.CallbackQuery):
    """Cancel the Tower game and refund the bet."""
    _, game_id = call.data.split(":")
    state = active_tower_games.get(game_id)
    if not state:
        await call.answer("❌ Игра не найдена! 😕", show_alert=True)
        return
    if state["user_id"] != call.from_user.id:
        await call.answer("❌ Это не ваша игра! 🚫", show_alert=True)
        return
    if state["level"] != 0:
        await call.answer("❌ Нельзя отменить после первого уровня! 🚫", show_alert=True)
        return

    bet = state["bet"]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (bet, state["user_id"]))
        await db.commit()

    await call.message.edit_text(
        f"🚫 <b>Игра в Башню отменена!</b> 😔\n"
        f"💸 <b>Ставка возвращена:</b> <code>{format_balance(bet)}</code> GG\n"
        f"💣 <b>Бомб на уровень:</b> {state['bombs_count']}\n"
        f"<i>Попробуйте снова с новыми силами!</i> 💪",
        parse_mode="HTML"
    )
    active_tower_games.pop(game_id, None)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("tower_collect:"))
async def tower_collect(call: types.CallbackQuery):
    """Collect winnings and end the Tower game."""
    _, game_id = call.data.split(":")
    state = active_tower_games.get(game_id)
    if not state:
        await call.answer("❌ Игра не найдена! 😕", show_alert=True)
        return
    if state["user_id"] != call.from_user.id:
        await call.answer("❌ Это не ваша игра! 🚫", show_alert=True)
        return
    if state["level"] == 0:
        await call.answer("❌ Слишком рано забирать приз! 🚫", show_alert=True)
        return

    win = int(state["bet"] * TOWER_MULTIPLIERS[state["bombs_count"]][state["level"] - 1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins + ?, win_amount = win_amount + ? WHERE user_id = ?",
                        (win, win, state["user_id"]))
        await db.commit()

    kb = build_final_tower_keyboard(game_id, state)
    await call.message.edit_text(
        f"🎉 <b>Победа! Вы забрали приз!</b> 🏆\n"
        f"💰 <b>Выигрыш:</b> <code>{format_balance(win)}</code> GG\n"
        f"💣 <b>Бомб на уровень:</b> {state['bombs_count']}\n"
        f"<i>Отличная игра! Попробуйте снова! 😎</i>",
        reply_markup=kb,
        parse_mode="HTML"
    )
    active_tower_games.pop(game_id, None)
    await call.answer()


# =================================== ФЕРМА ===========================

ENERGY_COST = 30_000  # 30к GG за 50к энергии
ENERGY_PER_PACK = 50_000
FEZ_PER_CYCLE = 2  # 2 Fezcoin за 5 мин
CYCLE_TIME = 5 * 60  # 5 минут в секундах
LEVEL_UP_HOURS = 72  # 3 дня для повышения уровня
MAX_LEVEL = 3
DAILY_FEZ = [240, 480, 720, 960, 1200, 1440, 1800, 2160, 2640, 3600]  # Fezcoin/сутки на lv3

# Фермы: id, название, стоимость (GG), базовый объём энергии (lv1)
FARMS = [
    (1, "Nano Rig", 50_000_000, 2_000_000),
    (2, "Quantum Node", 100_000_000, 4_000_000),
    (3, "Plasma Core", 250_000_000, 6_000_000),
    (4, "Nebula Array", 500_000_000, 8_000_000),
    (5, "Stellar Forge", 1_000_000_000, 10_000_000),
    (6, "Cosmic Harvester", 2_000_000_000, 12_000_000),
    (7, "Void Extractor", 5_000_000_000, 15_000_000),
    (8, "Galactic Miner", 10_000_000_000, 18_000_000),
    (9, "Eternal Engine", 20_000_000_000, 22_000_000),
    (10, "Universe Devourer", 50_000_000_000, 30_000_000)
]

# Состояния для FSM
class FarmStates(StatesGroup):
    select_farm = State()
    buy_energy = State()

# Функция форматирования чисел


# Обновление состояния фермы
async def update_farm_state(user_id):
    async with aiosqlite.connect(FARM_DB_PATH) as db:
        cursor = await db.execute(
            "SELECT farm_type, level, current_energy, max_energy, last_farm_time, total_farmed_time, pending_fezcoin, purchase_time FROM farms WHERE user_id = ?",
            (user_id,))
        farm = await cursor.fetchone()
        if not farm:
            return None

        farm_type, level, current_energy, max_energy, last_farm_time, total_farmed_time, pending_fezcoin, purchase_time = farm
        base_energy = FARMS[farm_type - 1][3]

        # Рассчёт циклов
        now = int(time.time())
        delta_time = now - last_farm_time
        max_cycles = delta_time // CYCLE_TIME
        cycles = min(max_cycles, current_energy // ENERGY_PER_PACK)

        # Обновление энергии, Fezcoin, времени
        current_energy -= cycles * ENERGY_PER_PACK
        pending_fezcoin += cycles * FEZ_PER_CYCLE
        total_farmed_time += (cycles * CYCLE_TIME) / 3600  # в часах

        # Проверка повышения уровня
        if total_farmed_time >= LEVEL_UP_HOURS and level < MAX_LEVEL:
            level += 1
            max_energy = base_energy * level
            total_farmed_time = 0

        await db.execute(
            "UPDATE farms SET current_energy = ?, max_energy = ?, last_farm_time = ?, total_farmed_time = ?, pending_fezcoin = ? WHERE user_id = ?",
            (current_energy, max_energy, now, total_farmed_time, pending_fezcoin, user_id))
        await db.commit()
        return farm_type, level, current_energy, max_energy, total_farmed_time, pending_fezcoin, purchase_time

# Команда /farm
async def cmd_farm(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("⚠️ <b>Ферма доступна только в привате!</b>", parse_mode="HTML")
        return

    user_id = message.from_user.id
    farm_data = await update_farm_state(user_id)

    if farm_data is None:
        await show_farm_selection(message, state, 0)
        return

    farm_type, level, current_energy, max_energy, total_farmed_time, pending_fezcoin, _ = farm_data
    farm_name = FARMS[farm_type - 1][1]
    daily_fez = DAILY_FEZ[farm_type - 1]

    level_text = f"<code>{level}</code>" if level < MAX_LEVEL else f"<code>{level}</code> (<i>максимум</i>)"
    progress_text = f"<code>{total_farmed_time:.1f}/3 дней</code> (<i>до lv{level + 1}</i>)" if level < MAX_LEVEL else "<code>максимальный уровень</code>"

    text = f"""🌌 <b>Твоя ферма: {farm_name}</b> 🌌

⚡ <b>Уровень:</b> {level_text}
⚡ <b>Энергия:</b> <code>{format_balance(current_energy)} / {format_balance(max_energy)}</code>
💰 <b>Fezcoin к сбору:</b> <code>{pending_fezcoin}</code>
⏳ <b>Профармлено:</b> {progress_text}

<blockquote>🚀 <b>Фармит: 2 Fez каждые 5 мин</b> (50к энергии). На lv3 при полной энергии: до <code>{daily_fez} Fezcoin/сутки</code>. <i>Поддерживай энергию, чтобы быстрее достичь lv{level + 1 if level < MAX_LEVEL else level}!</i></blockquote>"""

    if current_energy == 0:
        text = f"""🌌 <b>Твоя ферма: {farm_name}</b> 🌌

⚡ <b>Уровень:</b> {level_text}
⚡ <b>Энергия:</b> <code>0 / {format_balance(max_energy)}</code>
💰 <b>Fezcoin к сбору:</b> <code>{pending_fezcoin}</code>
⏳ <b>Профармлено:</b> {progress_text}

<blockquote>⚠️ <b>Ферма не фармит!</b> <i>Купи энергию: 50к = 30к GG, даст 2 Fez каждые 5 мин. Поддерживай энергию >0 для роста до lv{level + 1 if level < MAX_LEVEL else level}!</i></blockquote>"""

    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
             InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
            [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
             InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
            [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
        ]))
    except:
        await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
             InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
            [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
             InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
            [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
        ]))

# Текстовый обработчик для "ферма"
async def txt_farm(message: Message):
    """Альтернативная команда для запуска фермы."""
    await cmd_farm(message, None)

# Листание ферм для покупки
async def show_farm_selection(message: Message, state: FSMContext, farm_index: int, edit=True):
    farm = FARMS[farm_index]
    farm_name, cost, base_energy = farm[1], farm[2], farm[3]
    daily_fez = DAILY_FEZ[farm_index]

    text = f"""🌌 <b>Fezcoin Ферма</b> 🌌

🚀 <b>Выбери ферму</b> для фарма <i>Fezcoin</i>! <b>Только 1 ферма на аккаунт.</b>
<blockquote>💡 <b>{farm_name}</b> 💡
<b>Цена:</b> <code>{format_balance(cost)} GG</code>
<b>Энергия на lv1:</b> <code>{format_balance(base_energy)}</code> (до <code>{format_balance(base_energy * MAX_LEVEL)}</code> на lv3)
<i>Доход: до {daily_fez} Fezcoin/сутки на lv3!</i></blockquote>
🌟 <b>Листай для выбора!</b>"""

    keyboard = [[
        InlineKeyboardButton(text="[<]", callback_data=f"select_farm_{(farm_index - 1) % len(FARMS)}"),
        InlineKeyboardButton(text="[Купить]", callback_data=f"buy_farm_{farm_index}"),
        InlineKeyboardButton(text="[>]", callback_data=f"select_farm_{(farm_index + 1) % len(FARMS)}")
    ]]

    if edit:
        try:
            await message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
        except:
            await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    else:
        await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

    await state.set_state(FarmStates.select_farm)
    await state.update_data(farm_index=farm_index)

# Листание ферм для просмотра (Другие фермы)
async def handle_other_farms(callback: CallbackQuery, state: FSMContext):
    farm_index = int(callback.data.split("_")[2])
    farm = FARMS[farm_index]
    farm_name, cost, base_energy = farm[1], farm[2], farm[3]
    daily_fez = DAILY_FEZ[farm_index]

    text = f"""🌌 <b>Просмотр ферм</b> 🌌

<blockquote>💡 <b>{farm_name}</b> 💡
<b>Цена:</b> <code>{format_balance(cost)} GG</code>
<b>Энергия на lv1:</b> <code>{format_balance(base_energy)}</code> (до <code>{format_balance(base_energy * MAX_LEVEL)}</code> на lv3)
<i>Доход: до {daily_fez} Fezcoin/сутки на lv3!</i></blockquote>
🌟 <b>Листай для просмотра!</b>"""

    keyboard = [
        [InlineKeyboardButton(text="[<]", callback_data=f"other_farms_{(farm_index - 1) % len(FARMS)}"),
         InlineKeyboardButton(text="[>]", callback_data=f"other_farms_{(farm_index + 1) % len(FARMS)}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
    ]

    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    except:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

    await state.set_state(FarmStates.select_farm)
    await state.update_data(farm_index=farm_index)
    await callback.answer()

# Обработка callback'ов для листания ферм в меню покупки
async def handle_select_farm(callback: CallbackQuery, state: FSMContext):
    farm_index = int(callback.data.split("_")[2])
    await show_farm_selection(callback.message, state, farm_index)
    await callback.answer()

# Обработка покупки фермы
async def handle_buy_farm(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    farm_index = int(callback.data.split("_")[2])
    farm = FARMS[farm_index]
    farm_name, cost, base_energy = farm[1], farm[2], farm[3]
    daily_fez = DAILY_FEZ[farm_index]

    async with aiosqlite.connect(FARM_DB_PATH) as db_farm:
        cursor = await db_farm.execute("SELECT farm_type FROM farms WHERE user_id = ?", (user_id,))
        current_farm = await cursor.fetchone()
        if current_farm:
            await callback.answer("❌ <b>У вас уже есть ферма!</b>", show_alert=True)
            return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        user = await cursor.fetchone()
        if not user or user[0] < cost:
            await callback.message.edit_text(
                f"""❌ <b>Недостаточно GG!</b> Нужно: <code>{format_balance(cost)} GG</code>, у тебя: <code>{format_balance(user[0] if user else 0)} GG</code>.
<blockquote><i>Сыграй в /games или используй /bonus для GG!</i></blockquote>""",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
                ])
            )
            await callback.answer()
            return

    text = f"""🌌 <b>Подтвердите покупку фермы</b> 🌌

<blockquote>💡 <b>{farm_name}</b> 💡
<b>Цена:</b> <code>{format_balance(cost)} GG</code>
<b>Энергия на lv1:</b> <code>{format_balance(base_energy)}</code> (до <code>{format_balance(base_energy * MAX_LEVEL)}</code> на lv3)
<i>Доход: до {daily_fez} Fezcoin/сутки на lv3!</i></blockquote>
<b>После покупки купите энергию для запуска фарма.</b>"""

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_buy_{farm_index}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
        ])
    )
    await callback.answer()

# Подтверждение покупки фермы
async def handle_confirm_buy(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    farm_index = int(callback.data.split("_")[2])
    farm = FARMS[farm_index]
    farm_name, cost = farm[1], farm[2]

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        user = await cursor.fetchone()
        if not user or user[0] < cost:
            await callback.message.edit_text(
                f"""❌ <b>Недостаточно GG!</b> Нужно: <code>{format_balance(cost)} GG</code>, у тебя: <code>{format_balance(user[0] if user else 0)} GG</code>.
<blockquote><i>Сыграй в /games или используй /bonus для GG!</i></blockquote>""",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
                ])
            )
            await callback.answer()
            return

        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (cost, user_id))
        await db.commit()

    async with aiosqlite.connect(FARM_DB_PATH) as db_farm:
        await db_farm.execute(
            "INSERT OR REPLACE INTO farms (user_id, farm_type, level, current_energy, max_energy, last_farm_time, total_farmed_time, pending_fezcoin, purchase_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, farm_index + 1, 1, 0, farm[3], int(time.time()), 0, 0, int(time.time()))
        )
        await db_farm.commit()

    text = f"""🎉 <b>Ферма {farm_name} куплена!</b>
💰 <b>Списано:</b> <code>{format_balance(cost)} GG</code>.
<blockquote><i>Купи энергию (50к = 30к GG) для фарма до {DAILY_FEZ[farm_index]} Fezcoin/сутки на lv3!</i></blockquote>"""

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
             InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
            [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
             InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
            [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
        ])
    )
    await state.clear()
    await callback.answer()

# Покупка энергии
async def handle_buy_energy(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    farm_data = await update_farm_state(user_id)
    if not farm_data:
        await callback.message.edit_text(
            "❌ <b>У тебя нет фермы!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await callback.answer()
        return

    _, _, current_energy, max_energy, _, _, _ = farm_data
    max_packs = (max_energy - current_energy) // ENERGY_PER_PACK

    text = f"""💡 <b>Купить энергию</b> 💡

⚡ <b>50к энергии =</b> <code>30к GG</code>
⚡ <b>Текущая:</b> <code>{format_balance(current_energy)} / {format_balance(max_energy)}</code>
<b>Введи количество пакетов (1-{max_packs}, max до <code>{format_balance(max_energy)}</code>).</b>
<blockquote><i>Полный запас ({format_balance(max_energy)}) = {max_packs} пакетов = <code>{format_balance(max_packs * ENERGY_COST)} GG</code>.</i></blockquote>"""

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
        ])
    )
    await state.set_state(FarmStates.buy_energy)
    await callback.answer()

# Обработка ввода количества пакетов энергии
async def process_buy_energy(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        packs = int(message.text)
    except ValueError:
        await message.answer(
            "❌ <b>Введи число!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await state.clear()
        return

    farm_data = await update_farm_state(user_id)
    if not farm_data:
        await message.answer(
            "❌ <b>У тебя нет фермы!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await state.clear()
        return

    _, _, current_energy, max_energy, _, _, _ = farm_data
    max_packs = (max_energy - current_energy) // ENERGY_PER_PACK

    if packs < 1 or packs > max_packs:
        await message.answer(
            f"❌ <b>Недопустимое количество!</b> <b>Введи от 1 до {max_packs}.</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await state.clear()
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        coins = (await cursor.fetchone())[0]
        total_cost = packs * ENERGY_COST

        if coins < total_cost:
            await message.answer(
                f"""❌ <b>Недостаточно GG!</b> Нужно: <code>{format_balance(total_cost)} GG</code>, у тебя: <code>{format_balance(coins)} GG</code>.
<blockquote><i>Сыграй в /games или используй /bonus для GG!</i></blockquote>""",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
                ])
            )
            await state.clear()
            return

        async with aiosqlite.connect(FARM_DB_PATH) as db_farm:
            await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (total_cost, user_id))
            await db_farm.execute("UPDATE farms SET current_energy = current_energy + ? WHERE user_id = ?",
                                  (packs * ENERGY_PER_PACK, user_id))
            await db.commit()
            await db_farm.commit()

    await message.answer(
        f"""🎉 <b>Куплено <code>{packs}</code> пакетов</b> (<code>{format_balance(packs * ENERGY_PER_PACK)} энергии</code>)!
⚡ <b>Текущая:</b> <code>{format_balance(current_energy + packs * ENERGY_PER_PACK)} / {format_balance(max_energy)}</code>.
<blockquote><i>Ферма фармит быстрее! Нажми <b>Статус</b> для расчёта дохода.</i></blockquote>""",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
             InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
            [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
             InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
            [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
        ])
    )
    await state.clear()

# Сбор Fezcoin
async def handle_collect_fez(callback: CallbackQuery):
    user_id = callback.from_user.id
    farm_data = await update_farm_state(user_id)
    if not farm_data:
        await callback.message.edit_text(
            "❌ <b>У тебя нет фермы!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await callback.answer()
        return

    farm_type, level, current_energy, max_energy, total_farmed_time, pending_fezcoin, _ = farm_data
    farm_name = FARMS[farm_type - 1][1]

    if pending_fezcoin == 0:
        text = f"""❌ <b>Нет Fezcoin для сбора!</b> <b>Купи энергию для фарма.</b>
<blockquote><i>Пакет 50к энергии = 30к GG даст <b>2 Fez</b> каждые 5 мин!</i></blockquote>"""
        keyboard = [
            [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
        ]
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            async with aiosqlite.connect(FARM_DB_PATH) as db_farm:
                await db.execute("UPDATE users SET fezcoin = fezcoin + ? WHERE user_id = ?", (pending_fezcoin, user_id))
                await db_farm.execute("UPDATE farms SET pending_fezcoin = 0 WHERE user_id = ?", (user_id,))
                cursor = await db.execute("SELECT fezcoin FROM users WHERE user_id = ?", (user_id,))
                total_fezcoin = (await cursor.fetchone())[0]
                await db.commit()
                await db_farm.commit()

        level_text = f"<code>{level}</code>" if level < MAX_LEVEL else f"<code>{level}</code> (<i>максимум</i>)"
        progress_text = f"<code>{total_farmed_time:.1f}/3 дней</code> (<i>до lv{level + 1}</i>)" if level < MAX_LEVEL else "<code>максимальный уровень</code>"

        text = f"""💰 <b>Собрано: <code>{pending_fezcoin} Fezcoin</code>!</b>
⚡ <b>Энергия:</b> <code>{format_balance(current_energy)} / {format_balance(max_energy)}</code>
⏳ <b>Профармлено:</b> {progress_text}
<blockquote><i>Общий баланс Fezcoin: <code>{total_fezcoin}</code>. Используй /crypto для торговли!</i></blockquote>"""
        keyboard = [
            [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
             InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
            [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
             InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
            [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
        ]

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

# Продажа фермы
async def handle_sell_farm(callback: CallbackQuery):
    user_id = callback.from_user.id
    farm_data = await update_farm_state(user_id)
    if not farm_data:
        await callback.message.edit_text(
            "❌ <b>У тебя нет фермы!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await callback.answer()
        return

    farm_type, _, current_energy, _, _, pending_fezcoin, _ = farm_data
    farm_name, cost = FARMS[farm_type - 1][1], FARMS[farm_type - 1][2]
    refund_gg = (cost // 10) + ((current_energy // ENERGY_PER_PACK) * 20_000)

    text = f"""📉 <b>Продать {farm_name}</b> за <code>{format_balance(refund_gg)} GG</code> (<b>10% стоимости</b>)?
<blockquote><i>Несобранные Fezcoin (<code>{pending_fezcoin}</code>) начислятся. Энергия (<code>{format_balance(current_energy)}</code>) вернётся как <code>{format_balance((current_energy // ENERGY_PER_PACK) * 20_000)} GG</code>. Ферма удалится.</i></blockquote>"""

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Продать", callback_data="confirm_sell")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
        ])
    )
    await callback.answer()

# Подтверждение продажи фермы
async def confirm_sell_farm(callback: CallbackQuery):
    user_id = callback.from_user.id
    farm_data = await update_farm_state(user_id)
    if not farm_data:
        await callback.message.edit_text(
            "❌ <b>У тебя нет фермы!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await callback.answer()
        return

    farm_type, _, current_energy, _, _, pending_fezcoin, _ = farm_data
    farm_name, cost = FARMS[farm_type - 1][1], FARMS[farm_type - 1][2]
    refund_gg = (cost // 10) + ((current_energy // ENERGY_PER_PACK) * 20_000)

    async with aiosqlite.connect(DB_PATH) as db:
        async with aiosqlite.connect(FARM_DB_PATH) as db_farm:
            await db.execute("UPDATE users SET coins = coins + ?, fezcoin = fezcoin + ? WHERE user_id = ?",
                            (refund_gg, pending_fezcoin, user_id))
            await db_farm.execute("DELETE FROM farms WHERE user_id = ?", (user_id,))
            await db.commit()
            await db_farm.commit()

    await callback.message.edit_text(
        f"""🎉 <b>Ферма {farm_name} продана!</b>
💰 <b>Получено:</b> <code>{format_balance(cost // 10)} GG</code> (за ферму) + <code>{format_balance((current_energy // ENERGY_PER_PACK) * 20_000)} GG</code> (за энергию).
💰 <b>Начислено Fezcoin:</b> <code>{pending_fezcoin}</code>.
<blockquote><i>Выбери новую ферму через <b>Другие фермы</b>!</i></blockquote>""",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="back_to_farm")]
        ])
    )
    await callback.answer()

# Статус фермы
async def handle_status(callback: CallbackQuery):
    user_id = callback.from_user.id
    farm_data = await update_farm_state(user_id)
    if not farm_data:
        await callback.message.edit_text(
            "❌ <b>У тебя нет фермы!</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
            ])
        )
        await callback.answer()
        return

    farm_type, level, current_energy, max_energy, total_farmed_time, pending_fezcoin, purchase_time = farm_data
    farm_name = FARMS[farm_type - 1][1]
    daily_fez = DAILY_FEZ[farm_type - 1]
    purchase_date = datetime.fromtimestamp(purchase_time, tz=pytz.UTC).strftime('%Y-%m-%d %H:%M')

    level_text = f"<code>{level}</code>" if level < MAX_LEVEL else f"<code>{level}</code> (<i>максимум</i>)"
    progress_text = f"<code>{total_farmed_time:.1f}/3 дней</code> (<i>до lv{level + 1}</i>)" if level < MAX_LEVEL else "<code>максимальный уровень</code>"

    cycles_left = current_energy // ENERGY_PER_PACK
    time_left_min = cycles_left * (CYCLE_TIME / 60)
    time_left_text = f"<b>~{cycles_left} циклов (~{time_left_min:.1f} мин или ~{time_left_min / 60:.1f} часов)</b>" if current_energy > 0 else "<b>фарминг остановлен</b>"

    text = f"""🌌 <b>Статистика фермы: {farm_name}</b> 🌌

⚡ <b>Уровень:</b> {level_text}
⚡ <b>Энергия:</b> <code>{format_balance(current_energy)} / {format_balance(max_energy)}</code>
💰 <b>Fezcoin к сбору:</b> <code>{pending_fezcoin}</code>
⏳ <b>Профармлено:</b> {progress_text}
🕒 <b>Дата покупки:</b> <code>{purchase_date}</code>
📈 <b>Доход:</b> <code>2 Fez/5 мин</code>, до <code>{daily_fez} Fezcoin/сутки</code> на lv3
⏰ <b>Остаток энергии:</b> <code>{time_left_text}</code>"""

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_farm")]
        ])
    )
    await callback.answer()

# Возврат к главному меню фермы или выбору фермы
async def handle_back_to_farm(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    farm_data = await update_farm_state(user_id)

    if farm_data:
        # Если ферма есть, возвращаемся в меню "Твоя ферма"
        farm_type, level, current_energy, max_energy, total_farmed_time, pending_fezcoin, _ = farm_data
        farm_name = FARMS[farm_type - 1][1]
        daily_fez = DAILY_FEZ[farm_type - 1]

        level_text = f"<code>{level}</code>" if level < MAX_LEVEL else f"<code>{level}</code> (<i>максимум</i>)"
        progress_text = f"<code>{total_farmed_time:.1f}/3 дней</code> (<i>до lv{level + 1}</i>)" if level < MAX_LEVEL else "<code>максимальный уровень</code>"

        text = f"""🌌 <b>Твоя ферма: {farm_name}</b> 🌌

⚡ <b>Уровень:</b> {level_text}
⚡ <b>Энергия:</b> <code>{format_balance(current_energy)} / {format_balance(max_energy)}</code>
💰 <b>Fezcoin к сбору:</b> <code>{pending_fezcoin}</code>
⏳ <b>Профармлено:</b> {progress_text}

<blockquote>🚀 <b>Фармит: 2 Fez каждые 5 мин</b> (50к энергии). На lv3 при полной энергии: до <code>{daily_fez} Fezcoin/сутки</code>. <i>Поддерживай энергию, чтобы быстрее достичь lv{level + 1 if level < MAX_LEVEL else level}!</i></blockquote>"""

        if current_energy == 0:
            text = f"""🌌 <b>Твоя ферма: {farm_name}</b> 🌌

⚡ <b>Уровень:</b> {level_text}
⚡ <b>Энергия:</b> <code>0 / {format_balance(max_energy)}</code>
💰 <b>Fezcoin к сбору:</b> <code>{pending_fezcoin}</code>
⏳ <b>Профармлено:</b> {progress_text}

<blockquote>⚠️ <b>Ферма не фармит!</b> <i>Купи энергию: 50к = 30к GG, даст 2 Fez каждые 5 мин. Поддерживай энергию >0 для роста до lv{level + 1 if level < MAX_LEVEL else level}!</i></blockquote>"""

        try:
            await callback.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
                     InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
                    [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
                     InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
                    [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
                ])
            )
        except:
            await callback.message.answer(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡ Купить энергию", callback_data="buy_energy"),
                     InlineKeyboardButton(text="💰 Собрать Fezcoin", callback_data="collect_fez")],
                    [InlineKeyboardButton(text="📉 Продать ферму", callback_data="sell_farm"),
                     InlineKeyboardButton(text="🔍 Статус", callback_data="status")],
                    [InlineKeyboardButton(text="🌌 Другие фермы", callback_data="other_farms_0")]
                ])
            )
    else:
        # Если фермы нет, возвращаемся в меню выбора фермы
        farm_index = 0
        farm = FARMS[farm_index]
        farm_name, cost, base_energy = farm[1], farm[2], farm[3]
        daily_fez = DAILY_FEZ[farm_index]

        text = f"""🌌 <b>Fezcoin Ферма</b> 🌌

🚀 <b>Выбери ферму</b> для фарма <i>Fezcoin</i>! <b>Только 1 ферма на аккаунт.</b>
<blockquote>💡 <b>{farm_name}</b> 💡
<b>Цена:</b> <code>{format_balance(cost)} GG</code>
<b>Энергия на lv1:</b> <code>{format_balance(base_energy)}</code> (до <code>{format_balance(base_energy * MAX_LEVEL)}</code> на lv3)
<i>Доход: до {daily_fez} Fezcoin/сутки на lv3!</i></blockquote>
🌟 <b>Листай для выбора!</b>"""

        try:
            await callback.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="[<]", callback_data=f"select_farm_{(farm_index - 1) % len(FARMS)}"),
                     InlineKeyboardButton(text="[Купить]", callback_data=f"buy_farm_{farm_index}"),
                     InlineKeyboardButton(text="[>]", callback_data=f"select_farm_{(farm_index + 1) % len(FARMS)}")]
                ])
            )
        except:
            await callback.message.answer(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="[<]", callback_data=f"select_farm_{(farm_index - 1) % len(FARMS)}"),
                     InlineKeyboardButton(text="[Купить]", callback_data=f"buy_farm_{farm_index}"),
                     InlineKeyboardButton(text="[>]", callback_data=f"select_farm_{(farm_index + 1) % len(FARMS)}")]
                ])
            )

        await state.set_state(FarmStates.select_farm)
        await state.update_data(farm_index=farm_index)

    await callback.answer()

# Регистрация обработчиков
dp.message.register(txt_farm, lambda m: m.text and m.text.lower().startswith("ферма"))
dp.message.register(cmd_farm, Command("farm"))
dp.callback_query.register(handle_select_farm, F.data.startswith("select_farm_"))
dp.callback_query.register(handle_other_farms, F.data.startswith("other_farms_"))
dp.callback_query.register(handle_buy_farm, F.data.startswith("buy_farm_"))
dp.callback_query.register(handle_confirm_buy, F.data.startswith("confirm_buy_"))
dp.callback_query.register(handle_buy_energy, F.data == "buy_energy")
dp.message.register(process_buy_energy, FarmStates.buy_energy)
dp.callback_query.register(handle_collect_fez, F.data == "collect_fez")
dp.callback_query.register(handle_sell_farm, F.data == "sell_farm")
dp.callback_query.register(confirm_sell_farm, F.data == "confirm_sell")
dp.callback_query.register(handle_status, F.data == "status")
dp.callback_query.register(handle_back_to_farm, F.data == "back_to_farm")

# =================================== РУЛЕТКА ===========================

active_roulette_players = set()  # Для предотвращения множественных игр

# Вспомогательные функции для рулетки
def spin_roulette():
    return random.randint(0, 36)

def get_color(number):
    if number == 0:
        return "зеленый"
    red_numbers = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
    return "красный" if number in red_numbers else "черный"

def is_even(number):
    return number % 2 == 0 and number != 0

# Команда рулетки
@dp.message(Command("roulette"))
@dp.message(lambda m: m.text and m.text.lower().startswith("рул"))
async def cmd_roulette(message: types.Message):
    user_id = message.from_user.id
    if user_id in active_roulette_players:
        await message.reply("<i>🎰 Вы уже играете в рулетку! Дождитесь окончания.</i>", parse_mode="HTML")
        return

    # Проверка баланса
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_money = result[0]

    # Парсинг ставки и прогноза
    args = message.text.split()
    if len(args) < 3:
        await message.reply(
            "<i>🎰 Используйте: /roulette сумма_ставки ставка</i>\n"
            "Пример: <code>/roulette 1k к</code> или <code>/roulette 1000 14</code>\n"
            "Допустимые ставки: <code>🔴 красное</code>, <code>⚫ черное</code>, <code>🟢 зеленое</code>, <code>четное</code>, <code>нечетное</code>, "
            "<code>1-12</code>, <code>13-24</code>, <code>25-36</code>, <code>больше</code>, <code>меньше</code>, или число <i>0-36</i>",
            parse_mode="HTML"
        )
        return

    bet_amount_str = args[1]
    prediction_raw = args[2].lower()

    bet_amount = parse_bet_input(bet_amount_str, user_money)
    if bet_amount < 10:
        await message.reply("❗ Минимальная ставка — <b>10</b> монет.", parse_mode="HTML")
        return
    if user_money < bet_amount:
        await message.reply("❌ Недостаточно монет для ставки.", parse_mode="HTML")
        return

    # Проверка прогноза
    pred_num = None
    pred_str = None
    if re.match(r"^\d+$", prediction_raw):
        pred_num = int(prediction_raw)
        if pred_num < 0 or pred_num > 36:
            await message.reply(
                "❌ Некорректный номер. Допустимые значения: <code>🔴 красное</code>, <code>⚫ черное</code>, <code>🟢 зеленое</code>, <code>четное</code>, <code>нечетное</code>, "
                "<code>1-12</code>, <code>13-24</code>, <code>25-36</code>, <code>больше</code>, <code>меньше</code>, или число <i>0-36</i>.",
                parse_mode="HTML"
            )
            return
    else:
        pred_str = prediction_raw

    valid_predictions = {
        "красное", "кра", "red", "к",
        "черное", "чер", "black", "ч",
        "четное", "чет", "even", "чёт",
        "нечетное", "нечет", "odd", "нечёт",
        "1-12", "13-24", "25-36",
        "бол", "больше", "big", "б", "19-36",
        "мал", "меньше", "small", "м", "1-18",
        "зеро", "zero", "зеленый", "зеленое", "з"
    }

    if pred_num is None and pred_str not in valid_predictions:
        await message.reply(
            "<i>🎰 Некорректный тип ставки.</i>\n"
            "Допустимые ставки: <code>🔴 красное</code>, <code>⚫ черное</code>, <code>🟢 зеленое</code>, <code>четное</code>, <code>нечетное</code>, "
            "<code>1-12</code>, <code>13-24</code>, <code>25-36</code>, <code>больше</code>, <code>меньше</code>, или число <i>0-36</i>",
            parse_mode="HTML"
        )
        return

    active_roulette_players.add(user_id)  # Добавляем в активные

    try:
        # Списываем ставку
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet_amount, user_id))
            await db.commit()

        # Крутим рулетку
        winning_number = spin_roulette()
        winning_color = get_color(winning_number)
        winning_even = is_even(winning_number)

        # Добавляем эмодзи к цвету
        color_display = (
            f"🔴 {winning_color}" if winning_color == "красный" else
            f"⚫ {winning_color}" if winning_color == "черный" else
            f"🟢 {winning_color}"
        )

        payout = 0.0
        if pred_num is not None:
            if winning_number == pred_num:
                payout = bet_amount * 35
        else:
            if pred_str in ("кра", "красное", "red", "к") and winning_color == "красный":
                payout = bet_amount * 1.9
            elif pred_str in ("чер", "черное", "black", "ч") and winning_color == "черный":
                payout = bet_amount * 1.9
            elif pred_str in ("чет", "четное", "even", "чёт") and winning_even and winning_number != 0:
                payout = bet_amount * 1.9
            elif pred_str in ("нечет", "нечетное", "odd", "нечёт") and (not winning_even) and winning_number != 0:
                payout = bet_amount * 1.9
            elif pred_str == "1-12" and 1 <= winning_number <= 12:
                payout = bet_amount * 2.7
            elif pred_str == "13-24" and 13 <= winning_number <= 24:
                payout = bet_amount * 2.7
            elif pred_str == "25-36" and 25 <= winning_number <= 36:
                payout = bet_amount * 2.7
            elif pred_str in ("бол", "больше", "big", "б", "19-36") and 19 <= winning_number <= 36:
                payout = bet_amount * 1.9
            elif pred_str in ("мал", "меньше", "small", "м", "1-18") and 1 <= winning_number <= 18:
                payout = bet_amount * 1.9
            elif pred_str in ("зеро", "zero", "зеленый", "зеленое", "з") and winning_number == 0:
                payout = bet_amount * 36

        # Обновляем баланс и статистику
        async with aiosqlite.connect(DB_PATH) as db:
            if payout > 0:
                await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (payout, user_id))
                await db.execute("UPDATE users SET win_amount = win_amount + ? WHERE user_id = ?", (payout - bet_amount, user_id))
            else:
                await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?", (bet_amount, user_id))
            await db.commit()

            # Получаем новый баланс
            cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
            new_balance = (await cursor.fetchone())[0]

        # Формируем результат
        result_text = (
            f"<b>🎲 ♣️ ♥️   Рулетка ♦️ ♣️ 🎲</b>\n"
            f"<blockquote>📈 <b>Выпало:</b> <code>{winning_number}</code> ({color_display}, "
            f"{'четное' if winning_even and winning_number != 0 else 'нечетное' if not winning_even and winning_number != 0 else 'зеленый'})</blockquote>\n"
            f"{'<i>🎉 Выигрыш:</i>' if payout > 0 else '<i>😔 Проигрыш:</i>'} <i><b>{format_balance(payout if payout > 0 else bet_amount)}</b></i>\n"
            f"<i>💰 Баланс:</i> <i><b>{format_balance(new_balance)}</b></i>"
        )
        await message.reply(result_text, parse_mode="HTML")

    except Exception as e:
        await message.reply(f"❌ Произошла ошибка: {e}", parse_mode="HTML")
    finally:
        active_roulette_players.discard(user_id)


# =================================== БАНК ===========================

async def cmd_bank(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply(
                "❌ <b>Ошибка</b> ❌\n\n"
                "Вы не зарегистрированы.\n"
                "➡️ Используйте <code>/start</code> для регистрации.",
                parse_mode="HTML"
            )
            return
        user_coins = result[0]

    parts = message.text.split()
    moscow_tz = pytz.timezone('Europe/Moscow')

    if len(parts) == 1:
        # Показать список депозитов с автоматическим начислением процентов
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT deposit_id, amount, created_at, last_interest FROM deposits WHERE user_id = ?",
                (user_id,)
            )
            deposits = await cursor.fetchall()

        if not deposits:
            await message.reply(
                "╔════════════════════╗\n"
                "  <b>🏦 Ваш банк</b>\n"
                "╚════════════════════╝\n\n"
                "❌ У вас нет активных депозитов.\n"
                "💸 Создайте новый депозит:\n"
                "  • <code>/bank 100к</code>\n"
                "  • <code>/bank 1.5кк</code>\n"
                "  • <code>/bank все</code>",
                parse_mode="HTML"
            )
            return

        # Автоматическое начисление процентов
        now = datetime.now(pytz.UTC)
        updated_deposits = []
        for dep in deposits:
            deposit_id, amount, created_at, last_interest = dep
            last_interest_dt = datetime.fromisoformat(last_interest) if last_interest else datetime.fromisoformat(
                created_at)
            weeks_passed = (now - last_interest_dt).days // 7
            if weeks_passed > 0:
                new_amount = int(amount * (1.1 ** weeks_passed))  # 10% ставка
                await db.execute(
                    "UPDATE deposits SET amount = ?, last_interest = ? WHERE user_id = ? AND deposit_id = ?",
                    (new_amount, now.isoformat(), user_id, deposit_id)
                )
                await db.commit()
                updated_deposits.append((deposit_id, new_amount, created_at, now.isoformat()))
            else:
                updated_deposits.append(dep)

        response = (
            "╔════════════════════╗\n"
            "  <b>🏦 Ваши депозиты</b>\n"
            "╚════════════════════╝\n\n"
        )
        for deposit in updated_deposits:
            deposit_id, amount, created_at, last_interest = deposit
            created_at_dt = datetime.fromisoformat(created_at)
            created_at_msk = created_at_dt.replace(tzinfo=pytz.UTC).astimezone(moscow_tz)
            last_interest_msk = datetime.fromisoformat(last_interest).replace(tzinfo=pytz.UTC).astimezone(
                moscow_tz) if last_interest else "Нет начислений"
            response += (
                f"📌 <b>Депозит #{deposit_id}</b>\n"
                f"💰 Сумма: <code>{format_balance(amount)}</code> GG\n"
                f"📅 Открыт: <code>{created_at_msk.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                f"📈 Проценты на: <code>{last_interest_msk.strftime('%Y-%m-%d %H:%M:%S') if last_interest else 'Нет'}</code>\n"
                "────────────────────\n\n"
            )

        # Клавиатура для закрытия депозитов
        inline_keyboard = [
            [InlineKeyboardButton(text=f"🔒 Закрыть депозит #{dep[0]}", callback_data=f"bank_close_{dep[0]}")]
            for dep in updated_deposits]
        markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

        response += (
            f"💰 <b>Баланс:</b> <code>{format_balance(user_coins)}</code> GG\n"
            "<i>💸 Проценты (10%) начисляются автоматически каждую неделю по понедельникам в 00:00 МСК.</i>"
        )
        await message.reply(response, reply_markup=markup, parse_mode="HTML")
        return

    if len(parts) >= 2:
        # Создать новый депозит
        input_amount = " ".join(parts[1:]).lower()
        if input_amount in ("все", "всё"):
            amount = user_coins
        else:
            amount = parse_bet_input(input_amount)
            if amount < 0:
                await message.reply(
                    "╔════════════════════╗\n"
                    "  <b>🏦 Ошибка</b>\n"
                    "╚════════════════════╝\n\n"
                    "❌ Укажите корректную сумму депозита:\n"
                    "  • Число (например, 1000, 100к, 1.5кк)\n"
                    "  • Или <code>все</code>",
                    parse_mode="HTML"
                )
                return

        if amount < 10:
            await message.reply(
                "╔════════════════════╗\n"
                "  <b>🏦 Ошибка</b>\n"
                "╚════════════════════╝\n\n"
                "<i>❌ Минимальная сумма депозита — <b>10</b> GG.</i>",
                parse_mode="HTML"
            )
            return
        if amount > user_coins:
            await message.reply(
                "╔════════════════════╗\n"
                "  <b>🏦 Ошибка</b>\n"
                "╚════════════════════╝\n\n"
                "<i>❌ Недостаточно GG на балансе.</i>\n"
                f"<i>💰 Ваш баланс: <code>{format_balance(user_coins)}</code> GG</i>",
                parse_mode="HTML"
            )
            return
        if amount == 0:
            await message.reply(
                "╔════════════════════╗\n"
                "  <b>🏦 Ошибка</b>\n"
                "╚════════════════════╝\n\n"
                "<i>❌ Ваш баланс равен 0.</i>\n"
                "<i>➡️ Пополните баланс, чтобы создать депозит.</i>",
                parse_mode="HTML"
            )
            return

        # Проверяем количество депозитов
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM deposits WHERE user_id = ?", (user_id,))
            deposit_count = (await cursor.fetchone())[0]

        if deposit_count >= 4:
            await message.reply(
                "╔════════════════════╗\n"
                "  <b>🏦 Ошибка</b>\n"
                "╚════════════════════╝\n\n"
                "<i>❌ У вас уже 4 активных депозита.</i>\n"
                "<i>➡️ Закройте один, чтобы создать новый.</i>",
                parse_mode="HTML"
            )
            return

        # Создаем депозит
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT MAX(deposit_id) FROM deposits WHERE user_id = ?", (user_id,))
            max_id = (await cursor.fetchone())[0] or 0
            deposit_id = max_id + 1
            now = datetime.now(pytz.UTC).isoformat()
            await db.execute(
                "INSERT INTO deposits (user_id, deposit_id, amount, created_at, last_interest) VALUES (?, ?, ?, ?, ?)",
                (user_id, deposit_id, amount, now, now)
            )
            await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (amount, user_id))
            await db.commit()

        balance = format_balance(user_coins - amount)
        await message.reply(
            "╔════════════════════╗\n"
            "  <b>🏦 Депозит создан</b>\n"
            "╚════════════════════╝\n\n"
            f"📌 <b>Депозит #{deposit_id}</b>\n"
            f"💰 Сумма: <code>{format_balance(amount)}</code> GG\n"
            f"📅 Создан: <code>{datetime.now(moscow_tz).strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"💰 Ваш баланс: <code>{balance}</code> GG\n"
            "────────────────────\n"
            "<i>💸 Проценты (10%) будут начисляться еженедельно.</i>",
            parse_mode="HTML"
        )
        return

    await message.reply(
        "╔════════════════════╗\n"
        "  <b>🏦 Помощь по банку</b>\n"
        "╚════════════════════╝\n\n"
        "📋 <b>Команды:</b>\n"
        "  • <code>/bank</code> — Показать ваши депозиты\n"
        "  • <code>/bank &lt;сумма&gt;</code> — Создать депозит\n"
        "  • <code>/bank все</code> — Вложить весь баланс\n\n"
        "📌 Примеры:\n"
        "  • <code>/bank 100к</code>\n"
        "  • <code>/bank 1.5кк</code>\n"
        "────────────────────\n"
        "<i>💸 Проценты (10%) начисляются автоматически каждую неделю.</i>",
        parse_mode="HTML"
    )


# Обработчик текстового ввода "банк"
@dp.message(lambda m: m.text and m.text.lower().startswith("банк"))
async def txt_bank(message: types.Message):
    await cmd_bank(message)


# Обработчик закрытия депозита
@dp.callback_query(lambda c: c.data.startswith("bank_close_"))
async def bank_close_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    deposit_id = int(call.data.split("_")[2])

    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем существование пользователя
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        user_result = await cursor.fetchone()
        if not user_result:
            await call.answer("❌ Вы не зарегистрированы. Используйте /start.", show_alert=True)
            await call.message.edit_text(
                "❌ <b>Ошибка</b> ❌\n\n"
                "Вы не зарегистрированы.\n"
                "➡️ Используйте <code>/start</code> для регистрации.",
                parse_mode="HTML"
            )
            return
        new_coins = user_result[0]

        # Проверяем существование депозита
        cursor = await db.execute(
            "SELECT amount, last_interest, created_at FROM deposits WHERE user_id = ? AND deposit_id = ?",
            (user_id, deposit_id)
        )
        deposit = await cursor.fetchone()

        if not deposit:
            await call.answer("❌ Депозит не найден.", show_alert=True)
            return

        amount, last_interest, created_at = deposit
        now = datetime.now(pytz.UTC)
        last_interest_dt = datetime.fromisoformat(last_interest) if last_interest else datetime.fromisoformat(
            created_at)
        weeks_passed = (now - last_interest_dt).days // 7
        if weeks_passed > 0:
            amount = int(amount * (1.1 ** weeks_passed))  # 10% ставка
            await db.execute(
                "UPDATE deposits SET amount = ?, last_interest = ? WHERE user_id = ? AND deposit_id = ?",
                (amount, now.isoformat(), user_id, deposit_id)
            )

        # Закрываем депозит
        await db.execute("DELETE FROM deposits WHERE user_id = ? AND deposit_id = ?", (user_id, deposit_id))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()

        # Проверяем, остались ли другие депозиты
        cursor = await db.execute("SELECT COUNT(*) FROM deposits WHERE user_id = ?", (user_id,))
        remaining_deposits = (await cursor.fetchone())[0]

    moscow_tz = pytz.timezone('Europe/Moscow')
    balance = format_balance(new_coins + amount)

    # Формируем клавиатуру
    inline_keyboard = []
    if remaining_deposits > 0:
        inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="bank_back")])
    markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard) if inline_keyboard else None

    await call.message.edit_text(
        "╔════════════════════╗\n"
        "  <b>🏦 Депозит закрыт</b>\n"
        "╚════════════════════╝\n\n"
        f"📌 <b>Депозит #{deposit_id}</b>\n"
        f"💰 Вы получили: <code>{format_balance(amount)}</code> GG\n"
        f"📅 Дата: <code>{datetime.now(moscow_tz).strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"💰 Ваш баланс: <code>{balance}</code> GG",
        parse_mode="HTML",
        reply_markup=markup
    )
    await call.answer()


# Обработчик кнопки "Назад"
@dp.callback_query(lambda c: c.data == "bank_back")
async def bank_back_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем существование пользователя
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await call.answer("❌ Вы не зарегистрированы. Используйте /start.", show_alert=True)
            await call.message.edit_text(
                "❌ <b>Ошибка</b> ❌\n\n"
                "Вы не зарегистрированы.\n"
                "➡️ Используйте <code>/start</code> для регистрации.",
                parse_mode="HTML"
            )
            return
        user_coins = result[0]

        # Получаем список депозитов
        cursor = await db.execute(
            "SELECT deposit_id, amount, created_at, last_interest FROM deposits WHERE user_id = ?",
            (user_id,)
        )
        deposits = await cursor.fetchall()

    if not deposits:
        await call.message.edit_text(
            "╔════════════════════╗\n"
            "  <b>🏦 Ваш банк</b>\n"
            "╚════════════════════╝\n\n"
            "❌ У вас нет активных депозитов.\n"
            "💸 Создайте новый депозит:\n"
            "  • <code>/bank 100к</code>\n"
            "  • <code>/bank 1.5кк</code>\n"
            "  • <code>/bank все</code>",
            parse_mode="HTML"
        )
        await call.answer()
        return

    # Автоматическое начисление процентов
    now = datetime.now(pytz.UTC)
    moscow_tz = pytz.timezone('Europe/Moscow')
    updated_deposits = []
    async with aiosqlite.connect(DB_PATH) as db:
        for dep in deposits:
            deposit_id, amount, created_at, last_interest = dep
            last_interest_dt = datetime.fromisoformat(last_interest) if last_interest else datetime.fromisoformat(
                created_at)
            weeks_passed = (now - last_interest_dt).days // 7
            if weeks_passed > 0:
                new_amount = int(amount * (1.1 ** weeks_passed))  # 10% ставка
                await db.execute(
                    "UPDATE deposits SET amount = ?, last_interest = ? WHERE user_id = ? AND deposit_id = ?",
                    (new_amount, now.isoformat(), user_id, deposit_id)
                )
                await db.commit()
                updated_deposits.append((deposit_id, new_amount, created_at, now.isoformat()))
            else:
                updated_deposits.append(dep)

    response = (
        "╔════════════════════╗\n"
        "  <b>🏦 Ваши депозиты</b>\n"
        "╚════════════════════╝\n\n"
    )
    for deposit in updated_deposits:
        deposit_id, amount, created_at, last_interest = deposit
        created_at_dt = datetime.fromisoformat(created_at)
        created_at_msk = created_at_dt.replace(tzinfo=pytz.UTC).astimezone(moscow_tz)
        last_interest_msk = datetime.fromisoformat(last_interest).replace(tzinfo=pytz.UTC).astimezone(
            moscow_tz) if last_interest else "Нет начислений"
        response += (
            f"📌 <b>Депозит #{deposit_id}</b>\n"
            f"💰 Сумма: <code>{format_balance(amount)}</code> GG\n"
            f"📅 Открыт: <code>{created_at_msk.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"📈 Проценты на: <code>{last_interest_msk.strftime('%Y-%m-%d %H:%M:%S') if last_interest else 'Нет'}</code>\n"
            "────────────────────\n\n"
        )

    # Клавиатура для закрытия депозитов
    inline_keyboard = [[InlineKeyboardButton(text=f"🔒 Закрыть депозит #{dep[0]}", callback_data=f"bank_close_{dep[0]}")]
                       for dep in updated_deposits]
    markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    response += (
        f"💰 <b>Баланс:</b> <code>{format_balance(user_coins)}</code> GG\n"
        "<i>💸 Проценты (10%) начисляются автоматически каждую неделю по понедельникам в 00:00 МСК.</i>"
    )
    await call.message.edit_text(response, reply_markup=markup, parse_mode="HTML")
    await call.answer()

@dp.message(Command("bank"))
async def cmd_bank_handler(message: types.Message):
    await cmd_bank(message)  # Вызываем вашу существующую функцию

# =================================== КОЛЕСО ФОРТУНЫ (WHEEL) ===========================

active_wheel_players = set()  # Список активных игроков для предотвращения множественных игр

@dp.message(Command("wheel"))
@dp.message(lambda m: m.text and m.text.lower().startswith("колесо"))
async def cmd_wheel(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_money = result[0]

    if user_id in active_wheel_players:
        await message.reply("<i>Вы уже крутите колесо! Дождитесь окончания игры.</i>", parse_mode="HTML")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("<i>Используйте: /wheel сумма_ставки (минимум 10 монет)</i>", parse_mode="HTML")
        return

    bet = parse_bet_input(args[1], user_money)
    if bet < 10:
        await message.reply("❗ Минимальная ставка — <b>10</b> монет.", parse_mode="HTML")
        return
    if user_money < bet:
        await message.reply("Недостаточно монет для ставки.", parse_mode="HTML")
        return

    active_wheel_players.add(user_id)  # Добавляем в активные

    # Списываем ставку
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.commit()

    # Запускаем игру асинхронно
    await run_wheel_game(message, bet, user_id)

async def run_wheel_game(message: types.Message, bet: int, user_id: int):
    try:
        # Сообщение "Колесо крутится..."
        status_msg = await message.reply(
            "🎡 Колесо крутится...\n\n<b>Шансы выпадения:</b>\n❌ Проигрыш: 16%\nx0.2: 18%\nx0.5: 17%\nx1: 16%\nx1.5: 13%\nx2: 11%\nx5: 9%\n",
            parse_mode="HTML"
        )

        # Ждём 1.5 секунды для анимации
        await asyncio.sleep(1.5)

        # Сектора (как в вашем примере)
        sectors_common = ["❌ Проигрыш", "x0.2", "x0.5"]
        sectors_rare = ["x1", "x1.5", "x2", "x5"]

        # Определяем финальный сектор
        if bet < 1000000:
            final_sector = random.choice(sectors_common if random.random() < (2/3) else sectors_rare)
        else:
            final_sector = random.choice(sectors_common if random.random() < (3/4) else sectors_rare)

        # Рассчитываем выигрыш
        multiplier = 0.0
        if final_sector != "❌ Проигрыш" and final_sector.startswith("x"):
            multiplier = float(final_sector[1:])
        win_amount = int(bet * multiplier)

        # Обновляем баланс и статистику
        async with aiosqlite.connect(DB_PATH) as db:
            if win_amount > 0:
                await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (win_amount, user_id))
            if win_amount > bet:
                await db.execute("UPDATE users SET win_amount = win_amount + ? WHERE user_id = ?", (win_amount - bet, user_id))
            else:
                await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?", (bet - win_amount, user_id))
            await db.commit()

        # Итоговое сообщение
        final_text = (
            f"🎡 <b>Колесо фортуны</b>\n"
            f"Сектор: <b>{final_sector}</b>\n"
            f"Ставка: <code>{format_balance(bet)}</code>\n"
            f"Выигрыш: <code>{format_balance(win_amount)}</code>"
        )
        await status_msg.edit_text(final_text, parse_mode="HTML")

    finally:
        # Убираем из активных
        active_wheel_players.discard(user_id)

# =================================== ЛОТЕРЕЯ (LOTTERY) ===========================

LOTTERY_ICONS = ['🍒', '🍋', '🍉', '🔔', '⭐']  # Иконки для слотов (можно изменить)

@dp.message(Command("lottery"))
@dp.message(lambda m: m.text and m.text.lower().startswith("лотерея"))
async def cmd_lottery(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        if not result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        user_money = result[0]

    args = message.text.split()
    if len(args) < 2:
        await message.reply("💰 Укажите ставку. Пример: /lottery 100000", parse_mode="HTML")
        return

    bet = parse_bet_input(args[1], user_money)
    if bet < 10:
        await message.reply("❗ Минимальная ставка — <b>10</b> монет.", parse_mode="HTML")
        return
    if user_money < bet:
        await message.reply("Недостаточно монет для ставки.", parse_mode="HTML")
        return

    # Списываем ставку
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, user_id))
        await db.commit()

    # Генерация значков
    slots = [random.choice(LOTTERY_ICONS) for _ in range(5)]
    result_text = " ".join(slots)

    # Подсчёт совпадений
    max_count = max(slots.count(icon) for icon in LOTTERY_ICONS)
    progress = f"{max_count}/3"

    # Проверка победы
    if max_count >= 3:
        prize = bet * 3
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (prize, user_id))
            await db.execute("UPDATE users SET win_amount = win_amount + ? WHERE user_id = ?", (prize - bet, user_id))
            await db.commit()
        await message.reply(
            f"{result_text}\n\n"
            f"📊 Совпадений: {progress}\n"
            f"🍀 Вы выиграли! Ваша награда: {format_balance(prize)} 💰",
            parse_mode="HTML"
        )
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET lose_amount = lose_amount + ? WHERE user_id = ?", (bet, user_id))
            await db.commit()
        await message.reply(
            f"{result_text}\n\n"
            f"📊 Совпадений: {progress}\n"
            f"😔 Увы, вы проиграли {format_balance(bet)} 💰",
            parse_mode="HTML"
        )

#=================================== ПЕРЕВОД ДЕНЕГ ===========================

async def handle_transfer_logic(message: types.Message, transfer_amount: int, target_id: int, source_id: int):
    """Основная логика для перевода средств."""
    if transfer_amount < 100:
        await message.reply("❌ Сумма перевода должна быть не менее 100 GG.", parse_mode="HTML")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (source_id,))
        source_result = await cursor.fetchone()
        if not source_result:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        source_balance = source_result[0]
        if source_balance < transfer_amount:
            await message.reply(f"❌ Недостаточно GG. Ваш баланс: <code>{format_balance(source_balance)}</code>", parse_mode="HTML")
            return

        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (target_id,))
        target_result = await cursor.fetchone()
        if not target_result:
            await message.reply("❌ Получатель не зарегистрирован.", parse_mode="HTML")
            return

        if source_id == target_id:
            await message.reply("❌ Нельзя переводить деньги самому себе.", parse_mode="HTML")
            return

        # Комиссия 5% (минимум 5 GG)
        fee = max(5, int(transfer_amount * 0.05))
        received_amount = transfer_amount - fee

        # Выполняем перевод
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (transfer_amount, source_id))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (received_amount, target_id))
        await db.commit()

    try:
        # Уведомление получателю
        await bot.send_message(
            target_id,
            f"💸 <b>Вы получили перевод!</b>\n\n"
            f"💰 Сумма: <code>{format_balance(received_amount)}</code> GG\n"
            f"👤 От: {message.from_user.full_name} (ID: {source_id})\n"
            f"📊 Комиссия: <code>{format_balance(fee)}</code> GG (5%)",
            parse_mode="HTML"
        )
    except Exception:
        pass  # Игнорируем ошибки отправки, если пользователь заблокировал бота

    await message.reply(
        f"✅ <b>Перевод успешен!</b>\n\n"
        f"💰 Сумма: <code>{format_balance(received_amount)}</code> GG (получатель)\n"
        f"👤 Получатель: ID <code>{target_id}</code>\n"
        f"📊 Комиссия: <code>{format_balance(fee)}</code> GG (5%)\n"
        f"💸 Ваш баланс: <code>{format_balance(source_balance - transfer_amount)}</code>",
        parse_mode="HTML"
    )

@dp.message(Command("pay"))
async def process_pay_command(message: types.Message):
    command_args = message.text.split()
    sender_user_id = message.from_user.id

    # Проверка баланса отправителя заранее для "все"
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (sender_user_id,))
        sender_data = await cursor.fetchone()
        if not sender_data:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        sender_balance = sender_data[0]

    # Если ответ на сообщение другого пользователя и только сумма указана
    if message.reply_to_message and message.reply_to_message.from_user.id != sender_user_id and len(command_args) == 2:
        amount_input = command_args[1]
        parsed_amount = parse_bet_input(amount_input, sender_balance)
        if parsed_amount <= 0:
            await message.reply("❌ Некорректная сумма.", parse_mode="HTML")
            return
        target_user_id = message.reply_to_message.from_user.id
        await handle_transfer_logic(message, parsed_amount, target_user_id, sender_user_id)
        return

    # Если указан ID/юзернейм
    if len(command_args) < 3:
        await message.reply(
            "💸 <b>Перевод GG</b>\n\n"
            "📋 <b>Формат:</b> /pay <code>сумма</code> <code>ID/юзернейм</code>\n"
            "📝 <b>Примеры:</b>\n"
            "• /pay 1000 123456789\n"
            "• /pay 1k @username\n"
            "• /pay 500к @friend\n"
            "• /pay все 123456789 (весь баланс)\n\n"
            "<i>💡 Минимальная сумма перевода: <code>100</code> GG</i>\n"
            "<i>💡 Если ответить на сообщение пользователя, можно использовать: /pay <code>сумма</code></i>",
            parse_mode="HTML"
        )
        return

    amount_input = command_args[1]
    recipient_input = " ".join(command_args[2:]).removeprefix("@") if " ".join(command_args[2:]).startswith("@") else " ".join(command_args[2:])

    parsed_amount = parse_bet_input(amount_input, sender_balance)
    if parsed_amount <= 0:
        await message.reply("❌ Некорректная сумма.", parse_mode="HTML")
        return

    # Поиск по ID или username
    if recipient_input.isdigit():
        target_user_id = int(recipient_input)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (recipient_input,))
            result_data = await cursor.fetchone()
            if not result_data:
                await message.reply(f"❌ Пользователь @{recipient_input} не найден.", parse_mode="HTML")
                return
            target_user_id = result_data[0]

    await handle_transfer_logic(message, parsed_amount, target_user_id, sender_user_id)

@dp.message(lambda m: m.text and m.text.lower().startswith(("перевод", "pay")))
async def process_text_transfer(message: types.Message):
    message_text = message.text
    text_args = message_text.split()
    sender_user_id = message.from_user.id

    # Проверка баланса отправителя заранее для "все"
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (sender_user_id,))
        sender_data = await cursor.fetchone()
        if not sender_data:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        sender_balance = sender_data[0]

    # Если ответ на сообщение другого пользователя и только сумма указана
    if message.reply_to_message and message.reply_to_message.from_user.id != sender_user_id and len(text_args) == 2:
        amount_input = text_args[1]
        parsed_amount = parse_bet_input(amount_input, sender_balance)
        if parsed_amount <= 0:
            await message.reply("❌ Некорректная сумма.", parse_mode="HTML")
            return
        target_user_id = message.reply_to_message.from_user.id
        await handle_transfer_logic(message, parsed_amount, target_user_id, sender_user_id)
        return

    if len(text_args) < 2:
        await message.reply(
            "💸 <b>Перевод GG</b>\n\n"
            "📋 <b>Формат:</b> перевод <code>сумма</code> <code>ID/юзернейм</code>\n"
            "📝 <b>Примеры:</b>\n"
            "• перевод 1000 123456789\n"
            "• перевод 1k @username\n"
            "• перевод 500к @friend\n"
            "• перевод все 123456789 (весь баланс)\n\n"
            "<i>💡 Минимальная сумма перевода: <code>100</code> GG</i>\n"
            "<i>💡 Если ответить на сообщение пользователя, можно использовать: перевод <code>сумма</code></i>",
            parse_mode="HTML"
        )
        return

    amount_input = text_args[1]
    if len(text_args) > 2:
        recipient_full_input = " ".join(text_args[2:])
        recipient_clean = recipient_full_input.removeprefix("@") if recipient_full_input.startswith("@") else recipient_full_input
    else:
        recipient_clean = None

    parsed_amount = parse_bet_input(amount_input, sender_balance)
    if parsed_amount <= 0:
        await message.reply("❌ Некорректная сумма.", parse_mode="HTML")
        return

    # Если ответ на сообщение и нет указания получателя
    if message.reply_to_message and message.reply_to_message.from_user.id != sender_user_id and not recipient_clean:
        target_user_id = message.reply_to_message.from_user.id
        await handle_transfer_logic(message, parsed_amount, target_user_id, sender_user_id)
        return

    # Если указан получатель
    if recipient_clean:
        if recipient_clean.isdigit():
            target_user_id = int(recipient_clean)
        else:
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (recipient_clean,))
                result_data = await cursor.fetchone()
                if not result_data:
                    await message.reply(f"❌ Пользователь @{recipient_clean} не найден.", parse_mode="HTML")
                    return
                target_user_id = result_data[0]
        await handle_transfer_logic(message, parsed_amount, target_user_id, sender_user_id)
    else:
        await message.reply(
            "❌ Укажите получателя.\n"
            "📋 <b>Формат:</b> перевод <code>сумма</code> <code>ID/юзернейм</code>\n"
            "<i>💡 Минимальная сумма перевода: <code>100</code> GG</i>\n"
            "<i>Или ответьте на сообщение пользователя и напишите: перевод <code>сумма</code></i>",
            parse_mode="HTML"
        )

@dp.message(lambda m: m.reply_to_message and m.reply_to_message.from_user.id != m.from_user.id and ((m.text.startswith("/pay ") and len(m.text.split()) == 2) or (m.text.lower().startswith("перевод ") and len(m.text.split()) == 2)))
async def handle_reply_short_transfer(message: types.Message):
    """Обработчик короткого /pay или перевод в ответ на сообщение."""
    message_text = message.text
    sender_user_id = message.from_user.id

    # Проверка баланса
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT coins FROM users WHERE user_id = ?", (sender_user_id,))
        sender_data = await cursor.fetchone()
        if not sender_data:
            await message.reply("❌ Вы не зарегистрированы. Введите /start.", parse_mode="HTML")
            return
        sender_balance = sender_data[0]

    if message_text.startswith("/pay "):
        amount_input = message_text[5:].strip()
    elif message_text.lower().startswith("перевод "):
        amount_input = message_text[8:].strip()
    else:
        return

    parsed_amount = parse_bet_input(amount_input, sender_balance)
    if parsed_amount <= 0:
        await message.reply("❌ Некорректная сумма.", parse_mode="HTML")
        return

    target_user_id = message.reply_to_message.from_user.id
    await handle_transfer_logic(message, parsed_amount, target_user_id, sender_user_id)

#=================================== СПИСОК ===========================

@dp.message(lambda m: m.text and m.text.lower() in ["помощь", "список"])
async def txt_help(message: types.Message):
    await cmd_help(message)


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    user_id = message.from_user.id
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🎮 Игры", callback_data=f"help_games_{user_id}"),
            ],
            [
                InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/Ferzister"),
            ]
        ]
    )
    text = (
        "╔══════════════════╗\n"
        "   <b>📖 Меню и помощь</b>\n"
        "╚══════════════════╝\n\n"
        "🔹 <b>Основные команды:</b>\n"
        "  🔸 <b>/start</b> — Начать приключение и зарегистрироваться\n"
        "  🔸 <b>/profile</b> — Ваш профиль и статистика\n"
        "  🔸 <b>/bonus</b> — Получить бонус\n"
        "  🔸 <b>/top</b> — Топ-10 игроков по балансу\n"
        "  🔸 <b>/hide</b> — Скрыть/показать себя в топах\n"
        "  🔸 <b>/crypto</b> — Торговая площадка Fezcoin\n"
        "  🔸 <b>/status</b> — Просмотр и покупка статусов\n"
        "  🔸 <b>/box</b> — Открыть коробку с наградами (<i>каждые 6 часов</i>)\n"
        "  🔸 <b>/pay</b> — Перевод GG другому игроку (<i>/pay сумма ID/юзернейм</i>)\n"
        "  🔸 <b>/ref</b> — Просмотреть реферальную ссылку и статистику\n"
        "  🔸 <b>/donat</b> — Информация о покупке Fezcoin\n"
        "  🔸 <b>/farm</b> — Ферма для добычи Fezcoin\n"
        "  🔸 <b>/bank</b> — Просмотр банка\n"
        "  🔸 <b>/bank сумма</b> — Вложить в банк (<i>+10% еженедельно</i>)\n"
        "  🔸 <b>/promo</b> — Активировать/создать промокод\n\n"
        "🌟 <b>Выберите 'Игры' или свяжитесь с поддержкой!</b>"
    )
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(lambda c: c.data.startswith("help_games_"))
async def help_games_callback(call: types.CallbackQuery):
    parts = call.data.split("_")
    if len(parts) != 3:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return
    try:
        original_user_id = int(parts[2])
    except ValueError:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    if call.from_user.id != original_user_id:
        await call.answer("❌ Эта кнопка не для вас! Используйте /help, чтобы открыть свои кнопки.", show_alert=True)
        return

    user_id = call.from_user.id
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data=f"help_back_{user_id}"),
            ],
            [
                InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/Ferzister"),
            ]
        ]
    )
    text = (
        "╔══════════════════╗\n"
        "   <b>🎮 Игры</b>\n"
        "╚══════════════════╝\n\n"
        "🎲 <b>Доступные игры:</b>\n"
        "  🎰 <b>Монета</b> — Орёл или Решка (<code>/coin ставка</code>)\n"
        "  🎣 <b>Рыбалка</b> — Закинь удочку и лови рыбу (<code>/fish ставка</code>)\n"
        "  💣 <b>Минёр</b> — Открой клетки и не попади на мину (<code>/miner ставка</code>)\n"
        "  🎲 <b>Кубик</b> — Угадай выпадение кубика (<code>/dice ставка условие</code>)\n"
        "  🏰 <b>Башня</b> — Игра в башню (<code>/tower ставка</code>)\n"
        "  🎡 <b>Колесо фортуны</b> — Крути колесо и выигрывай (<code>/wheel ставка</code>)\n"
        "  🍒 <b>Лотерея</b> — Собери 3+ совпадения (<code>/lottery ставка</code>)\n"
        "  🎯 <b>Дуэль</b> — Вызови игрока на дуэль (<code>/duel ставка</code>)\n"
        "  🎲 <b>Кости</b> — Угадай сумму двух кубиков (<code>/cubes ставка [тип ставки]</code>)\n"
        "  🎰 <b>Рулетка</b> — Сделай ставку и испытай удачу (<code>/roulette ставка прогноз</code>)\n\n"
        "🌟 <b>Испытай удачу!</b>"
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("help_back_"))
async def help_back_callback(call: types.CallbackQuery):
    parts = call.data.split("_")
    if len(parts) != 3:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return
    try:
        original_user_id = int(parts[2])
    except ValueError:
        await call.answer("❌ Ошибка данных кнопки.", show_alert=True)
        return

    if call.from_user.id != original_user_id:
        await call.answer("❌ Эта кнопка не для вас! Используйте /help, чтобы открыть свои кнопки.", show_alert=True)
        return

    user_id = call.from_user.id
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🎮 Игры", callback_data=f"help_games_{user_id}"),
            ],
            [
                InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/Ferzister"),
            ]
        ]
    )
    text = (
        "╔══════════════════╗\n"
        "   <b>📖 Меню и помощь</b>\n"
        "╚══════════════════╝\n\n"
        "🔹 <b>Основные команды:</b>\n"
        "  🔸 <b>/start</b> — Начать приключение и зарегистрироваться\n"
        "  🔸 <b>/profile</b> — Ваш профиль и статистика\n"
        "  🔸 <b>/bonus</b> — Получить бонус \n"
        "  🔸 <b>/top</b> — Топ-10 игроков по балансу\n"
        "  🔸 <b>/hide</b> — Скрыть/показать себя в топах\n"
        "  🔸 <b>/crypto</b> — Торговая площадка Fezcoin\n"
        "  🔸 <b>/status</b> — Просмотр и покупка статусов\n"
        "  🔸 <b>/box</b> — Открыть коробку с наградами (<i>каждые 6 часов</i>)\n"
        "  🔸 <b>/pay</b> — Перевод GG другому игроку (<i>/pay сумма ID/юзернейм</i>)\n"
        "  🔸 <b>/ref</b> — Просмотреть реферальную ссылку и статистику\n"
        "  🔸 <b>/donat</b> — Информация о покупке Fezcoin\n"
        "  🔸 <b>/farm</b> — Ферма для добычи Fezcoin\n"
        "  🔸 <b>/bank</b> — Просмотр банка\n"
        "  🔸 <b>/bank сумма</b> — Вложить в банк (<i>+10% еженедельно</i>)\n"
        "  🔸 <b>/promo</b> — Активировать/создать промокод\n\n"
        "🌟 <b>Выберите 'Игры' или свяжитесь с поддержкой!</b>"
    )
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()



#=================================== ФУНКЦИИ ===========================

def format_balance(balance):
    balance = float(balance)
    if balance == 0:
        return "0"
    exponent = int(math.log10(abs(balance)))
    group = exponent // 3
    scaled_balance = balance / (10 ** (group * 3))
    formatted_balance = f"{scaled_balance:.2f}"
    suffix = "к" * group
    return formatted_balance.rstrip('0').rstrip('.') + suffix

class CallbackAntiSpamMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user_id = event.from_user.id
        now_ts = datetime.now(UTC).timestamp()
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT last_click FROM coin_spam WHERE user_id = ?", (user_id,))
            spam_row = await cursor.fetchone()
            if spam_row and now_ts - spam_row[0] < 1:
                await event.answer("Не так быстро! Подождите пару секунд.", show_alert=True)
                return
            await db.execute("INSERT OR REPLACE INTO coin_spam (user_id, last_click) VALUES (?, ?)", (user_id, now_ts))
            await db.commit()
        return await handler(event, data)

dp.callback_query.middleware(CallbackAntiSpamMiddleware())
def _to_decimal_safe(val):
    try:
        return Decimal(str(val))
    except Exception:
        return None

def parse_bet_input(arg: str, user_money: Optional[Union[int, float, str, Decimal]] = None) -> int:
    if arg is None:
        return -1

    s = str(arg).strip().lower()
    s = s.replace(" ", "").replace("_", "")

    if s in ("все", "всё", "all"):
        um = _to_decimal_safe(user_money)
        if um is None:
            return -1
        try:
            return int(um)
        except Exception:
            return -1

    m = re.fullmatch(r'([0-9]+(?:[.,][0-9]{1,2})?)([kк]*)', s)
    if not m:
        return -1

    num_str, k_suffix = m.groups()
    num_str = num_str.replace(',', '.')
    try:
        num = Decimal(num_str)
    except Exception:
        return -1

    multiplier = Decimal(1000) ** len(k_suffix)
    result = num * multiplier

    try:
        return int(result)
    except Exception:
        return -1


async def main():
    try:
        # Инициализация базы данных
        await init_db()
        print("База данных инициализирована")
    except Exception as e:
        print(f"Ошибка при инициализации базы данных: {e}")
        raise

    # Удаляем вебхук и игнорируем старые апдейты
    await bot.delete_webhook(drop_pending_updates=True)
    print("Бот запущен")
    # Запускаем polling
    await dp.start_polling(bot, drop_pending_updates=True)


if __name__ == "__main__":
    asyncio.run(main())




