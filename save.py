@dp.message(Command("gdata"))
async def send_data_db(message: types.Message):
    if message.from_user.id != 8493326566:
        return

    try:
        db_file = FSInputFile(DB_PATH)
        await message.answer_document(db_file, caption="Вот бд")
    except FileNotFoundError:
        await message.answer("Бд не найден!")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

    try:
        db_file = FSInputFile("banned.json")
        await message.answer_document(db_file, caption="Вот баннед")
    except FileNotFoundError:
        await message.answer("Баннед не найден!")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

    try:
        db_file = FSInputFile("farms.db")
        await message.answer_document(db_file, caption="Вот фармс")
    except FileNotFoundError:
        await message.answer("Фармс не найден!")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

    try:
        db_file = FSInputFile("mahyhhyyhhr.db")
        await message.answer_document(db_file, caption="Вот маркет")
    except FileNotFoundError:
        await message.answer("Маркет не найден!")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")