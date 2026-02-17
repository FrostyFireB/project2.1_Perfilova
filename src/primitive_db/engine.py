import shlex

import prompt

from src.primitive_db.core import create_table, drop_table, list_tables
from src.primitive_db.utils import load_metadata, save_metadata

META_FILE = "db_meta.json"


def print_help():
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def run():
    print("***База данных***")
    print_help()

    while True:
        metadata = load_metadata(META_FILE)

        user_input = prompt.string(">>>Введите команду: ")
        if not user_input.strip():
            continue
        
        try:
            args = shlex.split(user_input)
        except ValueError:
            print("Некорректное значение: введенная строка. Попробуйте снова.")
            continue

        command = args[0]

        if command == "exit":
            break

        if command == "help":
            print_help()
            continue

        if command == "list_tables":
            tables = list_tables(metadata)
            for t in tables:
                print(f"- {t}")
            continue

        if command == "drop_table":
            if len(args) != 2:
                print("Некорректное значение: drop_table. Попробуйте снова.")
                continue
            
            table_name = args[1]
            try:
                metadata = drop_table(metadata, table_name)
            except ValueError as e:
                print(f"Ошибка: {e}")
                continue
            
            save_metadata(META_FILE, metadata)
            print(f'Таблица "{table_name}" успешно удалена.')
            continue

        if command == "create_table":
            if len(args) < 3:
                print("Некорректное значение: create_table. Попробуйте снова.")
                continue

            table_name = args[1]
            columns = []

            for token in args[2:]:
                if ":" not in token:
                    print(f"Некорректное значение: {token}. Попробуйте снова.")
                    columns = None
                    break
                name, typ = token.split(":", 1)
                columns.append((name, typ))
            
            if columns is None:
                continue

            try:
                metadata = create_table(metadata, table_name, columns)
            except ValueError as e:
                print(f"Ошибка: {e}")
                continue

            save_metadata(META_FILE, metadata)

            print(f'Таблица "{table_name}" успешно создана.')
            continue

        print(f"Функции {command} нет. Попробуйте снова.")

