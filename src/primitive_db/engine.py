import shlex
from pathlib import Path

import prompt
from prettytable import PrettyTable

from src.primitive_db.core import (
    _parse_value,
    create_table,
    delete,
    drop_table,
    insert,
    list_tables,
    select,
    table_info,
    update,
)
from src.primitive_db.utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)

META_FILE = "db_meta.json"


def print_help():
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def get_col_type(metadata, table_name, col_name):
    cols = metadata.get(table_name)
    if not cols:
        return None
    for col in cols:
        if col["name"] == col_name:
            return col["type"]
    return None


def normalize_value_for_core(raw_value, typ):
    if typ == "str":
        # если кавычек нет — добавляем
        if not (len(raw_value) >= 2 and raw_value[0] == '"' and raw_value[-1] == '"'):
            return f'"{raw_value}"'
    return raw_value


def parse_simple_condition(text):
    text = text.strip()
    if "=" not in text:
        raise ValueError(f"Некорректное значение: {text}.")
    left, right = text.split("=", 1)
    return left.strip(), right.strip()


def safe_load_table_data(table_name):
    try:
        return load_table_data(table_name)
    except FileNotFoundError:
        print(
            "Ошибка: файл данных не найден. Возможно, база данных не инициализирована."
        )
        return None


def run():
    print("***База данных***")
    print_help()

    metadata = load_metadata(META_FILE)

    while True:

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
            res = drop_table(metadata, table_name)
            if res is None:
                continue

            metadata = res
            save_metadata(META_FILE, metadata)

            data_path = Path("data") / f"{table_name}.json"
            if data_path.exists():
                data_path.unlink()
            
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

            metadata2 = create_table(metadata, table_name, columns)
            if metadata2 is None:
                continue

            metadata = metadata2
            save_metadata(META_FILE, metadata)

            print(f'Таблица "{table_name}" успешно создана.')
            continue

        if command == "info":
            if len(args) != 2:
                print("Некорректное значение: info. Попробуйте снова.")
                continue

            table_name = args[1]

            table_data = safe_load_table_data(table_name)
            if table_data is None:
                continue

            res = table_info(metadata, table_name, table_data)
            if res is None:
                continue

            cols_str, count = res
            print(f'Таблица "{table_name}"')
            print(f"Столбцы: {cols_str}")
            print(f"Количество записей: {count}")
            continue

        if command == "insert":

            if len(args) < 5 or args[1] != "into":
                print("Некорректное значение: insert. Попробуйте снова.")
                continue

            table_name = args[2]
            
            # восстанавливаем "values (...)" из исходной строки,
            # чтобы вытащить то, что в скобках
            lower = user_input.lower()
            pos = lower.find("values")
            if pos == -1:
                print("Некорректное значение: values. Попробуйте снова.")
                continue

            values_part = user_input[pos + len("values"):].strip()
            if not (values_part.startswith("(") and values_part.endswith(")")):
                print("Некорректное значение: values. Попробуйте снова.")
                continue

            inside = values_part[1:-1].strip()

            raw_values = [v.strip() for v in inside.split(",")] if inside else []

            table_data = safe_load_table_data(table_name)
            if table_data is None:
                continue

            # подгоняем строки под core.py (для str добавим кавычки,
            # если shlex их убрал)
            try:
                user_columns = [
                c for c in metadata[table_name]
                if c["name"] != "ID"
            ]
            except KeyError:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            if len(raw_values) != len(user_columns):
                print("Некорректное значение: values. Попробуйте снова.")
                continue

            normalized_values = []
            for i, col in enumerate(user_columns):
                normalized_values.append(
                    normalize_value_for_core(raw_values[i], col["type"])
                )

            res = insert(metadata, table_name, table_data, normalized_values)
            if res is None:
                continue

            table_data, new_id = res
            save_table_data(table_name, table_data)
            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
            continue

        if command == "select":
            if len(args) < 3 or args[1] != "from":
                print("Некорректное значение: select. Попробуйте снова.")
                continue

            table_name = args[2]

            table_data = safe_load_table_data(table_name)
            if table_data is None:
                continue

            where_clause = None
            lower = user_input.lower()
            idx = lower.find(" where ")
            if idx != -1:
                where_text = user_input[idx + len(" where "):].strip()            
            
                try:
                    col, raw_val = parse_simple_condition(where_text)
                except ValueError as e:
                    print(f"{e} Попробуйте снова.")
                    continue

                typ = get_col_type(metadata, table_name, col)
                if typ is None:
                    print(f"Некорректное значение: {col}. Попробуйте снова.")
                    continue

                raw_val = normalize_value_for_core(raw_val, typ)
                where_clause = {col: _parse_value(raw_val, typ)}

            rows = select(table_data, where_clause)
            if rows is None:
                continue
            
            try:
                cols = [c["name"] for c in metadata[table_name]]
            except KeyError:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            t = PrettyTable()
            t.field_names = cols
            for row in rows:
                t.add_row([row.get(c) for c in cols])
            print(t)
            continue

        if command == "update":
            if len(args) < 6 or args[2] != "set":
                print("Некорректное значение: update. Попробуйте снова.")
                continue

            table_name = args[1]

            lower = user_input.lower()
            set_idx = lower.find(" set ")
            where_idx = lower.find(" where ")
            if set_idx == -1 or where_idx == -1 or where_idx < set_idx:
                print("Некорректное значение: update. Попробуйте снова.")
                continue        

            set_text = user_input[set_idx + len(" set "):where_idx].strip()
            where_text = user_input[where_idx + len(" where "):].strip()

            try:
                set_col, set_raw = parse_simple_condition(set_text)
                where_col, where_raw = parse_simple_condition(where_text)
            except ValueError as e:
                print(f"{e} Попробуйте снова.")
                continue

            set_typ = get_col_type(metadata, table_name, set_col)
            where_typ = get_col_type(metadata, table_name, where_col)
            if set_typ is None or where_typ is None:
                print("Некорректное значение: column. Попробуйте снова.")
                continue

            set_raw = normalize_value_for_core(set_raw, set_typ)
            where_raw = normalize_value_for_core(where_raw, where_typ)

            set_clause = {set_col: _parse_value(set_raw, set_typ)}
            where_clause = {where_col: _parse_value(where_raw, where_typ)}

            table_data = safe_load_table_data(table_name)
            if table_data is None:
                continue

            matched_rows = select(table_data, where_clause)
            if matched_rows is None:
                continue
            matched_ids = [r.get("ID") for r in matched_rows if "ID" in r]

            res = update(table_data, set_clause, where_clause)
            if res is None:
                continue

            table_data, updated = res
            save_table_data(table_name, table_data)

            if updated == 1 and len(matched_ids) == 1:
                print(
                    f'Запись с ID={matched_ids[0]} в таблице "{table_name}" '
                    "успешно обновлена."
                )
            else:
                print(f"Обновлено записей: {updated}")
            continue

        if command == "delete":
            if len(args) < 5 or args[1] != "from":
                print("Некорректное значение: delete. Попробуйте снова.")
                continue

            table_name = args[2]

            lower = user_input.lower()
            idx = lower.find(" where ")
            if idx == -1:
                print("Некорректное значение: where. Попробуйте снова.")
                continue
            
            where_text = user_input[idx + len(" where "):].strip()
            try:
                where_col, where_raw = parse_simple_condition(where_text)
            except ValueError as e:
                print(f"{e} Попробуйте снова.")
                continue

            where_typ = get_col_type(metadata, table_name, where_col)
            if where_typ is None:
                print(f"Некорректное значение: {where_col}. Попробуйте снова.")
                continue

            where_raw = normalize_value_for_core(where_raw, where_typ)
            where_clause = {where_col: _parse_value(where_raw, where_typ)}

            table_data = safe_load_table_data(table_name)
            if table_data is None:
                continue

            matched_rows = select(table_data, where_clause)
            if matched_rows is None:
                continue
            matched_ids = [r.get("ID") for r in matched_rows if "ID" in r]

            res = delete(table_data, where_clause)
            if res is None:
                continue

            new_data, deleted = res
            save_table_data(table_name, new_data)

            if deleted == 1 and len(matched_ids) == 1:
                print(
                    f"Запись с ID={matched_ids[0]} успешно удалена "
                    f'из таблицы "{table_name}".'
                )

            else:
                print(f"Удалено записей: {deleted}")
            continue

        print(f"Функции {command} нет. Попробуйте снова.")