from src.primitive_db.decorators import (
    confirm_action,
    create_cacher,
    handle_db_errors,
    log_time,
)

_select_cache = create_cacher()

def _reset_select_cache():
    global _select_cache
    _select_cache = create_cacher()

ALLOWED_TYPES = {"int", "str", "bool"}

def _parse_value(value: str, typ: str):
    if typ == "int":
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Некорректное значение: {value}.")
    if typ == "bool":
        v = value.lower()
        if v == "true":
            return True
        if v == "false":
            return False
        raise ValueError(f"Некорректное значение: {value}.")
    if typ == "str":
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            return value[1:-1]
        raise ValueError(f"Некорректное значение: {value}.")
    raise ValueError(f"Некорректное значение: {typ}.")

@handle_db_errors
def create_table(metadata, table_name, columns):
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')

    for name, typ in columns:
        if typ not in ALLOWED_TYPES:
            raise ValueError(f"Некорректное значение: {typ}.")
        if name == "ID" and typ != "int":
            raise ValueError("Некорректное значение: ID.")
    
    columns_wo_id = []
    for n, t in columns:
        if n != "ID":
            columns_wo_id.append((n, t))

    columns = [("ID", "int")] + columns_wo_id
    metadata[table_name] = [{"name": n, "type": t} for n, t in columns]
    _reset_select_cache()
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    del metadata[table_name]
    _reset_select_cache()
    return metadata


def list_tables(metadata):
    return list(metadata.keys())

@handle_db_errors
@log_time
def insert(metadata, table_name, table_data, values):
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')

    columns = metadata[table_name]  # список словарей {"name":..., "type":...}
    user_columns = [c for c in columns if c["name"] != "ID"]

    if len(values) != len(user_columns):
        raise ValueError("Некорректное значение: values. Попробуйте снова.")

    max_id = 0
    for row in table_data:
        if "ID" in row and isinstance(row["ID"], int) and row["ID"] > max_id:
            max_id = row["ID"]
    new_id = max_id + 1

    row = {"ID": new_id}
    for i in range(len(user_columns)):
        col = user_columns[i]
        row[col["name"]] = _parse_value(values[i], col["type"])
    table_data.append(row)
    _reset_select_cache()
    return table_data, new_id


@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    if where_clause is None:
        return table_data

    key = (id(table_data), tuple(sorted(where_clause.items())))

    def value_func():
        result = []
        for row in table_data:
            ok = True
            for k, v in where_clause.items():
                if row.get(k) != v:
                    ok = False
                    break
            if ok:
                result.append(row)
        return result

    return _select_cache(key, value_func)


@handle_db_errors
def update(table_data, set_clause, where_clause):
    updated = 0
    for row in table_data:
        ok = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                ok = False
                break
        if ok:
            for k, v in set_clause.items():
                row[k] = v
            updated += 1

    if updated > 0:
        _reset_select_cache()
    return table_data, updated


@handle_db_errors
@confirm_action("удаление записи")
def delete(table_data, where_clause):
    new_data = []
    deleted = 0
    for row in table_data:
        ok = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                ok = False
                break
        if ok:
            deleted += 1
        else:
            new_data.append(row)
    
    if deleted > 0:
        _reset_select_cache()
    return new_data, deleted


def table_info(metadata, table_name, table_data):
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')

    cols = metadata[table_name]
    columns_str = ", ".join([f'{c["name"]}:{c["type"]}' for c in cols])
    count = len(table_data)
    return columns_str, count