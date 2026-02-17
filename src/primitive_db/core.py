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
    return metadata


def drop_table(metadata, table_name):
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    del metadata[table_name]
    return metadata


def list_tables(metadata):
    return list(metadata.keys())

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
    return table_data, new_id


def select(table_data, where_clause=None):
    if where_clause is None:
        return table_data

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
    return table_data, updated


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
    return new_data, deleted


def table_info(metadata, table_name, table_data):
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')

    cols = metadata[table_name]
    columns_str = ", ".join([f'{c["name"]}:{c["type"]}' for c in cols])
    count = len(table_data)
    return columns_str, count