ALLOWED_TYPES = {"int", "str", "bool"}


def create_table(metadata, table_name, columns):
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')

    for name, typ in columns:
        if typ not in ALLOWED_TYPES:
            raise ValueError(f"Некорректное значение: {typ}.")

    for name, typ in columns:
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
