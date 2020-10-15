"""Module to handle saving / loading credentials from an encrypted SQLite DB."""

from .connect import SQLiteMessenger


def save(filepath: str, json: dict, key: str, password: str):
    """Save a JSON dict of login credentials to a SQLite database."""
    msgr = SQLiteMessenger(filepath=filepath, is_zipped=True, password=password)
    msgr.drop_table_if_exists(key)
    msgr.create_table(table=key, columns=['field', 'value'],
                      datatypes=['TEXT', 'TEXT'])
    msgr.insert_into(table=key, data=list(json.items()))


def load(filepath: str, key: str, password: str) -> dict:
    """Load a JSON dict of login credentials from a SQLite database."""
    msgr = SQLiteMessenger(filepath=filepath, is_zipped=True, password=password)
    data = msgr.fetch_data('SELECT * FROM [{}]'.format(key))
    return {field: value for field, value in data}
