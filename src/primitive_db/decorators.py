import time
from functools import wraps


def handle_db_errors(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: файл данных не найден. "
                "Возможно, база данных не инициализирована."
            )
            return None
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None
    return wrapper


def confirm_action(action_name):

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            prompt_text = f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            answer = input(prompt_text).strip().lower()
            if answer != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд")
        return result
    
    return wrapper


def create_cacher():

    cache = {}

    def cache_result(key, value_func):
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
