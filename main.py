import random
import string

from db import (
    ClickhouseTest,
    GarnetTest,
    KeydbTest,
    MongoTest,
    PostgresqlTest,
    RedisTest,
    ScyllaTest,
)

# Параметры тестирования
NUM_RECORDS = 10_00  # Количество записей
VALUE_LENGTH = 1_000  # Длина записи


def generate_data():
    """Генерация данных для записи в базы данных"""
    return {f'key_{i}': ''.join(random.choices(string.ascii_letters + string.digits, k=VALUE_LENGTH)) for i in range(NUM_RECORDS)}


def main():
    """Запуск тестирования"""
    data = generate_data()

    databases = {
        'clickhouse': ClickhouseTest(data),
        'garnet': GarnetTest(data),
        'keydb': KeydbTest(data),
        'mongo': MongoTest(data),
        'postgresql': PostgresqlTest(data),
        'redis': RedisTest(data),
        'scylla': ScyllaTest(data),
    }

    write_results = {}
    read_results = {}

    # Тесты на запись
    for name, client in databases.items():
        try:
            write_results[name] = client.write()
        except Exception as e:
            print(f"Ошибка записи в {name}: {e}")
            exit(1)

    # Тесты на чтение
    for name, client in databases.items():
        try:
            read_results[name] = client.read()
        except Exception as e:
            print(f"Ошибка чтения из {name}: {e}")
            exit(1)

    print_results(write_results, read_results)


def print_results(write_results, read_results):
    # Находим минимальные значения
    min_write_time = min(write_results.values())
    min_read_time = min(read_results.values())

    # Сортируем результаты по времени
    sorted_write_results = dict(sorted(write_results.items(), key=lambda item: item[1]))
    sorted_read_results = dict(sorted(read_results.items(), key=lambda item: item[1]))

    print("\nКоличество записей:", NUM_RECORDS)
    print("Длина одной записи:", VALUE_LENGTH)
    print("--------------------------------")
    print("Результаты тестирования записи:")
    print(f"{'База данных':<15} {'Время (секунд)':<20} {'Статус'}")
    print("-" * 50)
    for db, time in sorted_write_results.items():
        if time == min_write_time:
            print(f"{db:<15} {time:.2f} {'(самая быстрая)'}")
        else:
            percent_slower = ((time - min_write_time) / min_write_time) * 100
            print(f"{db:<15} {time:.2f} ({percent_slower:.2f}% медленнее)")

    print("\nРезультаты тестирования чтения:")
    print(f"{'База данных':<15} {'Время (секунд)':<20} {'Статус'}")
    print("-" * 50)
    for db, time in sorted_read_results.items():
        if time == min_read_time:
            print(f"{db:<15} {time:.2f} {'(самая быстрая)'}")
        else:
            percent_slower = ((time - min_read_time) / min_read_time) * 100
            print(f"{db:<15} {time:.2f} ({percent_slower:.2f}% медленнее)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Тестирование прервано пользователем")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
