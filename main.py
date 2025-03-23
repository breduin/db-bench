import random
import string
import sys

from db import (
    ClickhouseTest,
    GarnetTest,
    KeydbTest,
    MongoTest,
    PostgresqlTest,
    RedisTest,
    ScyllaTest,
    MariaDBTest,
)


# Количество записей
NUM_RECORDS = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000  
# Длина записи
VALUE_LENGTH = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000  


def generate_data():
    """Генерация данных для записи в базы данных"""
    return {
        f"key_{i}": "".join(
            random.choices(string.ascii_letters + string.digits, k=VALUE_LENGTH)
        )
        for i in range(NUM_RECORDS)
    }


def main():
    """Запуск тестирования"""
    data = generate_data()

    databases = {
        "clickhouse": ClickhouseTest(data),
        "garnet": GarnetTest(data),
        "keydb": KeydbTest(data),
        "mongo": MongoTest(data),
        "postgresql": PostgresqlTest(data),
        "redis": RedisTest(data),
        "scylla": ScyllaTest(data),
        "mariadb": MariaDBTest(data),
    }

    write_results = {}
    read_results = {}

    write_optimized_results = {}
    read_optimized_results = {}

    # Тесты на неоптимизированную запись
    print("-" * 50)
    print("Неоптимизированные тесты")
    print("-" * 50)
    for name, client in databases.items():
        try:
            write_results[name] = client.write()
        except Exception as e:
            print(f"Ошибка записи в {name}: {e}")
            write_results[name] = float("inf")
            continue

    # Тесты на неоптимизированное чтение
    for name, client in databases.items():
        try:
            read_results[name] = client.read()
        except Exception as e:
            print(f"Ошибка чтения из {name}: {e}")
            read_results[name] = float("inf")
            continue


    # Очистка баз данных
    for name, client in databases.items():
        client.clear()


    print("-" * 50)
    print("Тесты с пакетным запросом")
    print("-" * 50)


    # Тесты на оптимизированную запись
    for name, client in databases.items():
        try:
            write_optimized_results[name] = client.write_optimized()
        except Exception as e:
            print(f"Ошибка записи в {name}: {e}")
            write_optimized_results[name] = float("inf")
            continue

    # Тесты на оптимизированное чтение
    for name, client in databases.items():
        try:
            read_optimized_results[name] = client.read_optimized()
        except Exception as e:
            print(f"Ошибка чтения из {name}: {e}")
            read_optimized_results[name] = float("inf")
            continue

    print_results(
        write_results, read_results, write_optimized_results, read_optimized_results
    )


def print_results(
    write_results, read_results, write_optimized_results, read_optimized_results
):
    # Находим минимальные значения
    min_write_time = min(write_results.values())
    min_read_time = min(read_results.values())

    min_write_optimized_time = min(write_optimized_results.values())
    min_read_optimized_time = min(read_optimized_results.values())

    # Сортируем результаты по времени
    sorted_write_results = dict(sorted(write_results.items(), key=lambda item: item[1]))
    sorted_read_results = dict(sorted(read_results.items(), key=lambda item: item[1]))

    sorted_write_optimized_results = dict(
        sorted(write_optimized_results.items(), key=lambda item: item[1])
    )
    sorted_read_optimized_results = dict(
        sorted(read_optimized_results.items(), key=lambda item: item[1])
    )

    print("\nКоличество записей:", NUM_RECORDS)
    print("Длина одной записи:", VALUE_LENGTH)
    print("--------------------------------")
    print("Результаты тестирования записи (неоптимизированно):")
    print(f"{'База данных':<15} {'Время (секунд)':<20} {'Статус'}")
    print("-" * 50)
    for db, time in sorted_write_results.items():
        if time == min_write_time:
            print(f"{db:<15} {time:.2f} {'(самая быстрая)'}")
        else:
            percent_slower = ((time - min_write_time) / min_write_time) * 100
            print(f"{db:<15} {time:.2f} ({percent_slower:.2f}% медленнее)")

    print("\nРезультаты тестирования чтения (неоптимизированно):")
    print(f"{'База данных':<15} {'Время (секунд)':<20} {'Статус'}")
    print("-" * 50)
    for db, time in sorted_read_results.items():
        if time == min_read_time:
            print(f"{db:<15} {time:.2f} {'(самая быстрая)'}")
        else:
            percent_slower = ((time - min_read_time) / min_read_time) * 100
            print(f"{db:<15} {time:.2f} ({percent_slower:.2f}% медленнее)")

    print("\nРезультаты тестирования записи (с пакетным запросом):")
    print(f"{'База данных':<15} {'Время (секунд)':<20} {'Статус'}")
    print("-" * 50)
    for db, time in sorted_write_optimized_results.items():
        if time == min_write_optimized_time:
            print(f"{db:<15} {time:.2f} {'(самая быстрая)'}")
        else:
            percent_slower = (
                (time - min_write_optimized_time) / min_write_optimized_time
            ) * 100
            print(f"{db:<15} {time:.2f} ({percent_slower:.2f}% медленнее)")

    print("\nРезультаты тестирования чтения (с пакетным запросом):")
    print(f"{'База данных':<15} {'Время (секунд)':<20} {'Статус'}")
    print("-" * 50)
    for db, time in sorted_read_optimized_results.items():
        if time == min_read_optimized_time:
            print(f"{db:<15} {time:.2f} {'(самая быстрая)'}")
        else:
            percent_slower = (
                (time - min_read_optimized_time) / min_read_optimized_time
            ) * 100
            print(f"{db:<15} {time:.2f} ({percent_slower:.2f}% медленнее)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Тестирование прервано пользователем")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
