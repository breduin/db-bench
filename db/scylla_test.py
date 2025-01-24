import time

from abstract import AbstractTest
from cassandra.cluster import Cluster
from progress.bar import Bar
from cassandra.query import BatchStatement


class ScyllaTest(AbstractTest):
    name = "Scylla"

    def __init__(self, data):
        self.cluster = Cluster(["localhost"])
        self.session = self.cluster.connect()
        # Полная очистка БД
        self.session.execute("DROP KEYSPACE IF EXISTS test;")
        self.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
        )
        self.session.set_keyspace("test")
        self.session.execute(
            """
        CREATE TABLE IF NOT EXISTS data (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
        )
        self.data = data

    def write(self) -> float:
        """Тестирование записи в Scylla"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.session.execute(
                "INSERT INTO test.data (key, value) VALUES (%s, %s)", (key, value)
            )
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def write_optimized(self) -> float:
        """Тестирование записи в Scylla с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        # Подготовка данных для пакетной вставки
        data_to_insert = [
            {"key": key, "value": value} for key, value in self.data.items()
        ]
        batch_size = 500  # Максимальный размер пакета
        for i in range(0, len(data_to_insert), batch_size):
            batch = BatchStatement()  # Создание экземпляра BatchStatement
            for d in data_to_insert[i : i + batch_size]:
                batch.add(
                    "INSERT INTO data (key, value) VALUES (%s, %s)",
                    (d["key"], d["value"]),
                )
                bar.next()
            self.session.execute(batch)  # Выполнение пакетного запроса
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def read(self) -> float:
        """Тестирование чтения из Scylla"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.session.execute(
                "SELECT * FROM test.data WHERE key = %s", (key,)
            ).one()
            assert result.value == value
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def read_optimized(self) -> float:
        """Тестирование чтения из Scylla с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        keys = list(self.data.keys())
        batch_size = 100  # Максимальный размер пакета
        result_dict = {}

        for i in range(0, len(keys), batch_size):
            query = "SELECT * FROM test.data WHERE key IN ({})".format(
                ",".join(["%s"] * min(batch_size, len(keys) - i))
            )
            result = self.session.execute(query, keys[i : i + batch_size]).all()
            if result:
                result_dict.update({row.key: row.value for row in result})
            else:
                print(f"Нет результатов для ключей: {keys[i:i + batch_size]}")

        for key in keys:
            assert key in result_dict, f"Ключ {key} не найден в результатах"
            assert (
                result_dict[key] == self.data[key]
            ), f"Значение для ключа {key} не совпадает"
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration
