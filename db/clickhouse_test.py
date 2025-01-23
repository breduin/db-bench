import time
from clickhouse_driver import Client

from abstract import AbstractTest
from progress.bar import Bar


class ClickhouseTest(AbstractTest):
    name = 'Clickhouse'
    def __init__(self, data):
        self.data = data
        self.client = Client(host='localhost', port=9000)
        self.client.execute("DROP TABLE IF EXISTS data")
        self.client.execute("""
            CREATE TABLE IF NOT EXISTS data (
                key String,
                value String
            ) ENGINE = MergeTree()
            ORDER BY key
        """)

    def write(self) -> float:
        """Тестирование записи в Clickhouse"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.client.execute("INSERT INTO data (key, value) VALUES", [(key, value)])
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration

    def write_optimized(self) -> float:
        """Тестирование записи в Clickhouse с использованием bulk_insert_with_concurrency"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=1)
        start_time = time.time()
        for _ in range(1):
            self.client.execute("INSERT INTO data (key, value) VALUES", [(key, value) for key, value in self.data.items()], settings={"max_block_size": 100000})
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration

    def read(self) -> float:
        """Тестирование чтения из Clickhouse"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.client.execute(f"SELECT value FROM data WHERE key = '{key}'")
            assert result[0][0] == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration

    def read_optimized(self) -> float:
        """Тестирование чтения из Clickhouse с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        
        # Подготовка ключей для пакетного запроса
        keys = list(self.data.keys())
        keys_placeholder = ', '.join(f"'{key}'" for key in keys)
        
        # Выполнение пакетного запроса
        result = self.client.execute(f"SELECT key, value FROM data WHERE key IN ({keys_placeholder})")
        
        # Проверка результатов
        result_dict = {row[0]: row[1] for row in result}
        for key in keys:
            assert result_dict[key] == self.data[key]
            bar.next()
        
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
