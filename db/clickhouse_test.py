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