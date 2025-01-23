import time

from abstract import AbstractTest
from cassandra.cluster import Cluster
from progress.bar import Bar


class ScyllaTest(AbstractTest):
    name = 'Scylla'
    def __init__(self, data):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect()
        # Полная очистка БД
        self.session.execute("DROP KEYSPACE IF EXISTS test;")
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        self.session.set_keyspace('test')
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS data (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        self.data = data

    def write(self) -> float:
        """Тестирование записи в Scylla"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.session.execute("INSERT INTO test.data (key, value) VALUES (%s, %s)", (key, value))
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration

    def read(self) -> float:
        """Тестирование чтения из Scylla"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.session.execute("SELECT * FROM test.data WHERE key = %s", (key,)).one()
            assert result.value == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
