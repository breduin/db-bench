import psycopg2
import time


from abstract import AbstractTest
from progress.bar import Bar


class PostgresqlTest(AbstractTest):
    name = 'Postgresql'
    def __init__(self, data):
        self.data = data
        self.client = psycopg2.connect(host='localhost', port=5432, database='test', user='postgres', password='postgres')
        with self.client.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS data")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data (
                    key VARCHAR,
                    value VARCHAR
                )
            """)

    def write(self) -> float:
        """Тестирование записи в Postgresql"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        with self.client.cursor() as cursor:
            for key, value in self.data.items():
                cursor.execute("INSERT INTO data (key, value) VALUES (%s, %s)", (key, value))
                bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration

    def read(self) -> float:
        """Тестирование чтения из Postgresql"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        with self.client.cursor() as cursor:
            for key, value in self.data.items():
                cursor.execute("SELECT value FROM data WHERE key = %s", (key,))
                result = cursor.fetchall()
                assert result[0][0] == value
                bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
