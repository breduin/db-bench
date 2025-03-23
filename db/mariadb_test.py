import mysql.connector
from progress.bar import Bar
import time

class MariaDBTest:
    def __init__(self, data):
        self.data = data
        self.name = 'MariaDB'
        self.conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='password',
            database='test'
        )
        self.cursor = self.conn.cursor()
        self.setup()

    def setup(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, value TEXT)')
        self.conn.commit()

    def cleanup(self):
        self.cursor.execute('DROP TABLE IF EXISTS test_table')
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def write_one_by_one(self, data):
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        for key, value in self.data.items():
            key_id = int(key.split('_')[1])
            self.cursor.execute('INSERT INTO test_table (id, value) VALUES (%s, %s)', (key_id, value))
            bar.next()
        self.conn.commit()
        bar.finish()

    def read_one_by_one(self, data):
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        for key in self.data.keys():
            key_id = int(key.split('_')[1])
            self.cursor.execute('SELECT value FROM test_table WHERE id = %s', (key_id,))
            result = self.cursor.fetchone()
            assert result[0] == self.data[key]
            bar.next()
        bar.finish()

    def write_batch(self, data):
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=1)
        values = [(int(k.split('_')[1]), v) for k, v in self.data.items()]
        self.cursor.executemany('INSERT INTO test_table (id, value) VALUES (%s, %s)', values)
        self.conn.commit()
        bar.next()
        bar.finish()

    def read_batch(self, data):
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        self.cursor.execute('SELECT id, value FROM test_table ORDER BY id')
        results = self.cursor.fetchall()
        for result in results:
            key = f'key_{result[0]}'
            assert result[1] == self.data[key]
            bar.next()
        bar.finish()

    def write(self):
        start_time = time.time()
        self.cursor.execute('TRUNCATE TABLE test_table')  # Очищаем таблицу
        self.conn.commit()
        self.write_one_by_one(self.data)
        return time.time() - start_time

    def read(self):
        start_time = time.time()
        self.read_one_by_one(self.data)
        return time.time() - start_time

    def write_optimized(self):
        start_time = time.time()
        self.cursor.execute('TRUNCATE TABLE test_table')  # Очищаем таблицу
        self.conn.commit()
        self.write_batch(self.data)
        return time.time() - start_time

    def read_optimized(self):
        start_time = time.time()
        self.read_batch(self.data)
        return time.time() - start_time

    def clear(self):
        self.cleanup()
        self.conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='password',
            database='test'
        )
        self.cursor = self.conn.cursor()
        self.setup() 