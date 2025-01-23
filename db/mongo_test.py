import time
from clickhouse_driver import Client
from pymongo import MongoClient

from abstract import AbstractTest
from progress.bar import Bar


class MongoTest(AbstractTest):
    name = 'Mongo'
    def __init__(self, data):
        self.data = data
        self.client = MongoClient('localhost', 27017)
        self.client.test_db.data.drop()
        self.client.test_db.data.insert_many([{'key': key, 'value': value} for key, value in self.data.items()])

    def write(self) -> float:
        """Тестирование записи в Mongo"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.client.test_db.data.insert_one({'key': key, 'value': value})
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration

    def read(self) -> float:
        """Тестирование чтения из Mongo"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.client.test_db.data.find_one({'key': key})
            assert result['value'] == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
