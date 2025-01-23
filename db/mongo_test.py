import time
from clickhouse_driver import Client
from pymongo import MongoClient

from abstract import AbstractTest
from progress.bar import Bar


class MongoTest(AbstractTest):
    def __init__(self, data):
        self.data = data
        self.client = MongoClient('localhost', 27017)
        self.client.test_db.data.drop()
        self.client.test_db.data.insert_many([{'key': key, 'value': value} for key, value in self.data.items()])

    def write(self) -> float:
        """Тестирование записи в Mongo"""
        bar = Bar('Mongo|запись', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.client.test_db.data.insert_one({'key': key, 'value': value})
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд\n')
        return duration

    def read(self) -> float:
        """Тестирование чтения из Mongo"""
        bar = Bar('Mongo|чтение', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.client.test_db.data.find_one({'key': key})
            assert result['value'] == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд\n')
        return duration
