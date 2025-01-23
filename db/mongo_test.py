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

    def write_optimized(self) -> float:
        """Тестирование записи в Mongo с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=1)
        start_time = time.time()
        for _ in range(1):
            self.client.test_db.data.insert_many([{'key': key, 'value': value} for key, value in self.data.items()])
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

    def read_optimized(self) -> float:
        """Тестирование чтения из Mongo с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        keys = list(self.data.keys())
        result = self.client.test_db.data.find({'key': {'$in': keys}})
        result_dict = {item['key']: item['value'] for item in result}
        for key in keys:
            assert result_dict.get(key) == self.data[key]
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
