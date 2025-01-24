import time
import redis

from abstract import AbstractTest
from progress.bar import Bar


class KeydbTest(AbstractTest):
    name = "Keydb"

    def __init__(self, data):
        self.data = data
        self.r = redis.Redis(host="localhost", port=6378)
        self.clear()

    def clear(self):
        self.r.flushall()

    def write(self) -> float:
        """Тестирование записи в KeyDB"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.r.set(key, value)
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def write_optimized(self) -> float:
        """Тестирование записи в KeyDB с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=1)
        start_time = time.time()
        for _ in range(1):
            self.r.mset(self.data)
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def read(self) -> float:
        """Тестирование чтения из KeyDB"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.r.get(key)
            assert result.decode() == value
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def read_optimized(self) -> float:
        """Тестирование чтения из KeyDB с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        keys = list(self.data.keys())
        result = self.r.mget(keys)
        result_dict = {key: value.decode() for key, value in zip(keys, result)}
        for key in keys:
            assert result_dict[key] == self.data[key]
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration
