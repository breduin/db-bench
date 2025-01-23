import time
import redis

from abstract import AbstractTest
from progress.bar import Bar


class RedisTest(AbstractTest):
    name = 'Redis'
    def __init__(self, data):
        self.data = data
        self.r = redis.Redis(host='localhost', port=6379)
        self.r.flushall()  # Очистка всех данных в Redis

    def write(self) -> float:
        """Тестирование записи в Redis"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.r.set(key, value)
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
    
    def write_optimized(self) -> float:
        """Тестирование записи в Redis с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=1)
        start_time = time.time()
        for _ in range(1):
            self.r.mset({key: value for key, value in self.data.items()})
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
    
    def read(self) -> float:
        """Тестирование чтения из Redis"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.r.get(key)
            assert result.decode() == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
    
    def read_optimized(self) -> float:
        """Тестирование чтения из Redis с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        keys = list(self.data.keys())
        result = self.r.mget(keys)
        result_dict = {key: value.decode() for key, value in zip(keys, result)}
        for key in keys:
            assert result_dict[key] == self.data[key]
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд')
        return duration
