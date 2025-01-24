import time
import redis

from abstract import AbstractTest
from progress.bar import Bar


class GarnetTest(AbstractTest):
    name = "Garnet"

    def __init__(self, data):
        self.data = data
        self.r = redis.Redis(host="localhost", port=16379)
        self.r.flushall()  # Очистка всех данных в Garnet

    def write(self) -> float:
        """Тестирование записи в Garnet"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.r.set(key, value)
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def write_optimized(self) -> float:
        """Тестирование записи в Garnet с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"запись":<10}', max=1)
        start_time = time.time()
        for _ in range(1):
            keys = list(self.data.keys())
            # Создаем словарь для MSET
            mapping = {key: self.data[key] for key in keys}
            self.r.mset(mapping)
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration

    def read(self) -> float:
        """Тестирование чтения из Garnet"""
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
        """Тестирование чтения из Garnet с использованием пакетного запроса"""
        bar = Bar(f'{self.name:<15} | {"чтение":<10}', max=len(self.data))
        start_time = time.time()
        keys = list(self.data.keys())
        # Используем MGET для получения значений по множеству ключей
        result = self.r.mget(keys)
        result_dict = {
            key: value.decode() for key, value in zip(keys, result) if value is not None
        }
        for key in keys:
            assert result_dict[key] == self.data[key]
            bar.next()
        duration = time.time() - start_time
        print(f" {duration:.2f} секунд")
        return duration
