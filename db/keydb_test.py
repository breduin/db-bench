import time
import redis

from abstract import AbstractTest
from progress.bar import Bar


class KeydbTest(AbstractTest):
    def __init__(self, data):
        self.data = data
        self.r = redis.Redis(host='localhost', port=6378)
        self.r.flushall()  # Очистка всех данных в KeyDB

    def write(self) -> float:
        """Тестирование записи в KeyDB"""
        bar = Bar('KeyDB|запись', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.r.set(key, value)
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд\n')
        return duration
    
    def read(self) -> float:
        """Тестирование чтения из KeyDB"""
        bar = Bar('KeyDB|чтение', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.r.get(key)
            assert result.decode() == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд\n')
        return duration
    