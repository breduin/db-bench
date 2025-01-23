import time
import tarantool

from abstract import AbstractTest
from progress.bar import Bar


class TarantoolTest(AbstractTest):
    def __init__(self, data):
        self.data = data
        conn = tarantool.connect('localhost', 3301)
        self.space = conn.space('test')

    def write(self) -> float:
        """Тестирование записи в Tarantool"""
        bar = Bar('Tarantool|запись', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            self.space.upsert((key, value), [('=', 0, key)]) 
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд\n')
        return duration
    
    def read(self) -> float:
        """Тестирование чтения из Tarantool"""
        bar = Bar('Tarantool|чтение', max=len(self.data))
        start_time = time.time()
        for key, value in self.data.items():
            result = self.space.select(key)
            assert result[0][1] == value
            bar.next()
        duration = time.time() - start_time
        print(f' {duration:.2f} секунд\n')
        return duration
