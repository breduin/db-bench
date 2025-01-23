from abc import ABC, abstractmethod


class AbstractTest(ABC):
    @abstractmethod
    def write(self):
        raise NotImplementedError

    @abstractmethod
    def read(self):
        raise NotImplementedError

    @abstractmethod
    def write_optimized(self):
        raise NotImplementedError

    @abstractmethod
    def read_optimized(self):
        raise NotImplementedError
