from abc import ABC, abstractmethod


class AbstractTest(ABC):
    @abstractmethod
    def write(self):
        raise NotImplementedError

    @abstractmethod
    def read(self):
        raise NotImplementedError
