from abc import ABC, abstractmethod


class BaseCollector(ABC):
    """
    All collectors must implement fetch() and normalize()
    """

    @abstractmethod
    def fetch(self):
        """Fetch raw data from API"""
        pass

    @abstractmethod
    def normalize(self, raw):
        """Convert raw data into standard event format"""
        pass

    def run(self):
        raw_data = self.fetch()
        return [self.normalize(item) for item in raw_data]