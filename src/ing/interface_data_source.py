import abc
import pandas as pd


class IDataSource(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'get_data') and
                callable(subclass.get_data) or
                NotImplemented)

    @abc.abstractmethod
    def get_data(self) -> pd.DataFrame:
        """Return the data set"""
        raise NotImplementedError

