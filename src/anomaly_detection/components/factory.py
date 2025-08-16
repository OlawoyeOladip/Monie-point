# cleaner/factory.py
from typing import Dict, Type
from cleaner.base import BaseCleaner
from cleaner.exceptions import CleanerNotFoundError

class TransactionCleanerFactory:
    """
    Factory for cleaners. 
    Register concrete cleaners with @register or register() method.

    Usage:
        @TransactionCleanerFactory.register("synthetic")
        class SyntheticCleaner(BaseCleaner): ...
        
        cleaner = TransactionCleanerFactory.get_cleaner(
            "synthetic", file_path="data.csv"
        ) 
    """
    _registry: Dict[str, Type[BaseCleaner]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Decorator to register a cleaner class under 'name'.
        """
        def _decorator(cleaner_cls: Type[BaseCleaner]):
            cls._registry[name] = cleaner_cls
            return cleaner_cls
        return _decorator

    @classmethod
    def get_cleaner(cls, name: str, *args, **kwargs) -> BaseCleaner:
        """
        Instantiate and return a registered cleaner.

        :param name: key used when registering the cleaner
        :param args, kwargs: passed to cleaner constructor
        :raises CleanerNotFoundError if not registered
        """
        if name not in cls._registry:
            raise CleanerNotFoundError(
                f"Cleaner '{name}' not found. Registered keys: {list(cls._registry.keys())}"
            )
        cleaner_cls = cls._registry[name]
        return cleaner_cls(*args, **kwargs)
