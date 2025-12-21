from abc import ABC, abstractmethod
from typing import Optional, Any
import pandas as pd

class BaseProvider(ABC):
    """
    Abstract base class for data providers.
    """
    
    @abstractmethod
    def query(self, api_name: str, fields: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
        """
        Execute a generic query against the provider.
        
        Args:
            api_name: Name of the API/table to query (e.g. 'daily', 'adj_factor').
            fields: Comma-separated list of fields to return.
            **kwargs: Query parameters.
            
        Returns:
            pd.DataFrame: The result data.
        """
        pass
