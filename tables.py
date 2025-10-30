"""
Tables Module
Handles loading CSV files as dataframe attributes
"""

import pandas as pd
from pathlib import Path


class Tables:
    """Class to dynamically load CSV files as dataframe attributes"""
    
    def __init__(self):
        pass

    def create_dynamic_tables(self, folder_path):
        """
        Load all CSV files from the folder as dataframe attributes
        
        Args:
            folder_path (Path): Path to the folder containing CSV files
        """
        for file_path in folder_path.glob("*.csv"):
            file_name = file_path.name
            var = file_name.split('.')[0]
            attr_name = var
            setattr(self, attr_name, pd.read_csv(str(folder_path) + f"/{file_name}"))
    
    def get_table_list(self):
        """
        Get list of all loaded table names
        
        Returns:
            list: List of table names
        """
        return list(vars(self))
