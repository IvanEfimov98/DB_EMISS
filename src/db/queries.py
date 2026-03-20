"""Выборка данных из ArcticDB."""

import pandas as pd
from .connection import DatabaseConnection

class DataQueries:
    def __init__(self):
        self.db = DatabaseConnection()
        self.lib = self.db.get_library()

    def get_data_for_analysis(self, symbol: str, start_date=None, end_date=None, columns=None):
        query = {}
        if start_date and end_date:
            query['date_range'] = (pd.Timestamp(start_date), pd.Timestamp(end_date))
        if columns:
            query['columns'] = columns

        if query:
            data = self.lib.read(symbol, **query).data
        else:
            data = self.lib.read(symbol).data
        return data

    def get_symbols_by_prefix(self, prefix: str):
        return [s for s in self.lib.list_symbols() if s.startswith(prefix)]