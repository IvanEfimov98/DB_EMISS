"""Загрузка CSV в ArcticDB."""

import pandas as pd
from .connection import DatabaseConnection

class DataLoader:
    def __init__(self):
        self.db = DatabaseConnection()
        self.lib = self.db.get_library()

    def load_csv(self, filepath: str, table_name: str, date_col: str = 'date'):
        df = pd.read_csv(filepath, parse_dates=[date_col])
        df = df.set_index(date_col)

        if table_name in self.lib.list_symbols():
            self.lib.append(table_name, df)
            print(f"Appended {len(df)} rows to '{table_name}'")
        else:
            self.lib.write(table_name, df)
            print(f"Created '{table_name}' with {len(df)} rows")