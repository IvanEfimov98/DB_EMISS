"""Подключение к ArcticDB."""

import arcticdb as adb
import yaml
import os
from pathlib import Path

class DatabaseConnection:
    def __init__(self, config_path="config/settings.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.uri = self.config['db']['uri']
        self.library_name = self.config['db']['library']
        self.ac = adb.Arctic(self.uri)
        self._ensure_library()

    def _ensure_library(self):
        from arcticdb import LibraryOptions
        if self.library_name not in self.ac.list_libraries():
            self.ac.create_library(
                self.library_name,
                library_options=LibraryOptions(
                    dynamic_schema=self.config['db']['options']['dynamic_schema']
                )
            )
        self.lib = self.ac[self.library_name]

    def get_library(self):
        return self.lib

    def list_symbols(self):
        return self.lib.list_symbols()