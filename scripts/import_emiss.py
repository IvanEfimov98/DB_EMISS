"""Скрипт для загрузки CSV в ArcticDB."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
from db.loader import DataLoader

def main():
    loader = DataLoader()
    raw_dir = Path(__file__).parent.parent / 'data' / 'raw'
    if not raw_dir.exists():
        raw_dir.mkdir(parents=True)

    # Здесь можно добавить логику обхода файлов и вызова loader.load_csv
    # Например:
    # for csv_file in raw_dir.glob('*.csv'):
    #     loader.load_csv(str(csv_file), csv_file.stem, date_col='date')

if __name__ == '__main__':
    main()