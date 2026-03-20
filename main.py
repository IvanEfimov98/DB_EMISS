import argparse
import logging
from utils import setup_logging
from database import init_db
from indicator_manager import update_indicators
from loader import load_all_indicators, load_indicator_all_data

def main():
    parser = argparse.ArgumentParser(description='Управление загрузкой данных с fedstat.ru')
    parser.add_argument('--db', default='sqlite:///fedstat_data.db', help='URL базы данных (SQLite)')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Команда update_indicators
    subparsers.add_parser('update_indicators', help='Обновить список индикаторов')

    # Команда load_all
    parser_load_all = subparsers.add_parser('load_all', help='Загрузить все показатели')
    parser_load_all.add_argument('--force', action='store_true', help='Перезаписать существующие данные')
    parser_load_all.add_argument('--max', type=int, help='Максимальное количество индикаторов для загрузки')

    # Команда load_one
    parser_load_one = subparsers.add_parser('load_one', help='Загрузить один показатель')
    parser_load_one.add_argument('indicator_id', help='ID показателя')
    parser_load_one.add_argument('--force', action='store_true', help='Перезаписать существующие данные')

    args = parser.parse_args()

    setup_logging()
    init_db(args.db)

    if args.command == 'update_indicators':
        update_indicators()
    elif args.command == 'load_all':
        load_all_indicators(force=args.force, max_indicators=args.max)
    elif args.command == 'load_one':
        load_indicator_all_data(args.indicator_id, force=args.force)

if __name__ == '__main__':
    main()