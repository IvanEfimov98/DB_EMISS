"""Точка входа."""

import argparse
import logging
from src.utils.logging_config import setup_logging
from src.db.connection import DatabaseConnection
from src.llm.generators import WorkGenerator

def main():
    parser = argparse.ArgumentParser(description="Генерация научных работ")
    parser.add_argument('--type', choices=['article', 'thesis', 'coursework'], required=True)
    parser.add_argument('--title', required=True)
    parser.add_argument('--notes', default='')
    parser.add_argument('--output', default='outputs/result.docx')
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(f"Запуск: {args.type} на тему '{args.title}'")

    try:
        db = DatabaseConnection()
        logger.info(f"Подключено к ArcticDB, библиотека: {db.library_name}")
    except Exception as e:
        logger.error(f"Ошибка БД: {e}")
        return

    gen = WorkGenerator()
    plan = gen.generate_plan(args.type, args.title, args.notes)
    logger.info(f"План:\n{plan}")

if __name__ == '__main__':
    main()