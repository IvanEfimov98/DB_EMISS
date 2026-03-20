"""Настройка логирования."""

import logging
import sys
import yaml
from pathlib import Path

def setup_logging(config_path="config/settings.yaml"):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    level = getattr(logging, config['logging']['level'].upper(), logging.INFO)
    log_file = config['logging']['file']
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )