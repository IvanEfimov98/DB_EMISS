# DB_EMISS — система автоматической генерации научных работ на основе данных ЕМИСС и DeepSeek

Проект позволяет создавать научные статьи, курсовые, диссертации и другие работы с использованием данных из базы статистических показателей (ЕМИСС) и API DeepSeek.

## Структура

- `config/` — конфигурационные файлы
- `data/` — локальное хранилище ArcticDB и сырые CSV
- `src/` — исходный код
- `scripts/` — вспомогательные скрипты
- `tests/` — тесты
- `notebooks/` — Jupyter notebooks для экспериментов

## Быстрый старт

1. Установите зависимости: pip install -r requirements.txt
2. Настройте `config/settings.yaml` (укажите API ключ DeepSeek)
3. Загрузите данные в ArcticDB с помощью `scripts/import_emiss.py`
4. Запустите генерацию через `src/main.py`

## Контакты

Автор: Иван Ефимов  
GitHub: https://github.com/IvanEfimov98/DB_EMISS