import os

base = os.path.dirname(os.path.abspath(__file__))

# Папки, которые нужно создать
directories = [
    'config',
    'data/raw',
    'data/emiss_db',
    'notebooks',
    'src/db',
    'src/llm',
    'src/utils',
    'tests',
    'scripts',
    'outputs',
    'logs',
]

# Файлы, которые нужно создать (пустые)
files = [
    '.gitignore',
    'README.md',
    'requirements.txt',
    'setup.py',
    'config/settings.yaml',
    'src/__init__.py',
    'src/db/__init__.py',
    'src/db/connection.py',
    'src/db/loader.py',
    'src/db/queries.py',
    'src/llm/__init__.py',
    'src/llm/client.py',
    'src/llm/prompts.py',
    'src/llm/generators.py',
    'src/utils/__init__.py',
    'src/utils/file_helpers.py',
    'src/utils/logging_config.py',
    'src/main.py',
    'scripts/import_emiss.py',
    'tests/__init__.py',
    'tests/test_db.py',
    'tests/test_llm.py',
    'notebooks/exploratory.ipynb',
]

# Создаём папки
for d in directories:
    path = os.path.join(base, d)
    os.makedirs(path, exist_ok=True)
    print(f'Создана папка: {path}')

# Создаём файлы (пустые)
for f in files:
    path = os.path.join(base, f)
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8') as fp:
            fp.write('')
        print(f'Создан файл: {path}')
    else:
        print(f'Файл уже существует: {path}')

print('\nГотово! Теперь структура проекта создана.')