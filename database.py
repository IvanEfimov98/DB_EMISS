import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Table, Column, String, Boolean, DateTime, Integer, Float, inspect
import pandas as pd
from datetime import datetime

engine = None

def init_db(db_url='sqlite:///fedstat_data.db'):
    global engine
    engine = create_engine(db_url, echo=False)

def create_indicators_table():
    """Создаёт таблицу со списком индикаторов, если её нет."""
    if not inspect(engine).has_table('indicators'):
        metadata = MetaData()
        Table('indicators', metadata,
              Column('id', String, primary_key=True),
              Column('url', String),
              Column('name', String),
              Column('excluded', Boolean, default=False),
              Column('department', String),
              Column('group_level_2', String),
              Column('group_level_3', String),
              Column('group_level_4', String),
              Column('group_level_5', String),
              Column('group_level_6', String),
              Column('date_of_update', DateTime)
        )
        metadata.create_all(engine)

def save_indicators(df):
    """Сохраняет или обновляет данные об индикаторах в БД."""
    with engine.connect() as conn:
        for _, row in df.iterrows():
            # Проверяем, существует ли индикатор
            existing = conn.execute(
                sa.text("SELECT 1 FROM indicators WHERE id = :id"),
                {"id": row['id']}
            ).fetchone()
            if existing:
                # Обновляем существующую запись
                conn.execute(
                    sa.text("""
                        UPDATE indicators
                        SET url = :url, name = :name, excluded = :excluded,
                            department = :department, group_level_2 = :group_level_2,
                            group_level_3 = :group_level_3, group_level_4 = :group_level_4,
                            group_level_5 = :group_level_5, group_level_6 = :group_level_6,
                            date_of_update = :date_of_update
                        WHERE id = :id
                    """),
                    {
                        'id': row['id'],
                        'url': row['url'],
                        'name': row['name'],
                        'excluded': row['excluded'],
                        'department': row['department'],
                        'group_level_2': row['group_level_2'],
                        'group_level_3': row['group_level_3'],
                        'group_level_4': row['group_level_4'],
                        'group_level_5': row['group_level_5'],
                        'group_level_6': row['group_level_6'],
                        'date_of_update': row['date_of_update']
                    }
                )
            else:
                # Вставляем новую запись
                conn.execute(
                    sa.text("""
                        INSERT INTO indicators
                        (id, url, name, excluded, department, group_level_2,
                         group_level_3, group_level_4, group_level_5, group_level_6, date_of_update)
                        VALUES (:id, :url, :name, :excluded, :department, :group_level_2,
                                :group_level_3, :group_level_4, :group_level_5, :group_level_6, :date_of_update)
                    """),
                    {
                        'id': row['id'],
                        'url': row['url'],
                        'name': row['name'],
                        'excluded': row['excluded'],
                        'department': row['department'],
                        'group_level_2': row['group_level_2'],
                        'group_level_3': row['group_level_3'],
                        'group_level_4': row['group_level_4'],
                        'group_level_5': row['group_level_5'],
                        'group_level_6': row['group_level_6'],
                        'date_of_update': row['date_of_update']
                    }
                )
        conn.commit()

def create_indicator_table(indicator_id):
    """Создаёт таблицу для хранения данных конкретного индикатора, если её нет."""
    table_name = f"indicator_{indicator_id}"
    if not inspect(engine).has_table(table_name):
        metadata = MetaData()
        # Мы не знаем структуру заранее, поэтому создадим таблицу без колонок, а при вставке pandas сам создаст
        # Но для надёжности можно создать пустую таблицу, а потом использовать if_exists='replace'
        pass
    # Вместо этого будем использовать pandas to_sql с if_exists='replace' для полной перезаписи
    # Но для инкрементальной загрузки нужно будет проверять существование
    # Пока оставим как есть: при сохранении данных будем вызывать pd.to_sql с if_exists='replace'

def save_indicator_data(indicator_id, df):
    """Сохраняет данные индикатора в БД (заменяет старые)."""
    table_name = f"indicator_{indicator_id}"
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)

def get_indicators(excluded=False):
    """Возвращает DataFrame со списком индикаторов (по умолчанию не исключённых)."""
    with engine.connect() as conn:
        query = "SELECT * FROM indicators"
        if not excluded:
            query += " WHERE excluded = 0"
        df = pd.read_sql_query(query, conn)
    return df