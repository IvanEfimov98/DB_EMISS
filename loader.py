import pandas as pd
import logging
from fedstat_api import get_data_ids, post_data_ids_filtered, parse_sdmx_to_table
from database import save_indicator_data, get_indicators

logger = logging.getLogger(__name__)

def load_indicator_all_data(indicator_id, force=False):
    """
    Загружает все данные для индикатора (фильтр '*' для всех полей, кроме самого индикатора).
    force=True – принудительно перезаписывает данные, иначе проверяет, есть ли уже таблица.
    """
    # Проверяем, не загружен ли уже
    if not force:
        # Проверим существование таблицы через БД
        from database import engine, inspect
        if engine and inspect(engine).has_table(f"indicator_{indicator_id}"):
            logger.info(f"Индикатор {indicator_id} уже загружен. Пропускаем (используйте force=True для перезаписи).")
            return

    logger.info(f"Загрузка данных для индикатора {indicator_id}...")
    try:
        data_ids = get_data_ids(indicator_id)
    except Exception as e:
        logger.error(f"Ошибка получения data_ids для {indicator_id}: {e}")
        return

    # Строим фильтры: для каждого поля, кроме "Показатель", ставим "*"
    filters = {}
    for _, row in data_ids.iterrows():
        fid = row['filter_field_id']
        if fid == '0':
            continue
        filters[row['filter_field_title']] = '*'

    # Фильтруем data_ids
    filtered_ids = filter_data_ids(data_ids, filters)

    if filtered_ids.empty:
        logger.warning(f"После фильтрации для индикатора {indicator_id} не осталось данных.")
        return

    # Загружаем данные
    try:
        raw = post_data_ids_filtered(filtered_ids)
        df = parse_sdmx_to_table(raw)
    except Exception as e:
        logger.error(f"Ошибка загрузки/парсинга данных для {indicator_id}: {e}")
        return

    # Сохраняем в БД
    save_indicator_data(indicator_id, df)
    logger.info(f"Индикатор {indicator_id} загружен: {len(df)} записей.")

def filter_data_ids(data_ids, filters):
    """
    Фильтрует data_ids по словарю filters.
    filters: {'Название поля': ['значение1', ...] или '*'}
    Если значение '*', выбираются все значения для этого поля.
    """
    df = data_ids.copy()
    for field_title, values in filters.items():
        if values == '*':
            # Не фильтруем по этому полю – оставляем все строки
            continue
        if not isinstance(values, list):
            values = [values]
        # Нормализуем названия полей и значений (удаляем лишние пробелы, приводим к нижнему регистру)
        norm_field = field_title.strip().lower()
        mask = df['filter_field_title'].str.strip().str.lower() == norm_field
        if not mask.any():
            # Поле не найдено – пропускаем, возможно, оно не используется в этом индикаторе
            continue
        # Для найденных строк проверяем значения
        norm_values = [v.strip().lower() for v in values]
        df = df[~(mask & ~df['filter_value_title'].str.strip().str.lower().isin(norm_values))]
    return df

def load_all_indicators(force=False, max_indicators=None):
    """
    Загружает данные для всех неисключённых индикаторов.
    force – перезаписывать даже существующие.
    max_indicators – ограничение для тестирования.
    """
    indicators = get_indicators(excluded=False)
    if max_indicators:
        indicators = indicators.head(max_indicators)
    for idx, row in indicators.iterrows():
        load_indicator_all_data(row['id'], force=force)