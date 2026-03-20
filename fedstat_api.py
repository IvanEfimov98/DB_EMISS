import re
import requests
import demjson3
from bs4 import BeautifulSoup
import pandas as pd
import pandasdmx as sdmx
from io import BytesIO
from utils import retry

BASE_URL = "https://www.fedstat.ru"

@retry(max_tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.RequestException,))
def get_data_ids(indicator_id):
    """
    Получить таблицу фильтров для индикатора.
    Возвращает pandas.DataFrame с колонками:
    filter_field_id, filter_field_title, filter_value_id, filter_value_title, filter_field_object_ids
    """
    url = f"{BASE_URL}/indicator/{indicator_id}"
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    # Ищем скрипт, содержащий "filters: {" и "left_columns: ["
    scripts = soup.find_all('script')
    js_script = None
    for script in scripts:
        if script.string and 'filters: {' in script.string and 'left_columns: [' in script.string:
            js_script = script.string
            break
    if not js_script:
        raise ValueError("Не удалось найти скрипт с фильтрами на странице индикатора")

    # Извлекаем строки между "filters: {" и "left_columns: ["
    lines = js_script.split('\n')
    start = None
    end = None
    for i, line in enumerate(lines):
        if 'filters: {' in line:
            start = i
        if start is not None and 'left_columns: [' in line:
            end = i
            break
    if start is None or end is None:
        raise ValueError("Не удалось найти фильтры в скрипте")
    filter_lines = lines[start:end]

    # Собираем строку, подготавливаем к парсингу как JSON
    filter_text = '\n'.join(filter_lines)
    # Заменяем однострочные комментарии и удаляем trailing запятые
    filter_text = re.sub(r'//.*', '', filter_text)
    filter_text = re.sub(r',\s*}', '}', filter_text)
    filter_text = re.sub(r',\s*]', ']', filter_text)
    # Добавляем фигурные скобки для объекта
    filter_text = "{" + filter_text.split('{', 1)[1].rsplit('}', 1)[0] + "}"
    # Парсим через demjson3
    filters_obj = demjson3.decode(filter_text)

    # Построение таблицы фильтров
    data = []
    for i, (filter_key, filter_val) in enumerate(filters_obj.items()):
        if filter_key != 'filters':
            continue
        filters_list = filter_val
        for f in filters_list:
            field_id = str(f.get('id', ''))
            field_title = f.get('title', '')
            values = f.get('values', {})
            for val_id, val_info in values.items():
                val_title = val_info.get('title', '')
                data.append([field_id, field_title, val_id, val_title, ''])

    # Поиск object_ids из другого скрипта
    object_text = '\n'.join(lines[end:])
    object_text = re.sub(r'//.*', '', object_text)
    object_text = re.sub(r',\s*}', '}', object_text)
    object_text = re.sub(r',\s*]', ']', object_text)
    object_text = "{" + object_text.split('{', 1)[1].rsplit('}', 1)[0] + "}"
    obj = demjson3.decode(object_text)

    # Извлечение lineObjectIds, columnObjectIds, filterObjectIds
    object_mapping = {}
    for key in ['left_columns', 'top_columns', 'groups']:
        if key in obj:
            for item in obj[key]:
                if isinstance(item, dict) and 'id' in item:
                    object_mapping[item['id']] = key
    # filterObjectIds обычно находится в groups
    # Добавим специальный фильтр для индикатора (id=0)
    object_mapping['0'] = 'filterObjectIds'

    # Присваиваем filter_field_object_ids на основе object_mapping
    df = pd.DataFrame(data, columns=['filter_field_id', 'filter_field_title',
                                     'filter_value_id', 'filter_value_title',
                                     'filter_field_object_ids'])
    # Уникальные filter_field_id
    for fid in df['filter_field_id'].unique():
        if fid in object_mapping:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = object_mapping[fid]
        else:
            # По умолчанию lineObjectIds
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = 'lineObjectIds'

    return df

@retry(max_tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.RequestException,))
def post_data_ids_filtered(data_ids, data_format='sdmx'):
    """
    Отправить POST-запрос с отфильтрованными data_ids и вернуть сырые данные (bytes).
    data_format: 'sdmx' или 'excel'
    """
    # Группируем по filter_field_id и object_ids
    unique_filters = data_ids.drop_duplicates(subset=['filter_field_id', 'filter_field_object_ids'])
    # Строим body
    body = {
        'format': data_format,
        'id': data_ids[data_ids['filter_field_id'] == '0']['filter_value_id'].iloc[0],
        'indicator_title': data_ids[data_ids['filter_field_id'] == '0']['filter_value_title'].iloc[0],
    }
    # Добавляем lineObjectIds, columnObjectIds, filterObjectIds
    for _, row in unique_filters.iterrows():
        if row['filter_field_id'] == '0':
            continue
        body[row['filter_field_object_ids']] = row['filter_field_id']

    # Добавляем выбранные значения
    selected = []
    for _, row in data_ids.iterrows():
        selected.append(f"{row['filter_field_id']}_{row['filter_value_id']}")
    body['selectedFilterIds'] = selected

    url = f"{BASE_URL}/indicator/data.do?format={data_format}"
    response = requests.post(url, data=body, timeout=180)
    response.raise_for_status()
    return response.content

def parse_sdmx_to_table(data_raw, try_to_parse_obsvalue=True):
    """
    Преобразует SDMX XML в pandas DataFrame с подстановкой названий из codelist.
    """
    # Используем pandasdmx для чтения
    with BytesIO(data_raw) as f:
        msg = sdmx.read_sdmx(f)
    # Преобразуем в pandas DataFrame (пока только ObsValue, без подстановки названий)
    df = msg.to_pandas()
    # Переименовываем колонки (убираем префикс X)
    df.columns = [re.sub(r'^X(\d+)\.', r'\1-', col) for col in df.columns]
    df.columns = [re.sub(r'^X(\d+)', r'\1', col) for col in df.columns]

    # Извлекаем CodeLists из XML напрямую, чтобы подставить названия
    # pandasdmx не всегда сохраняет codelist, поэтому парсим вручную
    import xml.etree.ElementTree as ET
    root = ET.fromstring(data_raw)
    ns = {'generic': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_1/message',
          'structure': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_1/structure'}
    # Находим все CodeList
    codelists = {}
    for codelist in root.findall('.//structure:CodeList', ns):
        cid = codelist.get('id')
        title_elem = codelist.find('structure:Name', ns)
        title = title_elem.text if title_elem is not None else cid
        codes = {}
        for code in codelist.findall('structure:Code', ns):
            code_id = code.get('value')
            code_name = code.text
            codes[code_id] = code_name
        codelists[cid] = (title, codes)

    # Для каждой колонки, которая является кодом (название совпадает с cid),
    # заменяем значения на названия
    for col in df.columns:
        if col in codelists:
            title, codes = codelists[col]
            # Добавляем новую колонку с названием
            new_col = title
            df[new_col] = df[col].map(codes)
            # Старую колонку с кодом можно переименовать, добавив суффикс _code
            df.rename(columns={col: f"{col}_code"}, inplace=True)

    # Попытка преобразовать ObsValue в числовой тип
    if try_to_parse_obsvalue and 'ObsValue' in df.columns:
        df['ObsValue'] = pd.to_numeric(df['ObsValue'], errors='coerce')
        if df['ObsValue'].isna().any():
            # Если есть нечисловые значения, возможно, они должны быть такими
            # Но мы выведем предупреждение
            import warnings
            warnings.warn("Некоторые ObsValue не удалось преобразовать в число")

    return df