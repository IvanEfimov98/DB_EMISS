import re
import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from utils import retry
import demjson3
import os

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fedstat.ru"
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "ru,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "Referer": BASE_URL,
})
session.get(BASE_URL, timeout=30)

os.makedirs("logs", exist_ok=True)

def _normalize_js_string(s):
    s = re.sub(r'//.*', '', s)
    s = re.sub(r',\s*}', '}', s)
    s = re.sub(r',\s*]', ']', s)
    return s

def _extract_balanced_brace(text, start_pos):
    """Извлекает сбалансированный блок в фигурных скобках, начиная с позиции start_pos."""
    brace_start = text.find('{', start_pos)
    if brace_start == -1:
        return None
    balance = 1
    i = brace_start + 1
    while balance > 0 and i < len(text):
        if text[i] == '{':
            balance += 1
        elif text[i] == '}':
            balance -= 1
        i += 1
    if balance != 0:
        return None
    return text[brace_start:i]

def _extract_array(text, key):
    """Извлекает массив вида key: [ ... ]."""
    pos = text.find(f'{key}:')
    if pos == -1:
        return []
    bracket_start = text.find('[', pos)
    if bracket_start == -1:
        return []
    balance = 1
    i = bracket_start + 1
    while balance > 0 and i < len(text):
        if text[i] == '[':
            balance += 1
        elif text[i] == ']':
            balance -= 1
        i += 1
    if balance != 0:
        return []
    arr_str = text[bracket_start:i]
    arr_str = _normalize_js_string(arr_str)
    try:
        arr = demjson3.decode(arr_str)
        return arr if isinstance(arr, list) else []
    except:
        return []

def parse_js1(script_text):
    """Извлекает и парсит объект filters из скрипта."""
    pos = script_text.find('filters: {')
    if pos == -1:
        raise ValueError("Не найдено 'filters: {'")
    block = _extract_balanced_brace(script_text, pos)
    if not block:
        raise ValueError("Не удалось извлечь блок filters")
    block = _normalize_js_string(block)
    try:
        data = demjson3.decode(block)
        return data  # ожидаем словарь с ключом 'filters'
    except Exception as e:
        logger.exception("Ошибка парсинга JSON в parse_js1")
        raise ValueError(f"Ошибка парсинга фильтров: {e}")

def parse_js2(script_text):
    """Извлекает left_columns, top_columns, groups."""
    result = {}
    for key in ['left_columns', 'top_columns', 'groups']:
        arr = _extract_array(script_text, key)
        result[key] = arr
    return result

@retry(max_tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.RequestException,))
def get_data_ids(indicator_id):
    url = f"{BASE_URL}/indicator/{indicator_id}"
    logger.info(f"Запрос GET: {url}")
    response = session.get(url, timeout=180)
    response.raise_for_status()
    html_path = f"logs/indicator_{indicator_id}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    logger.info(f"HTML сохранён в {html_path}")
    soup = BeautifulSoup(response.text, 'html.parser')

    scripts = soup.find_all('script')
    logger.info(f"Найдено скриптов: {len(scripts)}")
    if len(scripts) < 12:
        raise ValueError(f"Найдено только {len(scripts)} скриптов, ожидалось минимум 12")
    script_tag = scripts[11]
    if not script_tag.string:
        raise ValueError("12-й скрипт не содержит текста")
    script_text = script_tag.string
    logger.debug(f"12-й скрипт длиной {len(script_text)} символов")

    # Парсим фильтры
    filter_dict = parse_js1(script_text)
    filter_list = filter_dict.get('filters', [])  # список объектов фильтров
    if not filter_list:
        raise ValueError("Не найдено фильтров в data_ids")

    # Парсим объекты (left_columns, top_columns, groups)
    object_dict = parse_js2(script_text)

    # Формируем mapping filter_field_id -> object_type
    object_ids = {}
    for key in ['left_columns', 'top_columns', 'groups']:
        obj_type = 'lineObjectIds'
        if key == 'top_columns':
            obj_type = 'columnObjectIds'
        elif key == 'groups':
            obj_type = 'filterObjectIds'
        for item in object_dict.get(key, []):
            if isinstance(item, dict) and 'id' in item:
                object_ids[str(item['id'])] = obj_type

    # Добавляем специальный filterObjectIds для индикатора (id=0)
    object_ids['0'] = 'filterObjectIds'

    # Собираем строки data_ids
    rows = []
    for filter_group in filter_list:
        field_id = str(filter_group.get('id', ''))
        field_title = filter_group.get('title', '')
        values = filter_group.get('values', {})
        for val_id, val_info in values.items():
            val_title = val_info.get('title', '')
            rows.append([field_id, field_title, str(val_id), val_title, ''])

    if not rows:
        raise ValueError("Не найдено ни одного фильтра в data_ids")

    df = pd.DataFrame(rows, columns=['filter_field_id', 'filter_field_title',
                                     'filter_value_id', 'filter_value_title',
                                     'filter_field_object_ids'])

    # Заполняем filter_field_object_ids
    for fid in df['filter_field_id'].unique():
        if fid in object_ids:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = object_ids[fid]
        else:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = 'lineObjectIds'

    logger.info(f"Получено {len(df)} строк data_ids")
    return df

@retry(max_tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.RequestException,))
def post_data_ids_filtered(data_ids, data_format='sdmx'):
    if data_ids.empty:
        raise ValueError("data_ids пуст, невозможно отправить POST")
    unique = data_ids.drop_duplicates(subset=['filter_field_id', 'filter_field_object_ids'])
    indicator_row = data_ids[data_ids['filter_field_id'] == '0'].iloc[0]
    body = {
        'format': data_format,
        'id': indicator_row['filter_value_id'],
        'indicator_title': indicator_row['filter_value_title']
    }
    for _, row in unique.iterrows():
        if row['filter_field_id'] == '0':
            continue
        body[row['filter_field_object_ids']] = row['filter_field_id']
    selected = []
    for _, row in data_ids.iterrows():
        selected.append(f"{row['filter_field_id']}_{row['filter_value_id']}")
    body['selectedFilterIds'] = selected

    url = f"{BASE_URL}/indicator/data.do?format={data_format}"
    response = session.post(url, data=body, timeout=180)
    response.raise_for_status()

    # Сохраняем ответ для отладки
    with open(f"logs/response.{data_format}", "wb") as f:
        f.write(response.content)

    return response.content

def _parse_sdmx_to_dataframe(xml_bytes):
    root = ET.fromstring(xml_bytes)
    ns = {
        'generic': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_1/message',
        'structure': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_1/structure',
        'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_1/common'
    }
    codelists = {}
    for cl in root.findall('.//structure:CodeList', ns):
        cid = cl.get('id')
        name_elem = cl.find('structure:Name', ns)
        name = name_elem.text if name_elem is not None else cid
        codes = {}
        for code in cl.findall('structure:Code', ns):
            code_id = code.get('value')
            code_name = code.text
            codes[code_id] = code_name
        codelists[cid] = (name, codes)

    series_list = []
    for series in root.findall('.//generic:Series', ns):
        series_attrs = {}
        for key in series.findall('generic:SeriesKey/generic:Value', ns):
            concept = key.get('concept')
            value = key.get('value')
            if concept:
                series_attrs[concept] = value
        for obs in series.findall('generic:Obs', ns):
            obs_attrs = dict(series_attrs)
            for attr in obs.findall('generic:ObsKey/generic:Value', ns):
                concept = attr.get('concept')
                value = attr.get('value')
                if concept:
                    obs_attrs[concept] = value
            obs_val = obs.find('generic:ObsValue', ns)
            if obs_val is not None:
                obs_attrs['ObsValue'] = obs_val.get('value')
            for attr in obs.findall('generic:Attributes/generic:Value', ns):
                concept = attr.get('concept')
                value = attr.get('value')
                if concept:
                    obs_attrs[concept] = value
            series_list.append(obs_attrs)

    df = pd.DataFrame(series_list)
    for col in df.columns:
        if col in codelists:
            name, codes = codelists[col]
            new_col = name
            df[new_col] = df[col].map(codes)
            df.rename(columns={col: f"{col}_code"}, inplace=True)
    return df

def parse_sdmx_to_table(data_raw, try_to_parse_obsvalue=True):
    df = _parse_sdmx_to_dataframe(data_raw)
    if try_to_parse_obsvalue and 'ObsValue' in df.columns:
        df['ObsValue'] = pd.to_numeric(df['ObsValue'], errors='coerce')
    return df