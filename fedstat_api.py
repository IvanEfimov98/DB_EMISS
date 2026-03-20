import re
import json
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from utils import retry
import demjson3
import logging
logger = logging.getLogger(__name__)

BASE_URL = "https://www.fedstat.ru"
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "ru,en-US;q=0.9,en;q=0.8",
    "Referer": BASE_URL,
})
session.get(BASE_URL, timeout=30)

def _normalize_js_string(s):
    s = re.sub(r'//.*', '', s)           # удалить однострочные комментарии
    s = re.sub(r',\s*}', '}', s)          # trailing запятые в объектах
    s = re.sub(r',\s*]', ']', s)          # trailing запятые в массивах
    return s

def _extract_json_object(script, start_key):
    pos = script.find(start_key)
    if pos == -1:
        return None
    brace_start = script.find('{', pos)
    if brace_start == -1:
        return None
    balance = 1
    i = brace_start + 1
    while balance > 0 and i < len(script):
        if script[i] == '{':
            balance += 1
        elif script[i] == '}':
            balance -= 1
        i += 1
    if balance != 0:
        return None
    return script[brace_start:i]

def _parse_js_filters(script):
    """Извлечь объект filters из скрипта."""
    block = _extract_json_object(script, 'filters:', ',left_columns:')
    if not block:
        raise ValueError("Не удалось извлечь блок filters")
    block = _normalize_js_string(block)
    # demjson3.parse может работать и с одинарными кавычками
    try:
        data = demjson3.decode(block)
    except Exception as e:
        raise ValueError(f"Ошибка парсинга JSON фильтров: {e}")
    return data

def _parse_js_object_ids(script):
    """Извлечь left_columns, top_columns, groups."""
    # Ищем три массива: left_columns, top_columns, groups
    # Они могут идти в разном порядке, но мы ищем каждый отдельно
    result = {}
    for key in ['left_columns', 'top_columns', 'groups']:
        # Ищем массив: key: [ ... ]
        match = re.search(rf'{key}:\s*(\[.*?\])\s*,', script, re.DOTALL)
        if not match:
            continue
        arr_str = match.group(1)
        arr_str = _normalize_js_string(arr_str)
        try:
            arr = demjson3.decode(arr_str)
        except Exception:
            continue
        # Определяем тип object_id
        obj_type = 'lineObjectIds'
        if key == 'top_columns':
            obj_type = 'columnObjectIds'
        elif key == 'groups':
            obj_type = 'filterObjectIds'
        for item in arr:
            if isinstance(item, dict) and 'id' in item:
                result[str(item['id'])] = obj_type
    return result

@retry(max_tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.RequestException,))
def get_data_ids(indicator_id):
    url = f"{BASE_URL}/indicator/{indicator_id}"
    response = session.get(url, timeout=180)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    script_text = None
    for script in soup.find_all('script'):
        if script.string and 'filters: {' in script.string and 'left_columns: [' in script.string:
            script_text = script.string
            break
    if not script_text:
        raise ValueError("Не удалось найти скрипт с фильтрами")

    logger.debug(f"Найден скрипт длиной {len(script_text)} символов. Начало: {script_text[:500]}")
    filters_data = _parse_js_filters(script_text)
    object_ids = _parse_js_object_ids(script_text)

    rows = []
    for filter_group in filters_data.get('filters', []):
        field_id = str(filter_group.get('id', ''))
        field_title = filter_group.get('title', '')
        values = filter_group.get('values', {})
        for val_id, val_info in values.items():
            val_title = val_info.get('title', '')
            rows.append([field_id, field_title, str(val_id), val_title, ''])
    df = pd.DataFrame(rows, columns=['filter_field_id', 'filter_field_title',
                                     'filter_value_id', 'filter_value_title',
                                     'filter_field_object_ids'])
    for fid in df['filter_field_id'].unique():
        if fid in object_ids:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = object_ids[fid]
        else:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = 'lineObjectIds'
    df.loc[df['filter_field_id'] == '0', 'filter_field_object_ids'] = 'filterObjectIds'
    return df

@retry(max_tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.RequestException,))
def post_data_ids_filtered(data_ids, data_format='sdmx'):
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