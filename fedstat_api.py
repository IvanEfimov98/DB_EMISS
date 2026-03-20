import re
import json
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

def _extract_filters_block(script):
    """Извлекает блок между 'filters: {' и ',left_columns:'."""
    start = script.find('filters: {')
    if start == -1:
        return None
    end = script.find(',left_columns:', start)
    if end == -1:
        return None
    # Вырезаем текст от начала объекта до конца блока
    block = script[start:end].strip()
    # Добавляем закрывающую скобку, если её нет
    if not block.endswith('}'):
        # Ищем последнюю закрывающую скобку в блоке
        # Проще взять весь блок и сбалансировать
        # Но для простоты: находим позицию последней '}' до end
        brace_pos = block.rfind('}')
        if brace_pos != -1:
            block = block[:brace_pos+1]
        else:
            block = block + '}'
    return block

def _parse_js_filters(script):
    block = _extract_filters_block(script)
    if not block:
        raise ValueError("Не удалось извлечь блок filters")
    logger.debug(f"Извлечённый блок (первые 500 символов):\n{block[:500]}")
    # Сохраняем блок в файл для анализа
    with open("logs/filters_block.txt", "w", encoding="utf-8") as f:
        f.write(block)
    block = _normalize_js_string(block)
    try:
        data = demjson3.decode(block)
    except Exception as e:
        logger.exception("Ошибка парсинга JSON фильтров")
        raise ValueError(f"Ошибка парсинга JSON фильтров: {e}")
    return data

def _parse_js_object_ids(script):
    result = {}
    for key in ['left_columns', 'top_columns', 'groups']:
        # Находим массив
        start = script.find(f'{key}:')
        if start == -1:
            continue
        bracket_start = script.find('[', start)
        if bracket_start == -1:
            continue
        balance = 1
        i = bracket_start + 1
        while balance > 0 and i < len(script):
            if script[i] == '[':
                balance += 1
            elif script[i] == ']':
                balance -= 1
            i += 1
        if balance != 0:
            continue
        arr_str = script[bracket_start:i]
        arr_str = _normalize_js_string(arr_str)
        try:
            arr = demjson3.decode(arr_str)
        except Exception:
            continue
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
        logger.warning(f"Найдено только {len(scripts)} скриптов, берём последний")
        script_tag = scripts[-1] if scripts else None
    else:
        script_tag = scripts[11]  # 12-й элемент

    if not script_tag or not script_tag.string:
        raise ValueError("Не удалось найти скрипт с фильтрами")
    script_text = script_tag.string
    logger.debug(f"Скрипт (первые 500 символов): {script_text[:500]}")
    # Сохраняем скрипт в файл для отладки
    with open("logs/script.txt", "w", encoding="utf-8") as f:
        f.write(script_text)

    filters_data = _parse_js_filters(script_text)
    object_ids = _parse_js_object_ids(script_text)

    # Выводим структуру filters_data
    logger.debug(f"Ключи filters_data: {list(filters_data.keys())}")
    if 'filters' in filters_data:
        logger.debug(f"Количество групп фильтров: {len(filters_data['filters'])}")
    else:
        logger.warning("В filters_data нет ключа 'filters'")

    rows = []
    for filter_group in filters_data.get('filters', []):
        field_id = str(filter_group.get('id', ''))
        field_title = filter_group.get('title', '')
        values = filter_group.get('values', {})
        for val_id, val_info in values.items():
            val_title = val_info.get('title', '')
            rows.append([field_id, field_title, str(val_id), val_title, ''])

    if not rows:
        logger.error("Не найдено ни одного фильтра в data_ids")
        raise ValueError("Не найдено ни одного фильтра в data_ids")

    df = pd.DataFrame(rows, columns=['filter_field_id', 'filter_field_title',
                                     'filter_value_id', 'filter_value_title',
                                     'filter_field_object_ids'])
    for fid in df['filter_field_id'].unique():
        if fid in object_ids:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = object_ids[fid]
        else:
            df.loc[df['filter_field_id'] == fid, 'filter_field_object_ids'] = 'lineObjectIds'
    df.loc[df['filter_field_id'] == '0', 'filter_field_object_ids'] = 'filterObjectIds'
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