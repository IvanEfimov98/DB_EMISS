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

def _extract_balanced_brace(text, start_pos):
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

def _clean_js_object(js_block):
    """Очистка JavaScript-объекта для парсинга как JSON."""
    # Удаляем однострочные комментарии
    lines = js_block.split('\n')
    cleaned_lines = []
    for line in lines:
        # Удаляем //, но не внутри строк (упрощённо)
        if '//' in line:
            # Простой способ: удаляем всё после //, если не внутри кавычек
            # Для сложных случаев можно улучшить, но для fedstat хватит
            line = re.sub(r'(?<!["\'])\\/\\/.*$', '', line)
        cleaned_lines.append(line)
    js_block = '\n'.join(cleaned_lines)
    # Убираем trailing запятые
    js_block = re.sub(r',\s*}', '}', js_block)
    js_block = re.sub(r',\s*]', ']', js_block)
    # Заменяем одинарные кавычки на двойные
    js_block = js_block.replace("'", '"')
    # Удаляем вызовы типа $('#grid')
    js_block = re.sub(r'\$\([^)]+\)', 'null', js_block)
    # Удаляем символ $
    js_block = js_block.replace('$', '')
    return js_block

def extract_filters_from_html(html_path):
    """Извлекает фильтры и массивы из 12-го скрипта HTML."""
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    scripts = soup.find_all('script')
    if len(scripts) < 12:
        raise ValueError(f"Найдено только {len(scripts)} скриптов, ожидалось минимум 12")
    script_text = scripts[11].string
    if not script_text:
        raise ValueError("12-й скрипт не содержит текста")

    # Ищем new FGrid({
    pos = script_text.find('new FGrid({')
    if pos == -1:
        raise ValueError("Не найдено new FGrid({")
    block = _extract_balanced_brace(script_text, pos)
    if not block:
        raise ValueError("Не удалось извлечь блок FGrid")
    cleaned = _clean_js_object(block)
    try:
        data = demjson3.decode(cleaned)
    except Exception as e:
        logger.exception("Ошибка парсинга FGrid")
        with open('logs/fgrid_failed.json', 'w', encoding='utf-8') as f:
            f.write(cleaned)
        raise ValueError(f"Ошибка парсинга JSON FGrid: {e}")

    filters = data.get('filters', {})
    left_columns = data.get('left_columns', [])
    top_columns = data.get('top_columns', [])
    groups = data.get('groups', [])
    return {'filters': filters, 'left_columns': left_columns, 'top_columns': top_columns, 'groups': groups}

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

    try:
        extracted = extract_filters_from_html(html_path)
    except Exception as e:
        logger.exception("Ошибка извлечения фильтров")
        raise

    filters = extracted['filters']
    left_columns = extracted['left_columns']
    top_columns = extracted['top_columns']
    groups = extracted['groups']

    # Построение mapping filter_field_id -> object_type
    object_ids = {}
    for item in left_columns:
        if isinstance(item, dict) and 'id' in item:
            object_ids[str(item['id'])] = 'lineObjectIds'
    for item in top_columns:
        if isinstance(item, dict) and 'id' in item:
            object_ids[str(item['id'])] = 'columnObjectIds'
    for item in groups:
        if isinstance(item, dict) and 'id' in item:
            object_ids[str(item['id'])] = 'filterObjectIds'
    # Специальный фильтр для индикатора (id=0)
    object_ids['0'] = 'filterObjectIds'

    # Формируем data_ids
    rows = []
    for fid, finfo in filters.items():
        field_id = str(fid)
        field_title = finfo.get('title', '')
        values = finfo.get('values', {})
        if not values:
            logger.warning(f"Фильтр {field_id} ({field_title}) не имеет значений")
            continue
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