import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
from database import save_indicators, create_indicators_table

logger = logging.getLogger(__name__)

def update_indicators():
    url = "https://www.fedstat.ru/indicators/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ru,en-US;q=0.9,en;q=0.8",
        "Referer": "https://www.fedstat.ru/",
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    # Находим все блоки с индикаторами
    items = soup.find_all('div', class_='publ_item')
    logger.info(f"Найдено блоков publ_item: {len(items)}")

    data = []
    for div in items:
        # ID индикатора из атрибута id
        div_id = div.get('id', '')
        if not div_id.startswith('indicator'):
            continue
        indicator_id = div_id.replace('indicator', '')
        # Ссылка и название
        link = div.find('a', class_='lnk lnk_txt')
        if not link:
            continue
        href = link.get('href', '')
        # Проверяем, что href ведёт на /indicator/...
        if '/indicator/' not in href:
            continue
        name = link.get_text(strip=True)
        url_ind = f"https://www.fedstat.ru{href}" if href.startswith('/') else href

        # Ведомство (может быть несколько, возьмём первое)
        department = ''
        dept_div = div.find('div', class_='publ_bot', id=lambda x: x and 'org' in x)
        if dept_div:
            dept_link = dept_div.find('a', class_='lnk')
            if dept_link:
                department = dept_link.get_text(strip=True)

        # Признак excluded
        excluded = False
        excl_span = div.find('span', class_='hide', id=f"indicator{indicator_id}_excluded")
        if excl_span and excl_span.get_text(strip=True) == 'true':
            excluded = True

        data.append({
            'id': indicator_id,
            'url': url_ind,
            'name': name,
            'excluded': excluded,
            'department': department,
            'group_level_2': '',
            'group_level_3': '',
            'group_level_4': '',
            'group_level_5': '',
            'group_level_6': '',
            'date_of_update': datetime.now().isoformat()
        })

    logger.info(f"Извлечено индикаторов: {len(data)}")
    if not data:
        logger.warning("Индикаторы не найдены. Проверьте структуру страницы.")
        return None

    df = pd.DataFrame(data)
    create_indicators_table()
    save_indicators(df)
    logger.info(f"Сохранено {len(df)} индикаторов в БД")
    return df