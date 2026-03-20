import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from database import save_indicators, create_indicators_table

def update_indicators():
    """Парсит страницу https://www.fedstat.ru/organizations/ и обновляет таблицу indicators."""
    url = "https://www.fedstat.ru/organizations/"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    # Находим корневой контейнер с иерархией
    orgs_tree = soup.find('div', id='orgsTree')
    if not orgs_tree:
        raise ValueError("Не удалось найти элемент orgsTree на странице")

    items = orgs_tree.find_all('div', recursive=False)
    data = []

    for item in items:
        department = item.find('div', class_='i_name org')
        if not department:
            continue
        department_name = department.get_text(strip=True)

        # Ищем вложенные группы (ved_child)
        ved_child = item.find('div', class_='ved_child')
        if ved_child:
            groups = ved_child.find_all('div', recursive=False)
            for group in groups:
                group_name_div = group.find('div', class_='ved_pl dtable group')
                group_name = ''
                if group_name_div:
                    group_name_elem = group_name_div.find('span', class_='i_name org') or group_name_div.find('span', class_='i_name ci')
                    if group_name_elem:
                        group_name = group_name_elem.get_text(strip=True)

                # Подгруппы (ved_child внутри группы)
                sub_ved_child = group.find('div', class_='ved_child')
                if sub_ved_child:
                    sub_groups = sub_ved_child.find_all('div', recursive=False)
                    for sub_group in sub_groups:
                        sub_name_div = sub_group.find('div', class_='ved_pl dtable group')
                        sub_name = ''
                        if sub_name_div:
                            sub_name_elem = sub_name_div.find('span', class_='i_name org') or sub_name_div.find('span', class_='i_name ci')
                            if sub_name_elem:
                                sub_name = sub_name_elem.get_text(strip=True)

                        # Индикаторы
                        indicators = sub_group.find_all('div', class_='ved_item group i_actual')
                        for ind in indicators:
                            link = ind.find('a')
                            if not link:
                                continue
                            href = link.get('href', '')
                            indicator_id = href.split('/')[-1] if href else ''
                            if not indicator_id:
                                continue
                            name = link.find('span', class_='i_name')
                            name = name.get_text(strip=True) if name else ''
                            data.append({
                                'id': indicator_id,
                                'url': f"https://www.fedstat.ru/indicator/{indicator_id}",
                                'name': name,
                                'excluded': False,
                                'department': department_name,
                                'group_level_2': group_name,
                                'group_level_3': sub_name,
                                'group_level_4': '',
                                'group_level_5': '',
                                'group_level_6': '',
                                'date_of_update': datetime.now()
                            })
                else:
                    # Если нет подгрупп, индикаторы могут быть прямо в группе
                    indicators = group.find_all('div', class_='ved_item group i_actual')
                    for ind in indicators:
                        link = ind.find('a')
                        if not link:
                            continue
                        href = link.get('href', '')
                        indicator_id = href.split('/')[-1] if href else ''
                        if not indicator_id:
                            continue
                        name = link.find('span', class_='i_name')
                        name = name.get_text(strip=True) if name else ''
                        data.append({
                            'id': indicator_id,
                            'url': f"https://www.fedstat.ru/indicator/{indicator_id}",
                            'name': name,
                            'excluded': False,
                            'department': department_name,
                            'group_level_2': group_name,
                            'group_level_3': '',
                            'group_level_4': '',
                            'group_level_5': '',
                            'group_level_6': '',
                            'date_of_update': datetime.now()
                        })
        else:
            # Возможно, индикаторы на верхнем уровне
            indicators = item.find_all('div', class_='ved_item group i_actual')
            for ind in indicators:
                link = ind.find('a')
                if not link:
                    continue
                href = link.get('href', '')
                indicator_id = href.split('/')[-1] if href else ''
                if not indicator_id:
                    continue
                name = link.find('span', class_='i_name')
                name = name.get_text(strip=True) if name else ''
                data.append({
                    'id': indicator_id,
                    'url': f"https://www.fedstat.ru/indicator/{indicator_id}",
                    'name': name,
                    'excluded': False,
                    'department': department_name,
                    'group_level_2': '',
                    'group_level_3': '',
                    'group_level_4': '',
                    'group_level_5': '',
                    'group_level_6': '',
                    'date_of_update': datetime.now()
                })

    df = pd.DataFrame(data)
    create_indicators_table()
    save_indicators(df)
    return df