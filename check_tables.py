#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_tables(url="https://www.dohod.ru/ik/analytics/dividend"):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Получаем страницу
        logger.info(f"Получаем страницу {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Парсим HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Находим все таблицы
        tables = soup.find_all('table')
        logger.info(f"Найдено таблиц на странице: {len(tables)}")
        
        # Проверяем каждую таблицу
        for i, table in enumerate(tables):
            logger.info(f"\n=== Таблица #{i+1} ===")
            
            # Получаем заголовки
            headers_row = table.find('tr')
            if headers_row:
                headers = [th.text.strip() for th in headers_row.find_all(['th', 'td'])]
                logger.info(f"Заголовки: {headers}")
            
            # Проверяем структуру данных
            rows = table.find_all('tr')
            logger.info(f"Количество строк: {len(rows)}")
            
            # Анализируем первую строку с данными (после заголовка)
            if len(rows) > 1:
                data_row = rows[1]
                cells = data_row.find_all('td')
                logger.info(f"Количество ячеек в первой строке данных: {len(cells)}")
                
                # Выводим первые несколько значений
                for j, cell in enumerate(cells[:5]):
                    logger.info(f"  Ячейка #{j+1}: {cell.text.strip()}")
                
                # Проверяем наличие ссылок в первой ячейке
                first_cell = cells[0] if cells else None
                if first_cell:
                    links = first_cell.find_all('a')
                    if links:
                        logger.info(f"  Ссылка в первой ячейке: {links[0]['href']}")
    
    except Exception as e:
        logger.error(f"Ошибка при проверке таблиц: {e}")

if __name__ == "__main__":
    check_tables() 