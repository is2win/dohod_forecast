#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
import os
import logging
import json
import time
from urllib.parse import urljoin

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DividendParser:
    def __init__(self, base_url="https://www.dohod.ru/ik/analytics/dividend"):
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.stock_list = []  # Список акций
        self.detailed_dividends = []  # Детальная информация о дивидендах
        
    def _parse_date(self, date_str):
        """Преобразование строки даты в объект datetime"""
        if not date_str or date_str == 'n/a':
            return None
        
        try:
            # Различные форматы дат, которые могут встретиться
            formats = ["%d.%m.%Y", "%d-%m-%Y"]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            # Если формат не распознан, возвращаем None
            return None
        except Exception as e:
            logger.error(f"Ошибка при парсинге даты '{date_str}': {e}")
            return None

    def _parse_dividend_value(self, value_str):
        """Преобразование строки с дивидендом в число"""
        if not value_str or value_str == 'n/a' or value_str == '0':
            return 0.0
        
        try:
            # Удаляем все нечисловые символы, кроме точки и запятой
            value_str = re.sub(r'[^\d.,]', '', value_str)
            # Заменяем запятую на точку для правильного парсинга
            value_str = value_str.replace(',', '.')
            return float(value_str)
        except Exception as e:
            logger.error(f"Ошибка при парсинге значения дивиденда '{value_str}': {e}")
            return 0.0

    def parse_main_page(self):
        """Парсинг основной страницы для получения списка акций"""
        logger.info(f"Получаем список акций с {self.base_url}")
        
        try:
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Находим таблицу с акциями
            tables = soup.find_all('table')
            
            if not tables:
                logger.error("Таблица с акциями не найдена")
                return False
            
            # Ищем сначала по хедеру главной таблицы дивидендов
            main_table = None
            for table in tables:
                first_row = table.find('tr')
                if not first_row:
                    continue
                
                headers = first_row.find_all(['th', 'td'])
                header_texts = [h.text.strip().lower() for h in headers]
                
                # Проверяем, что это основная таблица дивидендов
                # Обычно содержит колонки "актив", "закрытие реестра", "дивиденд"...
                keywords = ['актив', 'закрытие реестра', 'дивиденд']
                match_count = sum(1 for keyword in keywords if any(keyword in h for h in header_texts))
                
                if match_count >= 2:  # Если таблица содержит минимум 2 ключевых слова
                    main_table = table
                    logger.info(f"Найдена основная таблица дивидендов по заголовкам ({match_count} совпадений)")
                    break
            
            # Если не нашли по заголовкам, берем первую таблицу, которая содержит ссылки на страницы акций
            if not main_table:
                for table in tables:
                    links = table.find_all('a')
                    dividend_links = [link for link in links if 'dividend/' in link.get('href', '')]
                    
                    if dividend_links:
                        main_table = table
                        logger.info(f"Найдена таблица с ссылками на дивиденды (содержит {len(dividend_links)} ссылок)")
                        break
            
            # Если все еще не нашли, берем самую большую таблицу на странице
            if not main_table and tables:
                main_table = max(tables, key=lambda t: len(t.find_all('tr')))
                logger.info(f"Используем самую большую таблицу на странице ({len(main_table.find_all('tr'))} строк)")
            
            if not main_table:
                logger.error("Не удалось определить подходящую таблицу с акциями")
                return False
            
            # Парсим строки найденной таблицы
            rows = main_table.find_all('tr')
            
            # Пропускаем заголовок
            for row in rows[1:]:
                cells = row.find_all('td')
                if not cells or len(cells) < 2:
                    continue
                
                try:
                    # Получаем название и тикер акции
                    asset_cell = cells[0]
                    asset_name = asset_cell.text.strip()
                    asset_link = asset_cell.find('a')
                    
                    if not asset_link:
                        continue
                    
                    # Проверяем, что ссылка содержит путь к странице акции
                    href = asset_link.get('href', '')
                    if not href:
                        continue
                    
                    # Если ссылка не содержит полный путь, добавляем базовый URL
                    if not href.startswith('http'):
                        full_href = urljoin(self.base_url, href)
                    else:
                        full_href = href
                    
                    # Извлекаем тикер из ссылки
                    ticker = href.split('/')[-1]
                    if not ticker or ticker == '':
                        # Если не удалось извлечь тикер из ссылки, пробуем использовать название
                        ticker = asset_name.lower().replace(' ', '_')
                    
                    # Проверяем, что тикер не пустой и содержит только допустимые символы
                    if ticker and re.match(r'^[a-zA-Z0-9_\-]+$', ticker):
                        self.stock_list.append({
                            'ticker': ticker,
                            'name': asset_name,
                            'url': full_href
                        })
                        logger.debug(f"Добавлена акция: {asset_name} ({ticker}), URL: {full_href}")
                    
                except Exception as e:
                    logger.warning(f"Ошибка при обработке строки: {e}")
                    continue
            
            # Удаляем возможные дубликаты
            unique_tickers = set()
            unique_stock_list = []
            
            for stock in self.stock_list:
                if stock['ticker'] not in unique_tickers:
                    unique_tickers.add(stock['ticker'])
                    unique_stock_list.append(stock)
            
            self.stock_list = unique_stock_list
            
            logger.info(f"Всего найдено уникальных акций: {len(self.stock_list)}")
            return len(self.stock_list) > 0
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге основной страницы: {e}")
            return False

    def parse_stock_details(self, max_stocks=None):
        """Парсинг детальной информации по каждой акции"""
        logger.info("Начинаем парсинг детальной информации по акциям")
        
        if not self.stock_list:
            logger.error("Список акций пуст. Сначала выполните parse_main_page()")
            return False
        
        # Ограничиваем количество акций для обработки (для тестирования)
        stocks_to_process = self.stock_list[:max_stocks] if max_stocks else self.stock_list
        
        for i, stock in enumerate(stocks_to_process):
            logger.info(f"[{i+1}/{len(stocks_to_process)}] Обрабатываем {stock['name']} ({stock['ticker']})")
            
            try:
                # Делаем небольшую паузу между запросами
                if i > 0:
                    time.sleep(1)
                
                # Получаем страницу акции
                response = requests.get(stock['url'], headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Логируем заголовок страницы для отладки
                page_title = soup.find('title').text if soup.find('title') else "Нет заголовка"
                logger.info(f"Заголовок страницы: {page_title}")
                
                # Находим таблицу с детальной информацией о дивидендах
                tables = soup.find_all('table')
                
                if not tables:
                    logger.warning(f"Не найдено таблиц на странице {stock['url']}")
                    
                    # Проверим, есть ли данные в основной таблице для этого тикера
                    self._add_main_table_data_for_ticker(stock)
                    continue
                
                # Определим таблицу с историей дивидендов
                # Тщательнее ищем таблицу с историческими данными: не только текущую, но и любую таблицу с историей дивидендов
                dividend_tables = []  # Все таблицы с дивидендами
                
                # Ищем таблицы, заголовки которых могут содержать слова, связанные с дивидендами
                for idx, table in enumerate(tables):
                    # Проверим все th в этой таблице
                    headers = table.find_all('th')
                    if not headers:
                        # Проверим первую строку, возможно там td используется для заголовков
                        first_row = table.find('tr')
                        if first_row:
                            headers = first_row.find_all('td')
                    
                    header_texts = [h.text.lower() for h in headers]
                    
                    # Проверяем, является ли таблица "Все выплаты"
                    if any("все выплаты" in ' '.join(header_texts).lower() for h in header_texts):
                        dividend_tables.append((table, idx))
                        logger.info(f"Найдена таблица 'Все выплаты' (индекс {idx})")
                        continue
                    
                    # Проверяем, есть ли в заголовке "дата закрытия реестра" - это признак таблицы "Все выплаты"
                    if any("дата закрытия реестра" in h.lower() for h in header_texts):
                        dividend_tables.append((table, idx))
                        logger.info(f"Найдена таблица с датами закрытия реестра (индекс {idx})")
                        continue
                    
                    # Проверяем, это таблица "Совокупные выплаты по годам" - мы должны её игнорировать
                    if any("совокупные выплаты" in ' '.join(header_texts).lower() for h in header_texts):
                        logger.info(f"Игнорируем таблицу 'Совокупные выплаты по годам' (индекс {idx})")
                        continue
                    
                    # Ищем ключевые слова, связанные с детальными дивидендными выплатами
                    detailed_keywords = ['дата закрытия', 'реестр', 'дивиденд', 'объявления']
                    if any(keyword in ' '.join(header_texts) for keyword in detailed_keywords):
                        dividend_tables.append((table, idx))
                        logger.info(f"Найдена таблица с детальными данными о дивидендах (индекс {idx})")
                    
                    # Проверяем, является ли это таблица с годовыми данными - мы должны её игнорировать
                    annual_keywords = ['год', 'дивиденд (руб.)', 'изм. к пред. году']
                    if all(keyword in ' '.join(header_texts) for keyword in annual_keywords):
                        logger.info(f"Игнорируем таблицу с годовыми данными (индекс {idx})")
                        continue
                
                # Если не нашли ни одной таблицы, проверим наличие хотя бы одной таблицы для детального анализа
                if not dividend_tables and tables:
                    # Пытаемся найти таблицу "Все выплаты" через подзаголовок на странице
                    for element in soup.find_all(['p', 'h3', 'h4']):
                        if "все выплаты" in element.text.lower():
                            # Ищем следующую таблицу после этого заголовка
                            table = element.find_next('table')
                            if table:
                                dividend_tables.append((table, 0))
                                logger.info(f"Найдена таблица 'Все выплаты' после заголовка")
                                break
                    
                    # Если всё еще не нашли, берем первую таблицу с достаточным количеством столбцов
                    if not dividend_tables:
                        for idx, table in enumerate(tables):
                            first_row = table.find('tr')
                            if first_row and len(first_row.find_all(['td', 'th'])) >= 3:
                                dividend_tables.append((table, idx))
                                logger.info(f"Используем таблицу {idx} с {len(first_row.find_all(['td', 'th']))} столбцами")
                                break
                
                if not dividend_tables:
                    logger.warning(f"Не удалось определить таблицу с дивидендами для {stock['ticker']}")
                    
                    # Проверим, есть ли данные в основной таблице для этого тикера
                    self._add_main_table_data_for_ticker(stock)
                    continue
                
                # Парсим все таблицы с дивидендами
                row_count = 0
                for dividend_table, table_index in dividend_tables:
                    # Анализируем заголовки таблицы, чтобы понять структуру
                    headers_row = dividend_table.find('tr')
                    headers = []
                    
                    if headers_row:
                        # Выберем подходящие элементы для заголовков
                        header_elements = headers_row.find_all('th')
                        if not header_elements:
                            header_elements = headers_row.find_all('td')
                        
                        headers = [h.text.strip().lower() for h in header_elements]
                        logger.info(f"Заголовки таблицы {table_index}: {headers}")
                    
                    # Проверим, что это не таблица "Совокупные выплаты по годам"
                    if len(headers) == 3 and 'год' in headers[0] and 'дивиденд' in headers[1] and 'изм' in headers[2]:
                        logger.info(f"Пропускаем таблицу 'Совокупные выплаты по годам' (индекс {table_index})")
                        continue
                    
                    # Проверяем, это первая таблица (годовые данные) или вторая (подробные данные)
                    is_annual_table = any('год' in h for h in headers) and any('дивиденд' in h for h in headers) and len(headers) <= 3
                    is_detailed_table = any('дата' in h for h in headers) and any('реестр' in h for h in headers)
                    
                    # Пропускаем обработку таблицы годовых дивидендов (нам нужны только детальные данные)
                    if is_annual_table and not is_detailed_table:
                        logger.info(f"Пропускаем таблицу годовых дивидендов (индекс {table_index})")
                        continue
                
                    # Обработка таблицы годовых дивидендов (обычно первая таблица)
                    if is_annual_table:
                        logger.info(f"Таблица {table_index} определена как таблица годовых дивидендов")
                        
                        # Определяем индексы столбцов по заголовкам
                        year_idx = next((i for i, h in enumerate(headers) if 'год' in h), 0)
                        dividend_idx = next((i for i, h in enumerate(headers) if 'дивиденд' in h), 1)
                        
                        # Парсим строки таблицы
                        table_row_count = 0
                        rows = dividend_table.find_all('tr')
                        
                        for row in rows[1:]:  # Пропускаем заголовок
                            cells = row.find_all('td')
                            if len(cells) <= max(year_idx, dividend_idx):
                                continue  # Недостаточно ячеек
                            
                            try:
                                # Год дивиденда
                                year_cell = cells[year_idx]
                                year_str = year_cell.text.strip()
                                
                                # Извлекаем год из строки
                                year_match = re.search(r'(\d{4})', year_str)
                                year = int(year_match.group(1)) if year_match else None
                                
                                # Если нет года, это может быть прогноз на будущее
                                forecast_type = 1 if "прогноз" in year_str.lower() or not year else 0
                                
                                # Размер дивиденда
                                dividend_cell = cells[dividend_idx]
                                dividend_value_str = dividend_cell.text.strip()
                                
                                # Проверяем наличие числового значения
                                dividend_match = re.search(r'(\d+[.,]\d+|\d+)', dividend_value_str)
                                if not dividend_match:
                                    continue  # Нет числового значения
                                
                                dividend_value = self._parse_dividend_value(dividend_match.group(1))
                                
                                # Если дивиденд положительный и год определен
                                if dividend_value > 0 and (year is not None or forecast_type == 1):
                                    # Создаем дату закрытия реестра (если не известна)
                                    record_date = None
                                    if year:
                                        # По умолчанию 30 июня текущего года
                                        try:
                                            record_date = datetime(year, 6, 30)
                                        except ValueError:
                                            pass
                                    
                                    self.detailed_dividends.append({
                                        'ticker': stock['ticker'],
                                        'name': stock['name'],
                                        'record_date': record_date,
                                        'dividend_value': dividend_value,
                                        'period': f"Год {year}" if year else "Будущий период",
                                        'forecast_type': forecast_type,
                                        'year': year,
                                        'quarter': None,  # Годовой дивиденд
                                        'announcement_date': None
                                    })
                                    table_row_count += 1
                                
                            except Exception as e:
                                logger.warning(f"Ошибка при обработке годовых данных для {stock['ticker']} в строке: {e}")
                                continue
                        
                        logger.info(f"Обработано {table_row_count} строк для {stock['ticker']} в таблице {table_index}")
                        row_count += table_row_count
                        
                    # Обработка таблицы с детальными данными о дивидендах
                    elif is_detailed_table:
                        logger.info(f"Таблица {table_index} определена как таблица детальных дивидендов")
                        
                        # Находим индексы нужных столбцов
                        announcement_date_idx = next((i for i, h in enumerate(headers) if 'объявлен' in h), None)
                        record_date_idx = next((i for i, h in enumerate(headers) if 'реестр' in h or 'закрыт' in h), None)
                        year_idx = next((i for i, h in enumerate(headers) if 'год' in h and 'учет' in h), None)
                        dividend_idx = next((i for i, h in enumerate(headers) if 'дивиденд' in h), None)
                        
                        # Если не нашли нужные столбцы, возможно структура другая
                        if record_date_idx is None or dividend_idx is None:
                            # Пробуем определить по порядку (обычно 2-й столбец - дата закрытия, 4-й - дивиденды)
                            record_date_idx = 1 if len(headers) > 1 else None
                            dividend_idx = 3 if len(headers) > 3 else None
                        
                        # Если все еще не определены, пропускаем таблицу
                        if record_date_idx is None or dividend_idx is None:
                            logger.warning(f"Не удалось определить индексы столбцов для таблицы {table_index}")
                            continue
                        
                        logger.info(f"Индексы столбцов для таблицы {table_index}: record_date={record_date_idx}, dividend={dividend_idx}, announcement_date={announcement_date_idx}")
                        
                        # Парсим строки таблицы
                        table_row_count = 0
                        rows = dividend_table.find_all('tr')
                        
                        # Проверяем первую строку данных, чтобы понять формат
                        if len(rows) > 1:
                            sample_row = rows[1]
                            sample_cells = sample_row.find_all('td')
                            
                            if len(sample_cells) > max(record_date_idx, dividend_idx):
                                date_text = sample_cells[record_date_idx].text.strip()
                                dividend_text = sample_cells[dividend_idx].text.strip()
                                
                                # Проверяем, правильно ли определены индексы
                                date_format = bool(re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', date_text))
                                dividend_format = bool(re.search(r'\d+[.,]\d+|\d+', dividend_text))
                                
                                # Если индексы определены неверно, меняем местами
                                if not date_format and not dividend_format:
                                    # Не можем определить формат, пропускаем таблицу
                                    logger.warning(f"Не удалось определить формат данных для таблицы {table_index}")
                                    continue
                                if not date_format and dividend_format:
                                    # В столбце даты нет даты, ищем столбец с датой
                                    for i, cell in enumerate(sample_cells):
                                        cell_text = cell.text.strip()
                                        if re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', cell_text):
                                            record_date_idx = i
                                            break
                                if date_format and not dividend_format:
                                    # В столбце дивидендов нет числа, ищем столбец с числом
                                    for i, cell in enumerate(sample_cells):
                                        cell_text = cell.text.strip()
                                        if re.search(r'\d+[.,]\d+|\d+', cell_text) and not re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', cell_text):
                                            dividend_idx = i
                                            break
                            
                        # Обрабатываем строки данных
                        for row in rows[1:]:  # Пропускаем заголовок
                            cells = row.find_all('td')
                            if len(cells) <= max(record_date_idx, dividend_idx):
                                continue  # Недостаточно ячеек
                            
                            try:
                                # Дата закрытия реестра
                                record_date_cell = cells[record_date_idx]
                                record_date_str = record_date_cell.text.strip()
                                
                                # Извлекаем дату (DD.MM.YYYY)
                                date_match = re.search(r'(\d{2}[./-]\d{2}[./-]\d{4})', record_date_str)
                                if date_match:
                                    record_date_str = date_match.group(1)
                                else:
                                    # Не нашли дату в нужном формате
                                    continue
                                
                                record_date = self._parse_date(record_date_str)
                                if not record_date:
                                    continue  # Не удалось разобрать дату
                                
                                # Дата объявления дивидендов (если есть)
                                announcement_date = None
                                if announcement_date_idx is not None and announcement_date_idx < len(cells):
                                    announcement_cell = cells[announcement_date_idx]
                                    announcement_str = announcement_cell.text.strip()
                                    
                                    # Извлекаем дату (DD.MM.YYYY)
                                    announcement_match = re.search(r'(\d{2}[./-]\d{2}[./-]\d{4})', announcement_str)
                                    if announcement_match:
                                        announcement_str = announcement_match.group(1)
                                        announcement_date = self._parse_date(announcement_str)
                                
                                # Год для учета
                                year = record_date.year
                                
                                # Определяем тип прогноза
                                forecast_type = 0
                                if "прогноз" in record_date_str.lower():
                                    forecast_type = 1
                                elif record_date > datetime.now():
                                    forecast_type = 1
                                
                                # Размер дивиденда
                                dividend_cell = cells[dividend_idx]
                                dividend_value_str = dividend_cell.text.strip()
                                
                                # Извлекаем числовое значение
                                dividend_match = re.search(r'(\d+[.,]\d+|\d+)', dividend_value_str)
                                if not dividend_match:
                                    # Ищем в других ячейках
                                    for i, cell in enumerate(cells):
                                        if i != record_date_idx:  # Не проверяем ячейку с датой
                                            cell_text = cell.text.strip()
                                            match = re.search(r'(\d+[.,]\d+|\d+)', cell_text)
                                            if match and not re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', cell_text):
                                                dividend_value_str = match.group(1)
                                                break
                                else:
                                    dividend_value_str = dividend_match.group(1)
                                
                                # Преобразуем в число
                                dividend_value = self._parse_dividend_value(dividend_value_str)
                                
                                # Определяем квартал
                                quarter = (record_date.month - 1) // 3 + 1
                                
                                # Если дивиденд положительный и дата определена
                                if dividend_value > 0:
                                    self.detailed_dividends.append({
                                        'ticker': stock['ticker'],
                                        'name': stock['name'],
                                        'record_date': record_date,
                                        'dividend_value': dividend_value,
                                        'period': f"Q{quarter} {year}",
                                        'forecast_type': forecast_type,
                                        'year': year,
                                        'quarter': quarter,
                                        'announcement_date': announcement_date
                                    })
                                    table_row_count += 1
                                
                            except Exception as e:
                                logger.warning(f"Ошибка при обработке детальных данных для {stock['ticker']} в строке: {e}")
                                continue
                        
                        logger.info(f"Обработано {table_row_count} строк для {stock['ticker']} в таблице {table_index}")
                        row_count += table_row_count
                    
                    # Неизвестный тип таблицы, пробуем универсальный подход
                    else:
                        logger.info(f"Таблица {table_index} не определена как основная таблица дивидендов, пробуем универсальный подход")
                        
                        # Парсим строки таблицы
                        table_row_count = 0
                        rows = dividend_table.find_all('tr')
                        
                        # Пропускаем заголовок
                        for row in rows[1:]:
                            cells = row.find_all('td')
                            if len(cells) < 2:  # Нужно минимум 2 ячейки
                                continue
                            
                            try:
                                # Перебираем все ячейки в поисках даты и значения дивиденда
                                date_cell_idx = None
                                dividend_cell_idx = None
                                
                                for i, cell in enumerate(cells):
                                    cell_text = cell.text.strip()
                                    
                                    # Ищем ячейку с датой
                                    if re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', cell_text) and date_cell_idx is None:
                                        date_cell_idx = i
                                    
                                    # Ищем ячейку с числом, не содержащим даты
                                    elif re.search(r'\d+[.,]\d+|\d+', cell_text) and not re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', cell_text) and dividend_cell_idx is None:
                                        dividend_cell_idx = i
                                
                                # Если не нашли ни одного нужного значения
                                if date_cell_idx is None and dividend_cell_idx is None:
                                    continue
                                
                                # Извлекаем дату, если нашли
                                record_date = None
                                year = None
                                if date_cell_idx is not None:
                                    date_str = cells[date_cell_idx].text.strip()
                                    date_match = re.search(r'(\d{2}[./-]\d{2}[./-]\d{4})', date_str)
                                    if date_match:
                                        record_date = self._parse_date(date_match.group(1))
                                        if record_date:
                                            year = record_date.year
                                
                                # Если нет даты, ищем год в любой ячейке
                                if year is None:
                                    for cell in cells:
                                        year_match = re.search(r'(\d{4})', cell.text.strip())
                                        if year_match:
                                            year = int(year_match.group(1))
                                            break
                                
                                # Извлекаем дивиденд, если нашли
                                dividend_value = 0
                                if dividend_cell_idx is not None:
                                    dividend_str = cells[dividend_cell_idx].text.strip()
                                    dividend_match = re.search(r'(\d+[.,]\d+|\d+)', dividend_str)
                                    if dividend_match:
                                        dividend_value = self._parse_dividend_value(dividend_match.group(1))
                                
                                # Если нет дивиденда, ищем в любой ячейке
                                if dividend_value == 0:
                                    for i, cell in enumerate(cells):
                                        if i != date_cell_idx:  # Не проверяем ячейку с датой
                                            cell_text = cell.text.strip()
                                            dividend_match = re.search(r'(\d+[.,]\d+|\d+)', cell_text)
                                            if dividend_match and not re.search(r'\d{2}[./-]\d{2}[./-]\d{4}', cell_text):
                                                dividend_value = self._parse_dividend_value(dividend_match.group(1))
                                                break
                                
                                # Определяем тип прогноза
                                forecast_type = 0
                                for cell in cells:
                                    cell_text = cell.text.strip().lower()
                                    if "прогноз" in cell_text:
                                        forecast_type = 1
                                        break
                                
                                # Если у нас есть дата и она в будущем, это прогноз
                                if record_date and record_date > datetime.now():
                                    forecast_type = 1
                                
                                # Определяем квартал, если есть дата
                                quarter = None
                                if record_date:
                                    quarter = (record_date.month - 1) // 3 + 1
                                
                                # Определяем период
                                period = None
                                if year and quarter:
                                    period = f"Q{quarter} {year}"
                                elif year:
                                    period = f"Год {year}"
                                
                                # Если дивиденд положительный и у нас есть дата или год
                                if dividend_value > 0 and (record_date is not None or year is not None):
                                    self.detailed_dividends.append({
                                        'ticker': stock['ticker'],
                                        'name': stock['name'],
                                        'record_date': record_date,
                                        'dividend_value': dividend_value,
                                        'period': period or "Неизвестный период",
                                        'forecast_type': forecast_type,
                                        'year': year,
                                        'quarter': quarter,
                                        'announcement_date': None
                                    })
                                    table_row_count += 1
                                
                            except Exception as e:
                                logger.warning(f"Ошибка при универсальной обработке данных для {stock['ticker']} в строке: {e}")
                                continue
                        
                        logger.info(f"Обработано {table_row_count} строк для {stock['ticker']} в таблице {table_index}")
                        row_count += table_row_count
            
            except Exception as e:
                logger.error(f"Ошибка при обработке акции {stock['ticker']}: {e}")
                continue
        
        logger.info(f"Всего собрано дивидендных записей: {len(self.detailed_dividends)}")
        return True
    
    def _add_main_table_data_for_ticker(self, stock):
        """Добавляет данные из основной таблицы для указанного тикера"""
        try:
            # Возвращаемся на главную страницу
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Находим основную таблицу
            tables = soup.find_all('table')
            if not tables:
                return
            
            main_table = tables[0]
            rows = main_table.find_all('tr')
            
            # Пропускаем заголовок
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) < 8:  # Нужно минимум 8 ячеек для обработки
                    continue
                
                try:
                    # Проверяем, соответствует ли эта строка нужному тикеру
                    asset_cell = cells[0]
                    asset_link = asset_cell.find('a')
                    if not asset_link:
                        continue
                    
                    row_ticker = asset_link['href'].split('/')[-1]
                    if row_ticker != stock['ticker']:
                        continue
                    
                    # Название акции
                    asset_name = asset_cell.text.strip()
                    
                    # Период выплаты
                    period = cells[2].text.strip()
                    
                    # Размер дивиденда
                    dividend_value_str = cells[3].text.strip()
                    dividend_value = self._parse_dividend_value(dividend_value_str)
                    
                    # Дата закрытия реестра
                    record_date_str = cells[8].text.strip() if len(cells) > 8 else ""
                    record_date = self._parse_date(record_date_str)
                    
                    # Определяем тип прогноза
                    # 0 - фактические значения
                    # 1 - прогноз с сайта доход
                    # 2 - наш прогноз на основе прошлых лет (для этого метода всегда 0 или 1)
                    forecast_type = 0
                    
                    # Проверяем, является ли дивиденд прогнозом
                    if "прогноз" in dividend_value_str.lower():
                        forecast_type = 1
                    elif "(" in dividend_value_str:
                        forecast_type = 1
                    elif record_date and record_date > datetime.now():
                        forecast_type = 1
                    elif not record_date and dividend_value > 0:
                        forecast_type = 1
                    
                    # Определяем год дивиденда
                    year = None
                    year_match = re.search(r'(\d{4})', period)
                    if year_match:
                        year = int(year_match.group(1))
                    elif record_date:
                        year = record_date.year
                    
                    # Определяем квартал
                    quarter = None
                    quarter_patterns = [
                        r'[qQкК]\s*(\d)',  # Q1, К2, etc.
                        r'(\d)\s*[qQкК]',  # 1Q, 3К, etc.
                        r'(\d)\s*кв',      # 1кв, 2 кв, etc.
                        r'квартал\s*(\d)'  # квартал 1, etc.
                    ]
                    
                    for pattern in quarter_patterns:
                        quarter_match = re.search(pattern, period, re.IGNORECASE)
                        if quarter_match:
                            quarter = int(quarter_match.group(1))
                            break
                    
                    # Если не удалось определить квартал из периода, определим по месяцу
                    if quarter is None and record_date:
                        quarter = (record_date.month - 1) // 3 + 1
                    
                    # Добавляем данные
                    if dividend_value > 0:
                        self.detailed_dividends.append({
                            'ticker': stock['ticker'],
                            'name': asset_name,
                            'record_date': record_date,
                            'dividend_value': dividend_value,
                            'period': period,
                            'forecast_type': forecast_type,  # Тип прогноза
                            'year': year,
                            'quarter': quarter,
                            'announcement_date': None
                        })
                        logger.info(f"Добавлены данные из основной таблицы для {stock['ticker']}")
                    
                except Exception as e:
                    logger.warning(f"Ошибка при добавлении данных из основной таблицы для {stock['ticker']}: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Ошибка при получении данных из основной таблицы для {stock['ticker']}: {e}")

    def clean_data(self):
        """Очистка и подготовка данных"""
        logger.info("Начинаем очистку данных")
        
        # Преобразуем в DataFrame
        df = pd.DataFrame(self.detailed_dividends)
        
        if df.empty:
            logger.warning("Нет данных для очистки")
            self.clean_df = pd.DataFrame()
            return df
        
        # Заполняем отсутствующие значения
        df['announcement_date'] = df['announcement_date'].fillna("no data")
        df['record_date'] = df['record_date']
        df['year'] = df['year']
        df['dividend_value'] = df['dividend_value'].fillna(0.0)
        
        # Убеждаемся, что у нас есть колонка forecast_type
        if 'forecast_type' not in df.columns:
            df['forecast_type'] = 0
        
        # Приводим даты к строковому формату для удобства
        df['record_date_str'] = df['record_date'].apply(
            lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) and isinstance(x, datetime) else "no data"
        )
        
        # Получаем месяц выплаты дивидендов
        df['month'] = df['record_date'].apply(
            lambda x: x.month if pd.notnull(x) and isinstance(x, datetime) else None
        )
        
        # Фильтруем записи, удаляем дубликаты
        df = df.drop_duplicates(subset=['ticker', 'record_date_str', 'dividend_value'], keep='first')
        
        self.clean_df = df
        logger.info(f"После очистки осталось записей: {len(df)}")
        return df

    def forecast_dividends(self, years=10, history_years=3):
        """Прогнозирование дивидендов на будущее на основе исторических данных
        
        Args:
            years (int): Количество лет вперед для прогнозирования
            history_years (int): Глубина (количество лет) для проверки наличия недавних выплат
                               Если за последние history_years лет нет выплат, применяется критический сценарий
        """
        logger.info(f"Начинаем прогнозирование дивидендов на {years} лет вперед (проверка активности за последние {history_years} лет)")
        
        if self.clean_df.empty:
            logger.warning("Нет данных для прогнозирования")
            return None
        
        df = self.clean_df.copy()
        
        # Добавляем поле стратегии прогноза для всех записей
        df['forecast_strategy'] = df['forecast_type'].apply(
            lambda x: "0 - Фактические данные" if x == 0 else "1 - Прогноз сайта"
        )
        
        # Текущий год для расчета прогнозов
        current_year = datetime.now().year
        
        # Минимальный год для проверки активности компании
        recent_activity_year = current_year - history_years
        
        # Создаем пустой список для прогнозов
        forecast_data = []
        
        # Информация по прогнозам
        forecasts_by_ticker = {}
        
        # Группируем данные по тикеру
        for ticker, group in df.groupby('ticker'):
            logger.info(f"Анализируем данные для {ticker}")
            
            # Группируем исторические выплаты по кварталам
            ticker_quarters = {}
            
            # Также будем учитывать прогнозы сайта доход (тип 1)
            site_forecasts = {}
            
            # Собираем все фактические выплаты для акции (для возможного годового прогноза)
            all_payments = []
            
            # Собираем выплаты за последние years лет для проверки активности
            recent_payments_sum = 0
            
            # Сохраняем все уникальные даты выплат внутри года для более точного прогноза
            annual_payment_dates = {}
            
            # Анализируем все исторические данные (без ограничения на последние годы)
            for _, row in group.iterrows():
                # Обрабатываем фактические выплаты
                if row['forecast_type'] == 0 and row['year'] is not None:
                    # Собираем все фактические выплаты для возможного годового прогноза
                    if row['dividend_value'] > 0:
                        all_payments.append({
                            'dividend_value': row['dividend_value'],
                            'record_date': row['record_date'],
                            'month': row['month'],
                            'year': row['year']
                        })
                        
                        # Проверяем, входит ли запись в период для анализа активности
                        if row['year'] >= recent_activity_year:
                            recent_payments_sum += row['dividend_value']
                        
                        # Сохраняем информацию о датах выплат внутри года
                        if row['record_date'] is not None and isinstance(row['record_date'], datetime):
                            month_day_key = (row['record_date'].month, row['record_date'].day)
                            if month_day_key not in annual_payment_dates:
                                annual_payment_dates[month_day_key] = []
                            
                            annual_payment_dates[month_day_key].append({
                                'dividend_value': row['dividend_value'],
                                'quarter': row['quarter'],
                                'year': row['year']
                            })
                        
                    # Если у нас есть информация о квартале
                    if row['quarter'] is not None:
                        quarter = row['quarter']
                        
                        if quarter not in ticker_quarters:
                            ticker_quarters[quarter] = []
                        
                        ticker_quarters[quarter].append({
                            'dividend_value': row['dividend_value'],
                            'record_date': row['record_date'],
                            'month': row['month'],
                            'year': row['year']
                        })
                
                # Сохраняем прогнозы с сайта доход (тип 1)
                elif row['forecast_type'] == 1:
                    # Ключ: год-квартал (если известны)
                    forecast_key = f"{row['year']}-{row['quarter']}" if row['year'] is not None and row['quarter'] is not None else None
                    
                    if forecast_key:
                        site_forecasts[forecast_key] = {
                            'dividend_value': row['dividend_value'],
                            'record_date': row['record_date'],
                            'month': row['month'],
                            'year': row['year'],
                            'quarter': row['quarter']
                        }
            
            # Для каждого квартала делаем прогноз
            ticker_forecast_count = 0
            
            # Проверяем, были ли выплаты за последние history_years лет
            if recent_payments_sum == 0:
                logger.info(f"Для {ticker} нет выплат за последние {history_years} лет, применяем критический сценарий")
                try:
                    # Создаем стандартные квартальные даты
                    quarterly_dates = [
                        (3, 15),   # Q1 - 15 марта
                        (6, 15),   # Q2 - 15 июня
                        (9, 15),   # Q3 - 15 сентября
                        (12, 15)   # Q4 - 15 декабря
                    ]
                    
                    # Создаем прогнозы для каждого квартала
                    for quarter, (month, day) in enumerate(quarterly_dates, 1):
                        # Дивиденд за квартал всегда равен нулю
                        quarterly_dividend = 0.0
                        
                        for future_year in range(current_year + 1, current_year + years + 1):
                            try:
                                future_year_int = int(future_year)
                                
                                try:
                                    forecast_date = datetime(future_year_int, month, day)
                                except ValueError:
                                    # Обработка невалидных дат
                                    if month == 2 and day > 28:
                                        if future_year_int % 4 == 0 and (future_year_int % 100 != 0 or future_year_int % 400 == 0):
                                            forecast_date = datetime(future_year_int, 2, 29)
                                        else:
                                            forecast_date = datetime(future_year_int, 2, 28)
                                    elif day > 30 and month in [4, 6, 9, 11]:
                                        forecast_date = datetime(future_year_int, month, 30)
                                    else:
                                        if month > 1:
                                            next_month = datetime(future_year_int, month, 1)
                                            forecast_date = next_month - timedelta(days=1)
                                        else:
                                            forecast_date = datetime(future_year_int - 1, 12, 31)
                                
                                # Добавляем прогноз для квартала
                                forecast_data.append({
                                    'ticker': ticker,
                                    'name': group['name'].iloc[0],
                                    'record_date': forecast_date,
                                    'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                                    'dividend_value': quarterly_dividend,
                                    'period': f"Q{quarter} {future_year}",
                                    'forecast_type': 2,  # Наш прогноз (тип 2)
                                    'year': future_year,
                                    'quarter': quarter,
                                    'month': forecast_date.month,
                                    'announcement_date': "no data",
                                    'forecast_strategy': "3.4 - Неактивная компания"  # Специальная маркировка для неактивных компаний
                                })
                                ticker_forecast_count += 1
                                
                            except Exception as e:
                                logger.error(f"Не удалось создать прогноз для неактивной компании {ticker} Q{quarter} {future_year}: {e}")
                                continue
                
                except Exception as e:
                    logger.error(f"Ошибка при создании прогноза для неактивной компании {ticker}: {e}")
            
            else:
                # Проверяем наличие квартальных данных
                has_quarterly_data = False
                for quarter, history in ticker_quarters.items():
                    if len(history) > 1:  # Требуем более одной записи для квартала
                        has_quarterly_data = True
                        break
                
                # Если у акции всего один исторический платеж, не используем стратегию 3.1
                if len(all_payments) <= 1:
                    has_quarterly_data = False
                
                # Если у нас есть достаточно квартальных данных, используем их для прогноза (стратегия 3.1)
                if has_quarterly_data:
                    for quarter, history in ticker_quarters.items():
                        if not history:
                            continue
                        
                        # Рассчитываем средний дивиденд за квартал
                        avg_dividend = sum(item['dividend_value'] for item in history) / len(history)
                        
                        # Определяем типичный месяц выплаты
                        months = [item['month'] for item in history 
                                 if pd.notnull(item['month'])]
                        
                        if not months:
                            # Если нет данных о месяце, используем текущий месяц
                            most_common_month = datetime.now().month
                        else:
                            most_common_month = int(max(set(months), key=months.count))
                        
                        # Определяем типичный день выплаты
                        days = [item['record_date'].day for item in history 
                               if isinstance(item['record_date'], datetime) 
                               and item['record_date'].month == most_common_month]
                        
                        if not days:
                            most_common_day = 15  # По умолчанию середина месяца
                        else:
                            most_common_day = int(sum(days) // len(days))
                        
                        logger.info(f"Для {ticker}, Q{quarter}: месяц={most_common_month}, день={most_common_day}, средний дивиденд={avg_dividend:.2f}")
                        
                        # Создаем прогнозы на будущие периоды
                        for future_year in range(current_year + 1, current_year + years + 1):
                            try:
                                # Проверяем, есть ли прогноз с сайта доход на этот год и квартал
                                forecast_key = f"{future_year}-{quarter}"
                                site_forecast = site_forecasts.get(forecast_key)
                                
                                # Создаем дату закрытия реестра (убедимся, что все значения целочисленные)
                                future_year_int = int(future_year)
                                month_int = int(most_common_month)
                                day_int = int(most_common_day)
                                
                                # Если есть прогноз сайта, используем его дату, иначе генерируем свою
                                if site_forecast and site_forecast['record_date']:
                                    forecast_date = site_forecast['record_date']
                                    # Используем дивиденд из прогноза сайта
                                    dividend_value = site_forecast['dividend_value']
                                else:
                                    try:
                                        forecast_date = datetime(future_year_int, month_int, day_int)
                                        # Используем наш расчетный дивиденд
                                        dividend_value = round(avg_dividend, 2)
                                    except ValueError:
                                        # Обработка ошибок с невалидными датами
                                        if month_int == 2 and day_int > 28:
                                            # Для февраля используем последний день месяца
                                            if future_year_int % 4 == 0 and (future_year_int % 100 != 0 or future_year_int % 400 == 0):
                                                # Високосный год
                                                forecast_date = datetime(future_year_int, 2, 29)
                                            else:
                                                # Не високосный год
                                                forecast_date = datetime(future_year_int, 2, 28)
                                        elif day_int > 30 and month_int in [4, 6, 9, 11]:
                                            # Для месяцев с 30 днями
                                            forecast_date = datetime(future_year_int, month_int, 30)
                                        else:
                                            # Для других случаев используем предыдущий месяц
                                            if month_int > 1:
                                                next_month = datetime(future_year_int, month_int, 1)
                                                forecast_date = next_month - timedelta(days=1)
                                            else:
                                                forecast_date = datetime(future_year_int - 1, 12, 31)
                                        dividend_value = round(avg_dividend, 2)
                            
                                # Добавляем прогноз
                                forecast_data.append({
                                    'ticker': ticker,
                                    'name': group['name'].iloc[0],
                                    'record_date': forecast_date,
                                    'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                                    'dividend_value': dividend_value,
                                    'period': f"Q{quarter} {future_year}",
                                    'forecast_type': 2,  # Наш прогноз (тип 2)
                                    'year': future_year,
                                    'quarter': quarter,
                                    'month': forecast_date.month,
                                    'announcement_date': "no data",
                                    'forecast_strategy': "3.1 - Квартальные данные"  # Стратегия 3.1 из документации
                                })
                                ticker_forecast_count += 1
                                
                            except Exception as e:
                                logger.error(f"Не удалось создать прогноз для {ticker}, Q{quarter} {future_year}: {e}")
                                continue
                
                # Если у нас нет квартальных данных, но есть исторические даты выплат внутри года
                if ticker_forecast_count == 0 and annual_payment_dates:
                    logger.info(f"Для {ticker} используем исторические даты выплат внутри года для прогноза")
                    
                    # Для каждой уникальной даты внутри года делаем прогноз
                    for (month, day), payments in annual_payment_dates.items():
                        try:
                            # Считаем средний дивиденд для этой даты
                            avg_dividend = sum(item['dividend_value'] for item in payments) / len(payments)
                            
                            # Определяем квартал (если возможно)
                            quarters = [item['quarter'] for item in payments if item['quarter'] is not None]
                            if quarters:
                                # Берем наиболее частый квартал
                                quarter = max(set(quarters), key=quarters.count)
                            else:
                                # Определяем квартал по месяцу
                                quarter = (month - 1) // 3 + 1
                            
                            logger.info(f"Для {ticker}, дата {month}-{day}: квартал={quarter}, средний дивиденд={avg_dividend:.2f}")
                            
                            # Создаем прогнозы на будущие периоды
                            for future_year in range(current_year + 1, current_year + years + 1):
                                try:
                                    future_year_int = int(future_year)
                                    
                                    try:
                                        # Создаем дату закрытия реестра
                                        forecast_date = datetime(future_year_int, month, day)
                                    except ValueError:
                                        # Обработка невалидных дат
                                        if month == 2 and day > 28:
                                            if future_year_int % 4 == 0 and (future_year_int % 100 != 0 or future_year_int % 400 == 0):
                                                forecast_date = datetime(future_year_int, 2, 29)
                                            else:
                                                forecast_date = datetime(future_year_int, 2, 28)
                                        elif day > 30 and month in [4, 6, 9, 11]:
                                            forecast_date = datetime(future_year_int, month, 30)
                                        else:
                                            if month > 1:
                                                next_month = datetime(future_year_int, month, 1)
                                                forecast_date = next_month - timedelta(days=1)
                                            else:
                                                forecast_date = datetime(future_year_int - 1, 12, 31)
                                    
                                    dividend_value = round(avg_dividend, 2)
                                    
                                    # Добавляем прогноз
                                    forecast_data.append({
                                        'ticker': ticker,
                                        'name': group['name'].iloc[0],
                                        'record_date': forecast_date,
                                        'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                                        'dividend_value': dividend_value,
                                        'period': f"Q{quarter} {future_year}",
                                        'forecast_type': 2,  # Наш прогноз (тип 2)
                                        'year': future_year,
                                        'quarter': quarter,
                                        'month': forecast_date.month,
                                        'announcement_date': "no data",
                                        'forecast_strategy': "3.2 - Даты выплат"  # Стратегия 3.2 из документации
                                    })
                                    ticker_forecast_count += 1
                                
                                except Exception as e:
                                    logger.error(f"Не удалось создать прогноз для {ticker} на дату {month}-{day} {future_year}: {e}")
                                    continue
                        
                        except Exception as e:
                            logger.error(f"Ошибка при анализе исторических дат выплат для {ticker}: {e}")
                
                # Если все еще нет прогнозов, используем годовые данные
                if ticker_forecast_count == 0 and len(all_payments) > 0:
                    try:
                        logger.info(f"Для {ticker} недостаточно данных о датах выплат, создаем годовой прогноз")
                        
                        # Рассчитываем средний дивиденд
                        avg_dividend = sum(item['dividend_value'] for item in all_payments) / len(all_payments)
                        
                        # Определяем типичные даты выплат по кварталам
                        quarterly_dates = [
                            (3, 15),   # Q1 - 15 марта
                            (6, 15),   # Q2 - 15 июня
                            (9, 15),   # Q3 - 15 сентября
                            (12, 15)   # Q4 - 15 декабря
                        ]
                        
                        # Создаем прогнозы для каждого квартала
                        for quarter, (month, day) in enumerate(quarterly_dates, 1):
                            # Дивиденд за квартал - делим годовой на 4 квартала
                            quarterly_dividend = round(avg_dividend / 4, 2)
                            
                            for future_year in range(current_year + 1, current_year + years + 1):
                                try:
                                    future_year_int = int(future_year)
                                    
                                    try:
                                        forecast_date = datetime(future_year_int, month, day)
                                    except ValueError:
                                        # Обработка невалидных дат
                                        if month == 2 and day > 28:
                                            if future_year_int % 4 == 0 and (future_year_int % 100 != 0 or future_year_int % 400 == 0):
                                                forecast_date = datetime(future_year_int, 2, 29)
                                            else:
                                                forecast_date = datetime(future_year_int, 2, 28)
                                        elif day > 30 and month in [4, 6, 9, 11]:
                                            forecast_date = datetime(future_year_int, month, 30)
                                        else:
                                            if month > 1:
                                                next_month = datetime(future_year_int, month, 1)
                                                forecast_date = next_month - timedelta(days=1)
                                            else:
                                                forecast_date = datetime(future_year_int - 1, 12, 31)
                                    
                                    # Добавляем прогноз для квартала
                                    forecast_data.append({
                                        'ticker': ticker,
                                        'name': group['name'].iloc[0],
                                        'record_date': forecast_date,
                                        'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                                        'dividend_value': quarterly_dividend,
                                        'period': f"Q{quarter} {future_year}",
                                        'forecast_type': 2,  # Наш прогноз (тип 2)
                                        'year': future_year,
                                        'quarter': quarter,
                                        'month': forecast_date.month,
                                        'announcement_date': "no data",
                                        'forecast_strategy': "3.3 - Годовые данные"  # Стратегия 3.3 из документации
                                    })
                                    ticker_forecast_count += 1
                                    
                                except Exception as e:
                                    logger.error(f"Не удалось создать квартальный прогноз для {ticker} Q{quarter} {future_year}: {e}")
                                    continue
                        
                    except Exception as e:
                        logger.error(f"Ошибка при создании годового прогноза для {ticker}: {e}")
                        
                        # Если квартальные прогнозы не получились, делаем хотя бы один годовой прогноз
                        try:
                            # Определяем среднюю дату выплаты
                            months = [item['month'] for item in all_payments if pd.notnull(item['month'])]
                            if not months:
                                most_common_month = 6  # По умолчанию середина года
                            else:
                                most_common_month = int(max(set(months), key=months.count))
                            
                            # Определяем типичный день выплаты
                            days = [item['record_date'].day for item in all_payments 
                                  if isinstance(item['record_date'], datetime) 
                                  and pd.notnull(item['record_date'])]
                            
                            if not days:
                                most_common_day = 15  # По умолчанию середина месяца
                            else:
                                most_common_day = int(sum(days) // len(days))
                            
                            # Рассчитываем годовой дивиденд
                            yearly_dividend = avg_dividend
                            
                            # Создаем годовые прогнозы
                            for future_year in range(current_year + 1, current_year + years + 1):
                                try:
                                    future_year_int = int(future_year)
                                    month_int = int(most_common_month)
                                    day_int = int(most_common_day)
                                    
                                    try:
                                        forecast_date = datetime(future_year_int, month_int, day_int)
                                    except ValueError:
                                        # Обработка невалидных дат
                                        if month_int == 2 and day_int > 28:
                                            if future_year_int % 4 == 0 and (future_year_int % 100 != 0 or future_year_int % 400 == 0):
                                                forecast_date = datetime(future_year_int, 2, 29)
                                            else:
                                                forecast_date = datetime(future_year_int, 2, 28)
                                        elif day_int > 30 and month_int in [4, 6, 9, 11]:
                                            forecast_date = datetime(future_year_int, month_int, 30)
                                        else:
                                            if month_int > 1:
                                                next_month = datetime(future_year_int, month_int, 1)
                                                forecast_date = next_month - timedelta(days=1)
                                            else:
                                                forecast_date = datetime(future_year_int - 1, 12, 31)
                                    
                                    # Добавляем годовой прогноз
                                    forecast_data.append({
                                        'ticker': ticker,
                                        'name': group['name'].iloc[0],
                                        'record_date': forecast_date,
                                        'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                                        'dividend_value': round(yearly_dividend, 2),
                                        'period': f"Год {future_year}",
                                        'forecast_type': 2,  # Наш прогноз (тип 2)
                                        'year': future_year,
                                        'quarter': None,  # Годовой прогноз без квартала
                                        'month': forecast_date.month,
                                        'announcement_date': "no data",
                                        'forecast_strategy': "3.3 - Единый годовой"  # Вариант стратегии 3.3
                                    })
                                    ticker_forecast_count += 1
                                    
                                except Exception as e:
                                    logger.error(f"Не удалось создать годовой прогноз для {ticker} {future_year}: {e}")
                                    continue
                        
                        except Exception as e:
                            logger.error(f"Ошибка при создании единого годового прогноза для {ticker}: {e}")
                            
                # Если всё еще нет прогнозов, создаем аварийный прогноз
                if ticker_forecast_count == 0:
                    try:
                        logger.info(f"Для {ticker} создаем аварийный прогноз")
                        
                        # Стандартные квартальные даты
                        quarterly_dates = [
                            (3, 15),   # Q1 - 15 марта
                            (6, 15),   # Q2 - 15 июня
                            (9, 15),   # Q3 - 15 сентября
                            (12, 15)   # Q4 - 15 декабря
                        ]
                        
                        # Создаем прогнозы для каждого квартала
                        for quarter, (month, day) in enumerate(quarterly_dates, 1):
                            # Дивиденд за квартал равен нулю
                            quarterly_dividend = 0.0
                            
                            for future_year in range(current_year + 1, current_year + years + 1):
                                try:
                                    future_year_int = int(future_year)
                                    
                                    try:
                                        forecast_date = datetime(future_year_int, month, day)
                                    except ValueError:
                                        # Обработка невалидных дат
                                        if month == 2 and day > 28:
                                            if future_year_int % 4 == 0 and (future_year_int % 100 != 0 or future_year_int % 400 == 0):
                                                forecast_date = datetime(future_year_int, 2, 29)
                                            else:
                                                forecast_date = datetime(future_year_int, 2, 28)
                                        elif day > 30 and month in [4, 6, 9, 11]:
                                            forecast_date = datetime(future_year_int, month, 30)
                                        else:
                                            if month > 1:
                                                next_month = datetime(future_year_int, month, 1)
                                                forecast_date = next_month - timedelta(days=1)
                                            else:
                                                forecast_date = datetime(future_year_int - 1, 12, 31)
                                    
                                    # Добавляем прогноз для квартала
                                    forecast_data.append({
                                        'ticker': ticker,
                                        'name': group['name'].iloc[0],
                                        'record_date': forecast_date,
                                        'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                                        'dividend_value': quarterly_dividend,
                                        'period': f"Q{quarter} {future_year}",
                                        'forecast_type': 2,  # Наш прогноз (тип 2)
                                        'year': future_year,
                                        'quarter': quarter,
                                        'month': forecast_date.month,
                                        'announcement_date': "no data",
                                        'forecast_strategy': "3.4 - Аварийная стратегия"  # Стратегия 3.4 из документации
                                    })
                                    ticker_forecast_count += 1
                                    
                                except Exception as e:
                                    logger.error(f"Не удалось создать аварийный прогноз для {ticker} Q{quarter} {future_year}: {e}")
                                    continue
                        
                    except Exception as e:
                        logger.error(f"Ошибка при создании аварийного прогноза для {ticker}: {e}")
            
            forecasts_by_ticker[ticker] = ticker_forecast_count
        
        # Выводим статистику по прогнозам
        for ticker, count in forecasts_by_ticker.items():
            logger.info(f"Создано {count} прогнозов для {ticker}")
        
        # Создаем DataFrame с прогнозами
        if forecast_data:
            forecast_df = pd.DataFrame(forecast_data)
            
            # Определяем общие столбцы для объединения
            common_cols = list(set(df.columns) & set(forecast_df.columns))
            
            # Объединяем исходные данные и прогнозы
            result_df = pd.concat([df[common_cols], forecast_df[common_cols]], ignore_index=True)
            
            # Сортируем по тикеру и дате
            result_df = result_df.sort_values(['ticker', 'record_date'])
            
            self.result_df = result_df
            logger.info(f"Прогнозирование завершено, всего записей: {len(result_df)}")
        else:
            logger.warning("Не удалось создать прогнозы")
            self.result_df = df.copy()
            
        return self.result_df

    def save_to_excel(self, filename="dividend_forecast.xlsx"):
        """Сохранение результатов в Excel"""
        logger.info(f"Сохраняем результаты в файл {filename}")
        
        if self.result_df.empty:
            logger.warning("Нет данных для сохранения")
            return None
        
        # Выбираем нужные столбцы и переименовываем их
        output_columns = ['ticker', 'name', 'announcement_date', 'record_date_str', 
                          'year', 'quarter', 'dividend_value', 'forecast_type', 'period', 'forecast_strategy']
        available_columns = [col for col in output_columns if col in self.result_df.columns]
        
        output_df = self.result_df[available_columns].copy()
        
        # Преобразуем числовой тип прогноза в более понятный текст
        output_df['forecast_type_str'] = output_df['forecast_type'].apply(
            lambda x: 'Факт' if x == 0 else ('Прогноз сайта' if x == 1 else 'Наш прогноз')
        )
        
        column_mapping = {
            'ticker': 'Тикер',
            'name': 'Актив',
            'announcement_date': 'Дата объявления дивидендов',
            'record_date_str': 'Дата закрытия реестра',
            'year': 'Год для учета дивиденда',
            'quarter': 'Квартал',
            'dividend_value': 'Дивиденд',
            'forecast_type': 'Тип прогноза (код)',
            'forecast_type_str': 'Тип прогноза',
            'period': 'Период',
            'forecast_strategy': 'Стратегия прогноза'
        }
        
        # Переименовываем только те столбцы, которые есть в выходных данных
        rename_mapping = {k: v for k, v in column_mapping.items() if k in output_df.columns}
        output_df = output_df.rename(columns=rename_mapping)
        
        # Сохраняем в Excel
        output_df.to_excel(filename, index=False)
        logger.info(f"Данные успешно сохранены в файл {filename}")
        
        return filename

    def save_to_json(self, filename="dividend_forecast.json"):
        """Сохранение результатов в JSON"""
        logger.info(f"Сохраняем результаты в файл JSON {filename}")
        
        if self.result_df.empty:
            logger.warning("Нет данных для сохранения")
            return None
        
        # Создаем копию с дополнительной информацией
        json_data = self.result_df.copy()
        
        # Преобразуем числовой тип прогноза в текстовый
        json_data['forecast_type_str'] = json_data['forecast_type'].apply(
            lambda x: 'Факт' if x == 0 else ('Прогноз сайта' if x == 1 else 'Наш прогноз')
        )
        
        # Конвертируем даты в строки для JSON
        for col in json_data.columns:
            if json_data[col].dtype == 'datetime64[ns]' or json_data[col].apply(lambda x: isinstance(x, datetime)).any():
                json_data[col] = json_data[col].apply(
                    lambda x: x.strftime('%d.%m.%Y') if pd.notnull(x) and isinstance(x, datetime) else None
                )
        
        # Конвертируем DataFrame в список словарей
        records = json_data.to_dict(orient='records')
        
        # Сохраняем в JSON файл
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Данные успешно сохранены в файл JSON {filename}")
        return filename

def main():
    """Основная функция запуска парсера"""
    parser = DividendParser()
    
    # Парсим основную страницу для получения списка акций
    if parser.parse_main_page():
        # Обрабатываем все акции без ограничения
        max_stocks = None
        
        # Парсим детальную информацию по каждой акции
        if parser.parse_stock_details(max_stocks):
            # Очищаем данные
            parser.clean_data()
            
            # Делаем прогноз на 10 лет вперед, используя данные за 3 года
            parser.forecast_dividends(years=10, history_years=3)
            
            # Сохраняем результаты в Excel
            excel_file = parser.save_to_excel()
            
            # Сохраняем результаты в JSON
            json_file = parser.save_to_json()
            
            if excel_file:
                print(f"Обработка завершена, результаты сохранены в файл {excel_file}")
            if json_file:
                print(f"Данные также сохранены в JSON формате: {json_file}")
        else:
            print("Ошибка при парсинге детальной информации об акциях")
    else:
        print("Ошибка при парсинге основной страницы")

if __name__ == "__main__":
    main() 