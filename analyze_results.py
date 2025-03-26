#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pandas as pd
from collections import Counter

def analyze_results(filename="dividend_forecast.json"):
    # Загружаем данные
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Всего записей: {len(data)}")
    
    # Подсчитываем записи по тикерам
    ticker_counts = Counter([item['ticker'] for item in data])
    print("\nЗаписи по тикерам:")
    for ticker, count in ticker_counts.items():
        print(f"{ticker}: {count}")
    
    # Подсчитываем типы прогнозов
    forecast_types = Counter([item.get('forecast_type_str', 'Неизвестно') for item in data])
    print("\nТипы прогнозов:")
    for forecast_type, count in forecast_types.items():
        print(f"{forecast_type}: {count}")
    
    # Подсчитываем стратегии прогнозирования
    strategy_types = Counter([item.get('forecast_strategy', 'Неизвестно') for item in data])
    print("\nСтратегии прогнозирования:")
    for strategy, count in strategy_types.items():
        print(f"{strategy}: {count}")
    
    # Анализируем годы
    years = [item.get('year') for item in data if item.get('year') is not None]
    if years:
        min_year = min(years)
        max_year = max(years)
        print(f"\nДиапазон лет: с {min_year} по {max_year}")
    
    # Анализируем размеры дивидендов
    dividends = [float(item.get('dividend_value')) for item in data if item.get('dividend_value') is not None]
    if dividends:
        avg_dividend = sum(dividends) / len(dividends)
        max_dividend = max(dividends)
        min_dividend = min(dividends)
        print(f"\nАнализ дивидендов:")
        print(f"Средний: {avg_dividend:.2f}")
        print(f"Максимальный: {max_dividend:.2f}")
        print(f"Минимальный: {min_dividend:.2f}")
    
    # Анализ дивидендов по стратегиям прогнозирования
    strategy_dividends = {}
    for item in data:
        strategy = item.get('forecast_strategy', 'Неизвестно')
        dividend = float(item.get('dividend_value', 0))
        
        if strategy not in strategy_dividends:
            strategy_dividends[strategy] = []
        
        strategy_dividends[strategy].append(dividend)
    
    print("\nСредние дивиденды по стратегиям:")
    for strategy, dividends in strategy_dividends.items():
        if dividends:
            avg = sum(dividends) / len(dividends)
            print(f"{strategy}: {avg:.2f}")
    
    # Выводим примеры данных
    print("\nПримеры данных:")
    for i, item in enumerate(data[:3]):
        print(f"Запись {i+1}:")
        for key, value in item.items():
            print(f"  {key}: {value}")
        print()

if __name__ == "__main__":
    analyze_results() 