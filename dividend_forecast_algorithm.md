# Алгоритм прогнозирования дивидендных выплат

## Общее описание

Данный алгоритм используется для прогнозирования дивидендных выплат на основе исторических данных. Он анализирует прошлые выплаты дивидендов компаний и создает прогнозы на будущие периоды (до 10 лет вперед). Алгоритм учитывает сезонность выплат, исторические размеры дивидендов и структуру выплат по кварталам.

## Входные данные

- **Исторические данные о дивидендах**: информация о прошлых выплатах (дата, сумма, период)
- **Прогнозы с сайта**: прогнозы дивидендов с сайта dohod.ru
- **Список акций**: список компаний для анализа
- **Параметры прогнозирования**: 
  - `years`: количество лет для прогнозирования (по умолчанию 10)
  - `history_years`: количество лет для проверки активности компании (по умолчанию 3)

## Основные шаги алгоритма

### 1. Сбор исторических данных

- Парсинг сайта с информацией о дивидендах (dohod.ru)
- Анализ таблиц с историческими выплатами
- Выделение информации из таблицы "Все выплаты" (игнорирование таблицы "Совокупные выплаты по годам")
- Структурирование данных о фактических выплатах дивидендов
- Сбор прогнозов с сайта для будущих периодов

### 2. Предварительная обработка данных

- Очистка данных от дубликатов и некорректных значений
- Извлечение и преобразование дат
- Определение квартала для каждой выплаты
- Извлечение размера дивиденда
- Разделение данных на фактические значения и прогнозы с сайта
- Добавление поля `forecast_strategy` для всех записей:

```python
# Добавляем поле стратегии прогноза для всех записей
df['forecast_strategy'] = df['forecast_type'].apply(
    lambda x: "0 - Фактические данные" if x == 0 else "1 - Прогноз сайта"
)
```

### 3. Основной алгоритм прогнозирования

Алгоритм использует каскадный подход, применяя следующие стратегии в порядке приоритета:

#### 3.0. Добавление прогнозов с сайта для текущего года

В начале алгоритма добавляются прогнозы с сайта для текущего года:

```python
# Сначала добавляем прогнозы с сайта для текущего года
if site_forecasts:
    logger.info(f"Для {ticker} есть прогнозы с сайта, проверяем прогнозы для текущего года")
    
    # Добавляем прогнозы для текущего года с сайта
    for forecast_key, site_forecast in site_forecasts.items():
        try:
            forecast_year = site_forecast['year']
            quarter = site_forecast['quarter']
            
            # Добавляем только прогнозы для текущего года
            if forecast_year == current_year:
                forecast_date = site_forecast['record_date']
                
                # Добавляем прогноз на основе данных с сайта
                forecast_data.append({
                    'ticker': ticker,
                    'name': group['name'].iloc[0],
                    'record_date': forecast_date,
                    'record_date_str': forecast_date.strftime('%d.%m.%Y') if forecast_date else "no data",
                    'dividend_value': site_forecast['dividend_value'],
                    'period': f"Q{quarter} {forecast_year}" if quarter else f"{forecast_year}",
                    'forecast_type': 2,  # Наш прогноз (тип 2)
                    'year': forecast_year,
                    'quarter': quarter,
                    'month': forecast_date.month if forecast_date else None,
                    'announcement_date': "no data",
                    'forecast_strategy': "3.0 - Прогноз на основе данных сайта (текущий год)"
                })
        except Exception as e:
            logger.error(f"Не удалось добавить прогноз с сайта для {ticker} на текущий год: {e}")
```

#### Проверка активности компании и прогнозов с сайта

После добавления прогнозов для текущего года, алгоритм проверяет:
1. Активность компании (наличие выплат за последние 3 года)
2. Наличие ненулевых прогнозов с сайта

Критический сценарий (нулевые дивиденды) применяется если:
- Нет выплат за последние 3 года
- ИЛИ все прогнозы с сайта нулевые

```python
# Проверяем, были ли выплаты за последние history_years лет и/или нулевые прогнозы с сайта
if recent_payments_sum == 0 or (site_forecasts and site_forecasts_sum == 0):
    logger_message = f"Для {ticker} "
    if recent_payments_sum == 0:
        logger_message += f"нет выплат за последние {history_years} лет"
        if site_forecasts and site_forecasts_sum == 0:
            logger_message += " или "
    
    if site_forecasts and site_forecasts_sum == 0:
        logger_message += "все прогнозы с сайта нулевые"
    
    logger_message += ", применяем критический сценарий"
    logger.info(logger_message)
    
    # Создаем стандартные квартальные даты
    quarterly_dates = [
        (3, 15),   # Q1 - 15 марта
        (6, 15),   # Q2 - 15 июня
        (9, 15),   # Q3 - 15 сентября
        (12, 15)   # Q4 - 15 декабря
    ]
    
    # Создаем прогнозы с нулевыми дивидендами для каждого квартала
    for quarter, (month, day) in enumerate(quarterly_dates, 1):
        for future_year in range(current_year + 1, current_year + years + 1):
            # ...создание прогноза с нулевым дивидендом...
            forecast_data.append({
                'ticker': ticker,
                'name': group['name'].iloc[0],
                'record_date': forecast_date,
                'record_date_str': forecast_date.strftime('%d.%m.%Y'),
                'dividend_value': 0.0,
                'period': f"Q{quarter} {future_year}",
                'forecast_type': 2,  # Наш прогноз (тип 2)
                'year': future_year,
                'quarter': quarter,
                'month': forecast_date.month,
                'announcement_date': "no data",
                'forecast_strategy': "3.4 - Неактивная компания"  # Стратегия для неактивных компаний
            })
```

#### 3.0. Добавление прогнозов с сайта для будущих лет

Если компания активна и есть ненулевые прогнозы с сайта, добавляются прогнозы с сайта для будущих лет:

```python
# Если были прогнозы с сайта для будущих лет, добавляем их как информацию
if site_forecasts and site_forecasts_sum > 0:
    logger.info(f"Для {ticker} есть ненулевые прогнозы с сайта на будущие годы, учитываем их при прогнозировании")
    
    # Создаем прогнозы на основе прогнозов с сайта для будущих лет (но не текущего)
    for forecast_key, site_forecast in site_forecasts.items():
        try:
            future_year = site_forecast['year']
            quarter = site_forecast['quarter']
            
            # Пропускаем текущий год, так как его уже обработали выше
            if future_year == current_year:
                continue
                
            forecast_date = site_forecast['record_date']
            
            # Добавляем прогноз на основе данных с сайта
            forecast_data.append({
                'ticker': ticker,
                'name': group['name'].iloc[0],
                'record_date': forecast_date,
                'record_date_str': forecast_date.strftime('%d.%m.%Y') if forecast_date else "no data",
                'dividend_value': site_forecast['dividend_value'],
                'period': f"Q{quarter} {future_year}" if quarter else f"{future_year}",
                'forecast_type': 2,  # Наш прогноз (тип 2)
                'year': future_year,
                'quarter': quarter,
                'month': forecast_date.month if forecast_date else None,
                'announcement_date': "no data",
                'forecast_strategy': "3.0 - Прогноз на основе данных сайта (будущие годы)"
            })
        except Exception as e:
            logger.error(f"Не удалось создать прогноз на основе данных сайта для {ticker}: {e}")
```

#### 3.1. Прогнозирование на основе квартальных данных

Если компания активна и для неё есть достаточно квартальных исторических данных:

1. **Группировка данных по кварталам**:
   - Для каждого квартала (Q1, Q2, Q3, Q4) собираются исторические данные
   - Расчет типичных дат выплат для каждого квартала

2. **Статистический анализ**:
   - Рассчитывается средний размер дивиденда для каждого квартала
   - Определяется наиболее типичный месяц выплаты
   - Определяется типичный день месяца для выплаты

3. **Создание прогнозов**:
   - Для каждого будущего года и каждого квартала создается прогноз
   - Используются рассчитанные средние значения дивидендов
   - Если есть прогноз с сайта на это же время, используется он, иначе генерируется собственный прогноз
   - Даты выплат определяются на основе исторического анализа

```python
# Если у нас есть достаточно квартальных данных, дополняем прогнозы (стратегия 3.1)
if has_quarterly_data:
    for quarter, history in ticker_quarters.items():
        if not history:
            continue
        
        # Рассчитываем средний дивиденд за квартал
        avg_dividend = sum(item['dividend_value'] for item in history) / len(history)
        
        # Определяем типичный месяц выплаты и типичный день выплаты
        # ...
        
        # Создаем прогнозы на будущие периоды
        for future_year in range(current_year + 1, current_year + years + 1):
            try:
                # Проверяем, есть ли прогноз с сайта доход на этот год и квартал
                forecast_key = f"{future_year}-{quarter}"
                site_forecast = site_forecasts.get(forecast_key)
                
                # Если есть прогноз сайта, используем его дату и дивиденд, иначе генерируем свой
                if site_forecast and site_forecast['record_date']:
                    forecast_date = site_forecast['record_date']
                    dividend_value = site_forecast['dividend_value']
                else:
                    # ... создание собственного прогноза ...
                    dividend_value = round(avg_dividend, 2)
                
                # Добавляем прогноз
                forecast_data.append({
                    # ... данные прогноза ...
                    'forecast_strategy': "3.1 - Квартальные данные" if site_forecast is None else "3.0 - Прогноз на основе данных сайта"
                })
            except Exception as e:
                logger.error(f"Не удалось создать прогноз для {ticker}, Q{quarter} {future_year}: {e}")
```

#### 3.2. Прогнозирование на основе дат выплат внутри года

Если квартальных данных недостаточно, но есть информация о датах выплат внутри года:

1. **Анализ дат выплат**:
   - Собираются все уникальные даты выплат (месяц-день)
   - Для каждой даты рассчитывается средний дивиденд

2. **Определение квартала по дате**:
   - Для каждой даты определяется соответствующий квартал
   - Если невозможно определить по историческим данным, используется расчет по месяцу

3. **Создание прогнозов**:
   - Для каждой исторической даты выплаты и каждого будущего года создается прогноз
   - Если есть прогноз с сайта на это же время, используется он, иначе генерируется собственный прогноз

```python
# Если у нас нет квартальных данных, но есть исторические даты выплат внутри года
if ticker_forecast_count == 0 and annual_payment_dates:
    logger.info(f"Для {ticker} используем исторические даты выплат внутри года для прогноза")
    
    # Для каждой уникальной даты внутри года делаем прогноз
    for (month, day), payments in annual_payment_dates.items():
        # ... расчет среднего дивиденда и определение квартала ...
        
        # Создаем прогнозы на будущие периоды
        for future_year in range(current_year + 1, current_year + years + 1):
            try:
                # Проверяем, есть ли прогноз с сайта доход на этот год и квартал
                forecast_key = f"{future_year}-{quarter}"
                site_forecast = site_forecasts.get(forecast_key)
                
                # Если есть прогноз сайта, используем его, иначе генерируем свой
                if site_forecast and site_forecast['record_date']:
                    forecast_date = site_forecast['record_date']
                    dividend_value = site_forecast['dividend_value']
                else:
                    # ... создание собственного прогноза ...
                    dividend_value = round(avg_dividend, 2)
                
                # Добавляем прогноз
                forecast_data.append({
                    # ... данные прогноза ...
                    'forecast_strategy': "3.2 - Даты выплат" if site_forecast is None else "3.0 - Прогноз на основе данных сайта"
                })
            except Exception as e:
                logger.error(f"Не удалось создать прогноз для {ticker} {future_year}-{month}-{day}: {e}")
```

#### 3.3. Прогнозирование на основе годовых данных

Если недостаточно информации о квартальных выплатах и датах внутри года:

1. **Расчет среднего годового дивиденда**:
   - Вычисляется средний дивиденд на основе всех исторических выплат

2. **Распределение по кварталам**:
   - Годовой дивиденд равномерно распределяется по кварталам (делится на 4)
   - Используются стандартные даты для каждого квартала
   - Если есть прогноз с сайта на это же время, используется он, иначе генерируется собственный прогноз

3. **Создание прогнозов**:
   - Для каждого квартала и будущего года создается прогноз

```python
# Если все еще нет прогнозов, используем годовые данные
if ticker_forecast_count == 0 and len(all_payments) > 0:
    logger.info(f"Для {ticker} недостаточно данных о датах выплат, создаем годовой прогноз")
    
    # Рассчитываем средний дивиденд
    avg_dividend = sum(item['dividend_value'] for item in all_payments) / len(all_payments)
    
    # ... создание прогнозов на основе годовых данных ...
```

#### 3.4. Аварийная стратегия

Если не удалось создать прогноз ни одним из предыдущих методов, применяется аварийная стратегия:

1. **Использование стандартных дат**:
   - Создаются стандартные квартальные даты (15 марта, 15 июня, 15 сентября, 15 декабря)

2. **Создание нулевых прогнозов**:
   - Для каждого квартала и будущего года создается прогноз с нулевым дивидендом

```python
# Если все еще нет прогнозов, применяем аварийную стратегию
if ticker_forecast_count == 0:
    logger.info(f"Для {ticker} создаем аварийный прогноз")
    
    # Стандартные квартальные даты
    quarterly_dates = [
        (3, 15),   # Q1 - 15 марта
        (6, 15),   # Q2 - 15 июня
        (9, 15),   # Q3 - 15 сентября
        (12, 15)   # Q4 - 15 декабря
    ]
    
    # Создаем прогнозы с нулевыми дивидендами для каждого квартала
    # ...
```

## Сводка стратегий прогнозирования

| Код | Название | Описание |
|-----|----------|----------|
| 0 | Фактические данные | Исторические выплаты дивидендов |
| 1 | Прогноз сайта | Прогнозы с сайта dohod.ru |
| 3.0 - Прогноз на основе данных сайта (текущий год) | Данные текущего года с сайта | Прогнозы для текущего года с сайта dohod.ru |
| 3.0 - Прогноз на основе данных сайта (будущие годы) | Данные будущих лет с сайта | Прогнозы для будущих лет с сайта dohod.ru |
| 3.1 - Квартальные данные | Квартальные данные | Прогнозы на основе исторических квартальных выплат |
| 3.2 - Даты выплат | Даты выплат | Прогнозы на основе исторических дат выплат внутри года |
| 3.3 - Годовые данные | Годовые данные | Прогнозы на основе средних годовых выплат |
| 3.4 - Неактивная компания | Критический сценарий | Нулевые прогнозы для неактивных компаний или при нулевых прогнозах с сайта |
| 3.5 - Аварийная стратегия | Аварийная стратегия | Нулевые прогнозы при отсутствии исторических данных |

## Приоритет стратегий

1. Добавление прогнозов с сайта для текущего года (3.0 - текущий год)
2. Критический сценарий при отсутствии выплат за последние 3 года или при нулевых прогнозах с сайта (3.4)
3. Добавление прогнозов с сайта для будущих лет (3.0 - будущие годы)
4. Прогнозирование на основе квартальных данных (3.1)
5. Прогнозирование на основе дат выплат внутри года (3.2)
6. Прогнозирование на основе годовых данных (3.3)
7. Аварийная стратегия (3.5)

## Выходные данные

- **Прогнозы дивидендов**: для каждой компании создаются прогнозы на будущие периоды
- **Данные о стратегии прогнозирования**: для каждого прогноза указывается использованная стратегия
- **Результаты сохраняются в форматах**:
  - Excel (dividend_forecast.xlsx)
  - JSON (dividend_forecast.json) 