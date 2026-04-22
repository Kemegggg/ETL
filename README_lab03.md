# Лабораторная работа №3 - Интеграция данных из нескольких источников

**Вариант №10:** Интеграция данных технической поддержки

---

## Цель и задачи

Получить практические навыки интеграции, обработки и согласования данных из различных источников с использованием Python и его библиотек.

Задачи: чтение данных из трёх разнородных источников, очистка и согласование форматов, объединение в единый датафрейм, анализ качества технической поддержки, сохранение результатов в PostgreSQL.

---

## Источники данных

| Источник | Тип | Строк | Колонок |
|---|---|---|---|
| support_tickets | PostgreSQL | 120 | 10 |
| service_regulations.xlsx | Excel | 25 | 5 |
| support_quality.csv | CSV | 60 | 5 |

<img width="1187" height="233" alt="image-100" src="https://github.com/user-attachments/assets/d4d41d17-652d-47c7-ad6e-70128e66ed6c" />
*Терминал: `docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT * FROM support_tickets LIMIT 10;"`*

<img width="339" height="449" alt="image-101" src="https://github.com/user-attachments/assets/345b703f-3fba-4e98-8917-2906a20b9986" />
*Открытый файл service_regulations.xlsx — видны колонки: regulation_id, category, response_time_hours, resolution_time_hours, priority*

<img width="641" height="191" alt="image-102" src="https://github.com/user-attachments/assets/fa090bfc-42bd-44e7-9fdd-7859c03a7fc7" />
*Терминал: `head -10 ~/Downloads/lab_03/support_quality.csv`*

---

## ETL скрипт

Скрипт `etl_support.py` реализует шесть последовательных шагов:

1. **Чтение** — PostgreSQL через SQLAlchemy, Excel через openpyxl, CSV через pandas
2. **Очистка** — удаление дублей, заполнение пропусков, приведение типов дат и чисел
3. **Объединение** — агрегация регламентов по категории, оценок по агенту, outer merge
4. **Анализ** — рейтинг агентов, время решения vs SLA, процент выполнения регламента
5. **Визуализация** — 4 графика сохранены в папку plots/
6. **Сохранение** — итоговая таблица записана в PostgreSQL как integrated_support_data

```python
"""
Лабораторная работа 3. Вариант 10.
ETL: интеграция данных технической поддержки.
Источники: PostgreSQL (support_tickets) + Excel (service_regulations) + CSV (support_quality)
"""
import os
import logging
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# ─── Конфигурация ─────────────────────────────────────────────────────────────
DB_URL    = 'postgresql://etl_user:etl_pass@localhost:5432/etl_db'
BASE_DIR  = os.path.expanduser('~/Downloads/lab_03')
XLSX_PATH = os.path.join(BASE_DIR, 'service_regulations.xlsx')
CSV_PATH  = os.path.join(BASE_DIR, 'support_quality.csv')
PLOT_DIR  = os.path.join(BASE_DIR, 'plots')
os.makedirs(PLOT_DIR, exist_ok=True)

# ─── Логирование ──────────────────────────────────────────────────────────────
log_path = os.path.join(BASE_DIR, 'etl_support.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def read_data():
    log.info("ШАГ 1: Чтение данных из источников")
    engine = create_engine(DB_URL)
    df_pg  = pd.read_sql("SELECT * FROM support_tickets", engine)
    df_xl  = pd.read_excel(XLSX_PATH, sheet_name='Regulations')
    df_csv = pd.read_csv(CSV_PATH)
    return df_pg, df_xl, df_csv

def clean_df(df, name):
    log.info(f"Очистка [{name}]")
    df = df.drop_duplicates()
    df[df.select_dtypes(include='number').columns] = \
        df.select_dtypes(include='number').fillna(0)
    df[df.select_dtypes(include='object').columns] = \
        df.select_dtypes(include='object').fillna('Unknown')
    for col in df.columns:
        if col.endswith('_at') or 'date' in col.lower():
            if df[col].dtype == object:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def merge_data(df_pg, df_xl, df_csv):
    log.info("ШАГ 3: Объединение данных")
    reg_agg     = df_xl.groupby('category').agg(
        reg_response_hours=('response_time_hours','mean'),
        reg_resolution_hours=('resolution_time_hours','mean')).reset_index()
    quality_agg = df_csv.groupby('agent_id').agg(
        avg_rating=('rating','mean'),
        rating_count=('rating','count')).reset_index()
    merged = pd.merge(df_pg, reg_agg, on='category', how='outer')
    merged = pd.merge(merged, quality_agg, on='agent_id', how='outer', indicator=True)
    merged['in_time'] = (
        (merged['status'] == 'Resolved') &
        (merged['resolution_time_hours'] <= merged['reg_resolution_hours'])
    ).astype(int)
    merged = merged.drop(columns=['_merge'])
    return merged

def analyze(merged):
    log.info("ШАГ 4: Анализ")
    agent_rating = merged.dropna(subset=['avg_rating']).groupby('agent_id')['avg_rating']\
        .mean().sort_values(ascending=False).round(2)
    time_by_cat  = merged.groupby('category').agg(
        avg_resolution=('resolution_time_hours','mean'),
        reg_resolution=('reg_resolution_hours','mean'),
        ticket_count=('ticket_id','count')).round(2)
    resolved    = merged[merged['status'] == 'Resolved']
    pct_intime  = round(resolved['in_time'].sum() / len(resolved) * 100, 1) \
        if len(resolved) else 0
    return {'agent_rating': agent_rating, 'time_by_cat': time_by_cat,
            'pct_intime': pct_intime, 'top3': agent_rating.head(3),
            'prio_dist': merged['priority'].value_counts()}

def main():
    df_pg, df_xl, df_csv = read_data()
    df_pg, df_xl, df_csv = (clean_df(d, n) for d, n in
        zip([df_pg, df_xl, df_csv],
            ['support_tickets','service_regulations','support_quality']))
    merged  = merge_data(df_pg, df_xl, df_csv)
    results = analyze(merged)
    # visualize + save_to_db ...

if __name__ == '__main__':
    main()
```

<img width="1376" height="824" alt="image-103" src="https://github.com/user-attachments/assets/9f33c263-5b53-45f4-9535-84a7c3f11ec7" />
*Терминал: `python etl_support.py` — вывод логов всех 6 шагов*


<img width="719" height="174" alt="image-104" src="https://github.com/user-attachments/assets/1330b7d8-3e78-4156-a5e7-4fc1e7bdd18d" />
*Терминал: `cat ~/Downloads/lab_03/etl_support.log` — временные метки и статус каждого шага*

---

## Результаты интеграции

После объединения трёх источников получена итоговая таблица **120 строк, 15 колонок**, сохранённая в PostgreSQL как `integrated_support_data`.

<img width="1469" height="406" alt="image-105" src="https://github.com/user-attachments/assets/4f9b6b84-16a1-4d83-89eb-ef2167ef048d" />
*Терминал: `docker exec postgres_etl psql -U etl_user -d etl_db -c "SELECT * FROM integrated_support_data LIMIT 10;"`*

<img width="199" height="87" alt="image-106" src="https://github.com/user-attachments/assets/a3e849d4-9eff-4f2c-b55e-d7a854768533" />
*Итоговая статистика в терминале: строки по источникам, время выполнения*

---

## Анализ качества технической поддержки

**Ключевые показатели:**
- % тикетов закрытых в срок: **44.4%** (16 из 36 resolved)
- Топ агенты: AGT003 (3.80 ★), AGT008 (3.50 ★), AGT007 (3.25 ★)
- Распределение: High — 44, Medium — 41, Low — 35 тикетов

**Нарушения SLA по категориям:**

| Категория | Факт (ч) | Регламент (ч) | Нарушение |
|---|---|---|---|
| Billing | 9.96 | 8.94 | ❌ +1.02ч |
| General | 41.74 | 38.00 | ❌ +3.74ч |
| Hardware | 31.86 | 25.76 | ❌ +6.10ч |
| Network | 14.42 | 11.40 | ❌ +3.02ч |
| Technical | 19.01 | 20.34 | ✅ в срок |

<img width="1200" height="600" alt="image-107" src="https://github.com/user-attachments/assets/fe574ee0-841a-40bf-b0db-33d5f95a53b7" />
*Файл: plots/plot1_agent_rating.png — горизонтальный bar chart, красная линия = среднее*

<img width="1200" height="600" alt="image-108" src="https://github.com/user-attachments/assets/da39624c-6c76-4322-b8b1-492b3db24280" />
*Файл: plots/plot2_resolution_time.png — сгруппированный bar chart факт vs регламент*

<img width="840" height="840" alt="image-109" src="https://github.com/user-attachments/assets/2fa95cc0-f7c1-492f-85cd-807257bc5dd1" /
*Файл: plots/plot3_priority_pie.png — круговая диаграмма приоритетов*

<img width="1440" height="600" alt="image-110" src="https://github.com/user-attachments/assets/5574c966-45d7-4483-8b30-fcf25d92310e" />
*Файл: plots/plot4_daily_trend.png — линейный график по датам 2024*

---

## Структура проекта
<img width="872" height="842" alt="image-111" src="https://github.com/user-attachments/assets/fd2e4f1b-2b90-40f0-a4f8-50262cc8dc5f" />

<img width="709" height="665" alt="image-112" src="https://github.com/user-attachments/assets/c267c395-32fb-4d3d-bd71-311989b30578" />


```
lab_03/
├── generate_data.py         — генератор тестовых данных
├── etl_support.py           — основной ETL скрипт
├── etl_support.log          — лог выполнения
├── support_quality.csv      — источник CSV (60 строк)
├── service_regulations.xlsx — источник Excel (25 строк)
└── plots/
    ├── plot1_agent_rating.png
    ├── plot2_resolution_time.png
    ├── plot3_priority_pie.png
    └── plot4_daily_trend.png
```

---

## Выводы

- Освоены методы чтения данных из трёх разнородных источников: PostgreSQL (SQLAlchemy), Excel (openpyxl), CSV (pandas).
- Реализована очистка данных: удаление дублей, заполнение пропусков, приведение типов.
- Данные успешно объединены через outer merge по полям `category` и `agent_id` — итого 120 строк, 15 колонок.
- Анализ выявил нарушение SLA в 4 из 5 категорий — наибольшее отклонение в Hardware (+6.1ч).
- Только 44.4% тикетов закрыты в срок — рекомендуется усилить контроль по категориям General и Hardware.
- Лучший агент по рейтингу — AGT003 (3.80★), худший — AGT006 (1.60★).
