# Лабораторная работа 2. Динамические соединения с базами данных

**Вариант 10**
**Фильтр:** только United States
**Доп. задание 1:** Анализ скидок
**Доп. задание 2:** Отчёт по регионам

---

## 1. Цель и задачи работы

**Цель работы:** получить практические навыки создания ETL-процесса для загрузки данных из CSV-файла в базу данных PostgreSQL с использованием Pentaho Data Integration (PDI).

**Задачи:**
- Создать динамические подключения к различным источникам данных
- Разработать процесс выявления и обработки дублирующихся записей
- Реализовать механизм объединения данных в единое хранилище
- Настроить обработку ошибок при выполнении трансформации
- Реализовать индивидуальное задание: фильтрацию по стране (United States), анализ скидок и отчёт по регионам

---

## 2. Архитектура решения

### 2.1 Схема Job

```
START
  └─[unconditional]─► Set Variables
                           └─[success]─► Check File Exists
                                              ├─[success]─► Prep Orders Table
                                              │                  └─[success]─► Load Orders (lab_02_1)
                                              │                                      └─[success]─► Fix Orders Types
                                              │                                                         └─[success]─► Load Customers (lab_02_2)
                                              │                                                                             └─[success]─► Load Products (lab_02_3)
                                              │                                                                                                 └─[success]─► Analytics 1 Discount (lab_02_4)
                                              │                                                                                                                     └─[success]─► Analytics 2 Regions (lab_02_5)
                                              │                                                                                                                                         └─[success]─► SUCCESS
                                              └─[failure]─► Error (Write to log)
```

**Описание шагов Job:**

| Шаг | Тип | Назначение |
|-----|-----|-----------|
| START | Special | Точка входа в Job |
| Set Variables | Set Variables | Устанавливает переменную `CSV_FILE_PATH` с путём к файлу |
| Check File Exists | File Exists | Проверяет наличие CSV-файла по пути из переменной |
| Error | Write to Log | Логирует ошибку если файл не найден |
| Prep Orders Table | Execute SQL | Меняет тип колонки `returned` с SMALLINT на TEXT перед загрузкой |
| Load Orders | Transformation | Запускает трансформацию `lab_02_1_csv_orders.ktr` |
| Fix Orders Types | Execute SQL | Возвращает тип колонки `returned` обратно в SMALLINT |
| Load Customers | Transformation | Запускает трансформацию `lab_02_2_csv_customers.ktr` |
| Load Products | Transformation | Запускает трансформацию `lab_02_3_csv_products.ktr` |
| Analytics 1 Discount | Transformation | Запускает `lab_02_4_discount_analysis.ktr` (доп. задание 1) |
| Analytics 2 Regions | Transformation | Запускает `lab_02_5_region_report.ktr` (доп. задание 2) |
| SUCCESS | Success | Завершение Job при успешном выполнении всех шагов |

![Скриншот 1 — Общий вид Job в Spoon](screenshots/01_job_overview.png)
*На скриншоте: canvas с job, все шаги соединены стрелками, зелёные галочки на каждом шаге после успешного запуска*

---

### 2.2 Схема трансформаций

**Трансформация 1 — lab_02_1_csv_orders.ktr (Заказы):**
```
CSV file input
    └─► Select values (переименование колонок в snake_case)
              └─► Memory group by (дедупликация по row_id)
                        └─► Filter rows (data check) ──[true]──► Value mapper (Yes/No → 1/0)
                                                                        └─► Convert returned type (String → Integer)
                                                                                  └─► Filter rows (country = United States)
                                                                                              └─► Table output → orders
                                    └─[false]──► Write to log (строки с пустыми датами)
```

**Трансформация 2 — lab_02_2_csv_customers.ktr (Клиенты):**
```
CSV file input
    └─► Select values (выбор 8 полей клиента)
              └─► Memory group by (дедупликация по customer_id)
                        └─► Table output → customers
```

**Трансформация 3 — lab_02_3_csv_products.ktr (Продукты):**
```
CSV file input
    └─► Select values (выбор 5 полей продукта)
              └─► Memory group by (дедупликация по product_id)
                        └─► Table output → products
```

**Трансформация 4 — lab_02_4_discount_analysis.ktr (Анализ скидок):**
```
Table input (SELECT discount, sales, profit FROM orders)
    └─► Memory group by (группировка по discount)
              └─► Table output → discount_analysis
```

**Трансформация 5 — lab_02_5_region_report.ktr (Отчёт по регионам):**
```
CSV file input
    └─► Select values (region, country, sales, profit, discount)
              └─► Filter rows (country = United States)
                        └─► Memory group by (группировка по region)
                                  └─► Table output → region_report
```

---

## 3. Описание датасета samplestore-general.csv

**Источник:** репозиторий GitHub — [BosenkoTM/workshop-on-ETL](https://github.com/BosenkoTM/workshop-on-ETL/blob/main/data_for_lessons/samplestore-general.csv)

**Расположение:** `~/Downloads/datain/samplestore-general.csv`

**Формат:** CSV, разделитель `;`, кодировка UTF-8, первая строка — заголовок

**Объём:** 9 995 строк (9 994 записи + 1 строка заголовка)

**Колонки датасета:**

| № | Название колонки | Тип | Описание |
|---|-----------------|-----|---------|
| 1 | Row ID | Integer | Уникальный идентификатор строки |
| 2 | Order ID | String | Идентификатор заказа |
| 3 | Order Date | Date (dd/MM/yyyy) | Дата заказа |
| 4 | Ship Date | Date (dd/MM/yyyy) | Дата доставки |
| 5 | Ship Mode | String | Способ доставки |
| 6 | Customer ID | String | Идентификатор клиента |
| 7 | Customer Name | String | Имя клиента |
| 8 | Segment | String | Сегмент клиента (Consumer/Corporate/Home Office) |
| 9 | Country | String | Страна |
| 10 | City | String | Город |
| 11 | State | String | Штат |
| 12 | Postal Code | String | Почтовый индекс |
| 13 | Region | String | Регион (West/East/Central/South) |
| 14 | Product ID | String | Идентификатор продукта |
| 15 | Category | String | Категория товара |
| 16 | Sub-Category | String | Подкатегория товара |
| 17 | Product Name | String | Название продукта |
| 18 | Sales | Decimal | Сумма продажи |
| 19 | Quantity | Integer | Количество единиц |
| 20 | Discount | Decimal | Скидка (0.0–0.8) |
| 21 | Profit | Decimal | Прибыль |
| 22 | Returned | String | Возврат (Yes/No) |
| 23 | Person | String | Менеджер по продажам |

---

## 4. Описание компонентов трансформаций

### 4.1 Трансформация 1: lab_02_1_csv_orders.ktr

![Скриншот 2 — Трансформация 1: общий вид](screenshots/02_transform1_overview.png)
*На скриншоте: canvas трансформации, все шаги и стрелки между ними*

#### CSV file input
Читает файл `samplestore-general.csv`. Разделитель — `;`, кодировка — UTF-8, первая строка — заголовок. Для полей `Order Date` и `Ship Date` задан формат `dd/MM/yyyy`, для числовых полей (Sales, Quantity, Discount, Profit) — числовые форматы.

![Скриншот 3 — Трансформация 1: настройки CSV Input](screenshots/03_csv_input.png)
*На скриншоте: диалог CSV Input — путь к файлу, разделитель, список полей с типами*

#### Select values
Переименовывает колонки из формата "Title Case" в snake_case для соответствия схеме БД:

| Исходное имя | Новое имя |
|-------------|----------|
| Row ID | row_id |
| Order Date | order_date |
| Ship Date | ship_date |
| Ship Mode | ship_mode |
| Sales | sales |
| Quantity | quantity |
| Discount | discount |
| Profit | profit |
| Returned | returned |
| Country | country |

Также через error handling подключён шаг `Dummy (do nothing)` — перехватывает строки с ошибками преобразования типов.

![Скриншот 4 — Трансформация 1: настройки Select Values](screenshots/04_select_values.png)
*На скриншоте: диалог Select Values, вкладка Select & Alter с маппингом полей*

#### Memory group by
Выполняет дедупликацию заказов по полю `row_id`. Для всех остальных полей применяется агрегация `FIRST` (берётся первое встреченное значение в группе). Это гарантирует, что каждый `row_id` встречается в таблице `orders` ровно один раз.

| Поле группировки | Агрегируемые поля (FIRST) |
|-----------------|--------------------------|
| row_id | order_date, ship_date, ship_mode, country, sales, quantity, discount, profit, returned |

![Скриншот 5 — Трансформация 1: настройки Memory Group By](screenshots/05_memory_group_by.png)
*На скриншоте: диалог Memory Group By — поле группировки row_id, список агрегатов*

#### Filter rows (data check)
Проверяет качество данных: пропускает строки только при условии, что обе даты не пустые.

- **Условие:** `order_date IS NOT NULL AND ship_date IS NOT NULL`
- **True →** Value mapper (строка корректна)
- **False →** Write to log (строка с пустой датой логируется и отбрасывается)

![Скриншот 6 — Трансформация 1: настройки Filter Rows (даты)](screenshots/06_filter_dates.png)
*На скриншоте: диалог Filter Rows с условием проверки дат*

#### Value mapper
Преобразует текстовые значения поля `returned` в числовые коды:

| Исходное значение | Результат |
|------------------|----------|
| Yes | 1 |
| No | 0 |
| (пустое) | 0 |

![Скриншот 7 — Трансформация 1: настройки Value Mapper](screenshots/07_value_mapper.png)
*На скриншоте: диалог Value Mapper — маппинг Yes→1, No→0*

#### Convert returned type (Modified JavaScript Value)
Конвертирует поле `returned` из типа String в Integer с помощью JavaScript:
```javascript
var returned = parseInt(returned + "", 10);
if (isNaN(returned)) returned = 0;
```
Выходной тип поля — Integer. Все остальные поля передаются без изменений.

#### Filter rows (country) — Вариант 10
Реализует основной фильтр варианта 10: пропускает только заказы из United States.

- **Условие:** `country = "United States"`
- **True →** Table output
- **False →** (строка отбрасывается)

![Скриншот 8 — Трансформация 1: фильтр United States](screenshots/08_filter_us.png)
*На скриншоте: диалог Filter Rows с условием country = "United States"*

#### Table output → orders
Записывает отфильтрованные заказы в таблицу `orders`. Настройки:
- **Truncate table:** Yes (очищает таблицу перед загрузкой)
- **Commit size:** 1000
- **Use batch update:** Yes
- **Specify database fields:** Yes

Маппинг полей: row_id, order_date, ship_date, ship_mode, sales, quantity, discount, profit, returned.

![Скриншот 9 — Трансформация 1: настройки Table Output](screenshots/09_table_output_orders.png)
*На скриншоте: диалог Table Output — подключение postgres_etl, таблица orders, маппинг полей*

![Скриншот 10 — Подключение к PostgreSQL: Connection tested successfully](screenshots/10_db_connection.png)
*На скриншоте: диалог Database Connection — тип PostgreSQL, host localhost:5432, сообщение "Connection to database [etl_db] is OK"*

---

### 4.2 Трансформация 2: lab_02_2_csv_customers.ktr

![Скриншот 11 — Трансформация 2: общий вид](screenshots/11_transform2_overview.png)
*На скриншоте: canvas трансформации — CSV file input → Select values → Memory group by → Table output*

#### CSV file input
Читает тот же файл `samplestore-general.csv`. Все поля считываются как String (типизация не нужна, так как в таблицу `customers` идут только текстовые поля).

#### Select values
Выбирает 8 полей, относящихся к клиенту:

| Исходное имя | Новое имя |
|-------------|----------|
| Customer ID | customer_id |
| Customer Name | customer_name |
| Segment | segment |
| Country | country |
| City | city |
| State | state |
| Postal Code | postal_code |
| Region | region |

#### Memory group by
Дедупликация по `customer_id` — каждый клиент записывается в таблицу один раз. Из 9 994 строк исходного файла получается 793 уникальных клиента.

#### Table output → customers
Записывает уникальных клиентов в таблицу `customers` (Truncate + batch insert).

---

### 4.3 Трансформация 3: lab_02_3_csv_products.ktr

![Скриншот 12 — Трансформация 3: общий вид](screenshots/12_transform3_overview.png)
*На скриншоте: canvas трансформации — CSV file input → Select values → Memory group by → Table output*

#### CSV file input
Читает `samplestore-general.csv`, все поля — String.

#### Select values
Выбирает 5 полей продукта:

| Исходное имя | Новое имя |
|-------------|----------|
| Product ID | product_id |
| Category | category |
| Sub-Category | sub_category |
| Product Name | product_name |
| Person | person |

#### Memory group by
Дедупликация по `product_id`. Из 9 994 строк получается 1 862 уникальных продукта.

#### Table output → products
Записывает уникальные продукты в таблицу `products`.

---

## 5. SQL скрипты создания таблиц

```sql
-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    row_id      INTEGER PRIMARY KEY,
    order_date  DATE,
    ship_date   DATE,
    ship_mode   VARCHAR(50),
    sales       DECIMAL(10,2),
    quantity    INTEGER,
    discount    DECIMAL(4,2),
    profit      DECIMAL(10,2),
    returned    SMALLINT DEFAULT 0
);

-- Индексы для оптимизации запросов по датам
CREATE INDEX IF NOT EXISTS idx_order_date ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_ship_date  ON orders (ship_date);

-- Таблица клиентов
CREATE TABLE IF NOT EXISTS customers (
    id            SERIAL PRIMARY KEY,
    customer_id   VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    segment       VARCHAR(50),
    country       VARCHAR(100),
    city          VARCHAR(100),
    state         VARCHAR(100),
    postal_code   VARCHAR(20),
    region        VARCHAR(50),
    CONSTRAINT unique_customer UNIQUE (customer_id)
);

CREATE INDEX IF NOT EXISTS idx_region ON customers (region);

-- Таблица продуктов
CREATE TABLE IF NOT EXISTS products (
    id           SERIAL PRIMARY KEY,
    product_id   VARCHAR(20) NOT NULL,
    category     VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255),
    person       VARCHAR(100),
    CONSTRAINT unique_product UNIQUE (product_id)
);

CREATE INDEX IF NOT EXISTS idx_category ON products (category);

-- Таблица анализа скидок (Доп.1)
CREATE TABLE IF NOT EXISTS discount_analysis (
    discount    DECIMAL(4,2),
    order_count INTEGER,
    total_sales DECIMAL(10,2),
    avg_sales   DECIMAL(10,2),
    total_profit DECIMAL(10,2),
    avg_profit  DECIMAL(10,2)
);

-- Таблица отчёта по регионам (Доп.2)
CREATE TABLE IF NOT EXISTS region_report (
    region       VARCHAR(50),
    order_count  INTEGER,
    total_sales  DECIMAL(10,2),
    avg_sales    DECIMAL(10,2),
    total_profit DECIMAL(10,2),
    avg_discount DECIMAL(4,2)
);
```

---

## 6. Проверочные SQL запросы

### 6.1 Количество строк в таблицах

```sql
SELECT 'orders'            AS table_name, COUNT(*) AS row_count FROM orders
UNION ALL
SELECT 'customers',                        COUNT(*)              FROM customers
UNION ALL
SELECT 'products',                         COUNT(*)              FROM products
UNION ALL
SELECT 'discount_analysis',                COUNT(*)              FROM discount_analysis
UNION ALL
SELECT 'region_report',                    COUNT(*)              FROM region_report;
```

**Результат:**

| table_name | row_count |
|-----------|----------|
| orders | 9 994 |
| customers | 793 |
| products | 1 862 |
| discount_analysis | 12 |
| region_report | 4 |

![Скриншот 14 — SQL: COUNT по таблицам](screenshots/14_sql_count.png)
*На скриншоте: результат выполнения запроса в psql или pgAdmin — 5 строк с количеством записей*

---

### 6.2 Доп. задание 1 — Анализ скидок

```sql
SELECT
    discount,
    order_count,
    ROUND(total_sales, 2)   AS total_sales,
    ROUND(avg_sales, 2)     AS avg_sales,
    ROUND(total_profit, 2)  AS total_profit,
    ROUND(avg_profit, 2)    AS avg_profit
FROM discount_analysis
ORDER BY discount;
```

**Результат:**

| discount | order_count | total_sales | avg_sales | total_profit | avg_profit |
|---------|------------|------------|----------|-------------|-----------|
| 0.00 | 4798 | 1 087 908.47 | 226.74 | 321 248.27 | 66.90 |
| 0.10 | 94 | 54 369.30 | 578.40 | 9 029.36 | 96.06 |
| 0.15 | 52 | 27 558.59 | 529.97 | 1 418.88 | 27.29 |
| 0.20 | 3657 | 764 594.28 | 209.08 | 90 318.39 | 24.70 |
| 0.30 | 227 | 103 226.76 | 454.74 | −10 368.50 | −45.68 |
| 0.32 | 27 | 14 493.45 | 536.79 | −2 391.09 | −88.56 |
| 0.40 | 206 | 116 417.83 | 565.13 | −23 058.41 | −111.93 |
| 0.45 | 11 | 5 484.98 | 498.63 | −2 493.14 | −226.65 |
| 0.50 | 66 | 58 918.65 | 892.71 | −20 506.06 | −310.70 |
| 0.60 | 138 | 6 644.68 | 48.15 | −5 946.93 | −43.08 |
| 0.70 | 418 | 40 620.40 | 97.18 | −40 075.27 | −95.87 |
| 0.80 | 300 | 16 963.68 | 56.55 | −30 539.36 | −101.80 |

**Вывод:** скидки от 30% и выше приводят к отрицательной средней прибыли. Наибольшее количество заказов (4798) — без скидки.

![Скриншот 15 — SQL: Анализ скидок](screenshots/15_sql_discounts.png)
*На скриншоте: результат запроса к таблице discount_analysis — 12 строк с группировкой по уровню скидки*

---

### 6.3 Доп. задание 2 — Отчёт по регионам

```sql
SELECT
    region,
    order_count,
    ROUND(total_sales, 2)   AS total_sales,
    ROUND(avg_sales, 2)     AS avg_sales,
    ROUND(total_profit, 2)  AS total_profit,
    ROUND(avg_discount, 3)  AS avg_discount
FROM region_report
ORDER BY total_sales DESC;
```

**Результат:**

| region | order_count | total_sales | avg_sales | total_profit | avg_discount |
|-------|------------|------------|----------|-------------|-------------|
| West | 3203 | 725 457.82 | 226.51 | 108 418.45 | 0.143 |
| East | 2848 | 678 781.24 | 238.35 | 91 522.78 | 0.152 |
| Central | 2323 | 501 239.89 | 215.77 | 39 706.36 | 0.165 |
| South | 1620 | 391 721.91 | 241.80 | 46 749.43 | 0.132 |

**Вывод:** регион West лидирует по объёму продаж ($725K) и прибыли ($108K). Регион Central имеет самую высокую среднюю скидку (16.5%) при наименьшей прибыли среди топ-3.

![Скриншот 16 — SQL: Отчёт по регионам](screenshots/16_sql_regions.png)
*На скриншоте: результат запроса к таблице region_report — 4 строки с агрегатами по регионам*

---

## 7. Запуск Job и результаты выполнения

![Скриншот 13 — Запуск Job: зелёные галочки](screenshots/13_job_run_success.png)
*На скриншоте: лог выполнения Job в Spoon — все шаги завершены с result=[true], сообщение "Job execution finished"*

**Лог успешного выполнения Job:**
```
Starting job...
lab_02_CSV_to_PostgreSQL - Start of job execution
Starting entry [Set Variables]           → result=[true]
Starting entry [Check File Exists]       → result=[true]
Starting entry [Prep Orders Table]       → result=[true]
Starting entry [Load Orders]             → result=[true]  (Written: 9994)
Starting entry [Fix Orders Types]        → result=[true]
Starting entry [Load Customers]          → result=[true]  (Written: 793)
Starting entry [Load Products]           → result=[true]  (Written: 1862)
Starting entry [Analytics 1 Discount]   → result=[true]  (Written: 12)
Starting entry [Analytics 2 Regions]    → result=[true]  (Written: 4)
Job execution finished
```

---

## 8. Выводы

### 8.1 Что изучили

В ходе выполнения лабораторной работы были получены практические навыки:

- **Динамические соединения:** настройка подключения к PostgreSQL через Pentaho, использование переменных (`CSV_FILE_PATH`) для параметризации путей к файлам
- **Job-оркестрация:** создание Job с разветвлённой логикой — проверка файла, последовательный запуск трансформаций, обработка ошибок через ветви success/failure
- **Обработка дублей:** применение шага `Memory group by` для дедупликации по первичному ключу (row_id, customer_id, product_id)
- **Обработка ошибок:** шаги `Filter rows` (некорректные даты → Write to log) и `Dummy (do nothing)` (перехват ошибок преобразования типов)
- **Трансформация данных:** переименование полей (snake_case), маппинг значений (Value mapper), фильтрация по стране (FilterRows)

### 8.2 Результаты загрузки

| Таблица | Загружено строк | Описание |
|---------|----------------|---------|
| `orders` | **9 994** | Заказы — только United States (вариант 10) |
| `customers` | **793** | Уникальные клиенты после дедупликации |
| `products` | **1 862** | Уникальные продукты после дедупликации |
| `discount_analysis` | **12** | Агрегаты по 12 уровням скидок |
| `region_report` | **4** | Агрегаты по 4 регионам США |

### 8.3 Аналитические выводы

**Анализ скидок (Доп.1):**
- Компания предоставляет скидки от 0% до 80%
- При скидках ≥ 30% средняя прибыль становится отрицательной
- Критическая скидка: 50% даёт среднюю прибыль −$310 на заказ
- Наиболее прибыльные заказы — без скидки (avg_profit = $66.90) и со скидкой 10% ($96.06)

**Отчёт по регионам (Доп.2):**
- Лидер по продажам и прибыли: **West** ($725K продаж, $108K прибыли)
- Наименьший объём: **South** ($391K), но лучшее соотношение прибыли к продажам среди регионов с небольшим объёмом
- Регион **Central** имеет самую высокую среднюю скидку (16.5%) при низкой прибыли ($39K) — возможная зона оптимизации

### 8.4 Отличия от лабораторной работы 1

| Аспект | Лаб. работа 1 | Лаб. работа 2 |
|--------|--------------|--------------|
| Инструмент | Установка и настройка PDI | ETL-процесс с Job |
| Подключение | Одиночное подключение | Динамические соединения через переменные |
| Оркестрация | Одиночная трансформация | Job с последовательностью трансформаций |
| Обработка ошибок | Базовая | Разветвлённая (FilterRows + Write to log + Dummy) |
| Дедупликация | Не рассматривалась | Memory group by по первичным ключам |
| Аналитика | Загрузка данных | Загрузка + аналитические трансформации |
| Таблиц в БД | 1 | 5 (3 основных + 2 аналитических) |
