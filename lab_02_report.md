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

*На скриншоте: canvas с job, все шаги соединены стрелками, зелёные галочки на каждом шаге после успешного запуска*
<img width="1464" height="808" alt="image-83" src="https://github.com/user-attachments/assets/c100fa2e-cc73-48af-8997-7e1be6ea276d" />

<img width="1242" height="801" alt="image-84" src="https://github.com/user-attachments/assets/9b687a49-90f8-4767-8e97-943465682bb2" />


---

### 2.2 Схема трансформаций

**Трансформация 1 — lab_02_1_csv_orders.ktr (Заказы):**

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
<img width="1199" height="409" alt="image-85" src="https://github.com/user-attachments/assets/10c449bc-14fe-4534-b36d-a3901d095eb0" />

<img width="948" height="353" alt="image-86" src="https://github.com/user-attachments/assets/0ce43f1d-1800-4b40-9831-ba545b13de7e" />

<img width="689" height="458" alt="image-87" src="https://github.com/user-attachments/assets/e152c954-1663-45a0-a806-1f4ea9175ace" />

*На скриншоте: диалог Select Values, вкладка Select & Alter с маппингом полей*

<img width="431" height="180" alt="image-88" src="https://github.com/user-attachments/assets/761b2dbc-ea49-4c93-a87d-4cd9ffc91067" />


#### Filter rows (data check)
Проверяет качество данных: пропускает строки только при условии, что обе даты не пустые.

- **Условие:** `order_date IS NOT NULL AND ship_date IS NOT NULL`
- **True →** Value mapper (строка корректна)
- **False →** Write to log (строка с пустой датой логируется и отбрасывается)

*На скриншоте: диалог Filter Rows с условием проверки дат*

<img width="738" height="375" alt="image-89" src="https://github.com/user-attachments/assets/78a0d64e-4082-4cb1-a5d9-87965fb5a7c1" />


#### Convert returned type (Modified JavaScript Value)
Конвертирует поле `returned` из типа String в Integer с помощью JavaScript:
```javascript
var returned = parseInt(returned + "", 10);
if (isNaN(returned)) returned = 0;
```
Выходной тип поля — Integer. Все остальные поля передаются без изменений.

<img width="913" height="670" alt="image-90" src="https://github.com/user-attachments/assets/49f87dae-b494-4a91-b389-b90767a41601" />

---

### 4.2 Трансформация 2: lab_02_2_csv_customers.ktr

<img width="893" height="349" alt="image-91" src="https://github.com/user-attachments/assets/ca2b4449-c0bc-436f-9822-e41417f807d8" />

<img width="890" height="288" alt="image-92" src="https://github.com/user-attachments/assets/e03ac10f-a44d-4752-adc0-f8a7de815724" />


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

<img width="821" height="352" alt="image-93" src="https://github.com/user-attachments/assets/502d09e9-22aa-4281-8b31-1863dc430151" />

<img width="878" height="286" alt="image-94" src="https://github.com/user-attachments/assets/b21be8ea-f0f4-4d15-9e6f-41c91c9c5b4b" />

<img width="1253" height="757" alt="image-95" src="https://github.com/user-attachments/assets/26cdc8d5-c9f4-41c6-9ab0-855793f7c981" />

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

<img width="574" height="314" alt="image-96" src="https://github.com/user-attachments/assets/c920da2f-33eb-438b-8862-98c4f22f1eec" />


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


*На скриншоте: результат запроса к таблице discount_analysis — 12 строк с группировкой по уровню скидки*
<img width="569" height="344" alt="image-97" src="https://github.com/user-attachments/assets/50eb37b4-3c36-4ad1-b2db-87c1219f6236" />

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

*На скриншоте: результат запроса к таблице region_report — 4 строки с агрегатами по регионам*
<img width="576" height="219" alt="image-98" src="https://github.com/user-attachments/assets/033a8616-61ff-49d3-9c53-361ca9a99f02" />

---

## 7. Запуск Job и результаты выполнения

*На скриншоте: лог выполнения Job в Spoon — все шаги завершены с result=[true], сообщение "Job execution finished"*
<img width="1057" height="826" alt="image-99" src="https://github.com/user-attachments/assets/0152a9b5-a9f0-4cd7-bf7c-2a16b5db7129" />


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

