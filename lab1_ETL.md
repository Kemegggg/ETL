# Лабораторная работа №1 — Установка и настройка ETL-инструмента. Создание конвейеров данных

**Предмет:** ETL  
**Вариант №10:** Анализ банковских транзакций (Banking Transactions Dataset)

---

## Цель работы

Изучение основных принципов работы с ETL-инструментами на примере Pentaho Data Integration (PDI), настройка конвейера обработки данных, фильтрация и замена значений в Excel-файле, а также выгрузка обработанных данных в базу данных PostgreSQL.

### Решённые задачи

- Развёртывание PostgreSQL в контейнере Docker и создание целевой таблицы
- Создание ETL-трансформации в Pentaho Spoon (файл `lab_01.ktr`)
- Загрузка исходных данных из файла `bank.xlsx` (116 200 строк)
- Фильтрация строк с отсутствующим описанием транзакции
- Классификация типов транзакций по ключевым словам (замена значений)
- Выгрузка обработанных данных в таблицу `bank_transactions` (PostgreSQL)
- Верификация результата с помощью SQL-запросов

---

## Инструменты и технологии

| Компонент | Описание |
|---|---|
| Pentaho Data Integration 9.4 | ETL-инструмент (Spoon) |
| PostgreSQL 15 | Целевая база данных |
| Docker Desktop | Контейнерная среда |
| Apache POI | Движок чтения Excel .xlsx |
| bank.xlsx | Исходный датасет (Kaggle) |

---

## Описание датасета

**Источник:** [Banking Transactions Dataset](https://www.kaggle.com/datasets/apoorvwatsky/bank-transaction-data) (Kaggle)

Датасет содержит записи транзакций по банковским счетам индийского банка за период 2017–2018 годов.

| Параметр | Значение |
|---|---|
| Формат файла | Microsoft Excel 2007+ (.xlsx) |
| Лист с данными | Sheet1 |
| Всего строк (с заголовком) | 116 201 |
| Строк данных | 116 200 |
| Количество колонок | 9 |
| Период транзакций | 2017–2018 гг. |
| Размер файла | ≈ 6,5 МБ |

### Описание колонок

| № | Колонка (оригинал) | Переименование | Тип | Описание |
|---|---|---|---|---|
| 1 | Account No | account_no | VARCHAR(50) | Номер банковского счёта |
| 2 | DATE | transaction_date | DATE | Дата проведения транзакции |
| 3 | TRANSACTION DETAILS | transaction_details | VARCHAR(500) | Текстовое описание транзакции |
| 4 | CHQ.NO. | chq_no | VARCHAR(50) | Номер чека (часто NULL) |
| 5 | VALUE DATE | value_date | DATE | Дата валютирования |
| 6 | WITHDRAWAL AMT | withdrawal_amt | NUMERIC(15,2) | Сумма списания |
| 7 | DEPOSIT AMT | deposit_amt | NUMERIC(15,2) | Сумма зачисления |
| 8 | BALANCE AMT | (исключена) | — | Содержит Excel-формулы, исключена |
| 9 | . | (исключена) | — | Служебная колонка, исключена |

---

## ETL-конвейер

Трансформация реализована в файле `lab_01.ktr` и состоит из пяти последовательных шагов:

```
Read Excel bank.xlsx → Rename Columns → Filter Empty Transactions → Classify Transaction Type → Load to PostgreSQL
```

### Шаг 1 — Read Excel bank.xlsx

Тип компонента: `Microsoft Excel Input (ExcelInput)`

Чтение исходного файла `bank.xlsx`. Важно: используется движок **Apache POI** (Excel 2007 XLSX), а не JXL — последний не поддерживает формат `.xlsx` и вызывает ошибку `jxl.read.biff.CompoundFile`.

| Параметр | Значение |
|---|---|
| Файл | bank.xlsx |
| Лист | Sheet1 |
| Движок чтения | Apache POI |
| Прочитано строк | 116 200 |

### Шаг 2 — Rename Columns

Тип компонента: `Select Values (SelectValues)`

Переименование колонок из исходных названий в snake_case, совместимые с PostgreSQL. Колонки `BALANCE AMT` и `.` исключаются из потока.

| Исходное название | Новое название |
|---|---|
| Account No | account_no |
| DATE | transaction_date |
| TRANSACTION DETAILS | transaction_details |
| CHQ.NO. | chq_no |
| VALUE DATE | value_date |
| WITHDRAWAL AMT | withdrawal_amt |
| DEPOSIT AMT | deposit_amt |

### Шаг 3 — Filter Empty Transactions

Тип компонента: `Filter Rows (FilterRows)`

Удаление строк без описания транзакции.

| Параметр | Значение |
|---|---|
| Условие | `transaction_details IS NOT NULL` |
| Строк до фильтра | 116 200 |
| Строк после фильтра | 113 702 |
| Удалено строк | 2 498 (2,1%) |

### Шаг 4 — Classify Transaction Type

Тип компонента: `Modified JavaScript Value (ScriptValueMod)`

Добавление нового поля `transaction_type` на основе ключевых слов в `transaction_details`.

```javascript
var details = (transaction_details != null)
    ? String(transaction_details).toUpperCase() : '';
var transaction_type;

if      (details.indexOf('TRF') >= 0
         || details.indexOf('TRANSFER') >= 0) transaction_type = 'Transfer';
else if (details.indexOf('NEFT') >= 0)        transaction_type = 'NEFT Transfer';
else if (details.indexOf('RTGS') >= 0)        transaction_type = 'RTGS Transfer';
else if (details.indexOf('IMPS') >= 0)        transaction_type = 'IMPS Transfer';
else if (details.indexOf('ATM')  >= 0)        transaction_type = 'ATM Withdrawal';
else if (details.indexOf('POS')  >= 0)        transaction_type = 'POS Payment';
else if (details.indexOf('FDRL') >= 0)        transaction_type = 'Internal Transfer';
else if (details.indexOf('ECS')  >= 0
         || details.indexOf('ACH') >= 0)      transaction_type = 'ECS/ACH Payment';
else if (details.indexOf('CHQ')  >= 0
         || details.indexOf('CHEQUE') >= 0)   transaction_type = 'Cheque';
else if (details.indexOf('INT.COLL') >= 0)    transaction_type = 'Interest Collection';
else if (details.indexOf('INTEREST') >= 0)    transaction_type = 'Interest';
else if (details.indexOf('SALARY') >= 0)      transaction_type = 'Salary';
else                                           transaction_type = 'Other';
```

Результаты классификации:

| Тип транзакции | Количество | % от общего |
|---|---|---|
| Other | 54 282 | 47,7% |
| Internal Transfer | 16 871 | 14,8% |
| Transfer | 12 110 | 10,7% |
| NEFT Transfer | 9 155 | 8,1% |
| IMPS Transfer | 8 959 | 7,9% |
| RTGS Transfer | 6 562 | 5,8% |
| POS Payment | 2 387 | 2,1% |
| Cheque | 2 086 | 1,8% |
| ATM Withdrawal | 1 061 | 0,9% |
| Interest Collection | 113 | 0,1% |
| ECS/ACH Payment | 70 | 0,1% |
| Salary | 41 | 0,0% |
| Interest | 5 | 0,0% |
| **ИТОГО** | **113 702** | **100%** |

### Шаг 5 — Load to PostgreSQL

Тип компонента: `Table Output (TableOutput)`

Выгрузка обработанных данных в PostgreSQL через JDBC.

| Параметр | Значение |
|---|---|
| СУБД | PostgreSQL 15 |
| Хост | localhost |
| Порт | 5432 |
| База данных | etl_db |
| Пользователь | etl_user |
| Таблица | bank_transactions |
| Режим загрузки | TRUNCATE перед вставкой |
| Записано строк | 113 702 |

DDL-скрипт создания таблицы:

```sql
CREATE TABLE IF NOT EXISTS bank_transactions (
    id                  SERIAL PRIMARY KEY,
    account_no          VARCHAR(50),
    transaction_date    DATE,
    transaction_details VARCHAR(500),
    chq_no              VARCHAR(50),
    value_date          DATE,
    withdrawal_amt      NUMERIC(15,2),
    deposit_amt         NUMERIC(15,2),
    transaction_type    VARCHAR(50)
);
```

---

## SQL-запросы для проверки

### 1. Количество загруженных строк

```sql
SELECT COUNT(*) AS total_rows
FROM bank_transactions;
```

| total_rows |
|---|
| 113702 |

### 2. Первые 10 записей

```sql
SELECT *
FROM bank_transactions
LIMIT 10;
```

| id | account_no | transaction_date | transaction_details | transaction_type |
|---|---|---|---|---|
| 1 | 409000611074' | 2017-06-29 | TRF FROM Indiaforensic SERVICES | Transfer |
| 2 | 409000611074' | 2017-07-05 | TRF FROM Indiaforensic SERVICES | Transfer |
| 3 | 409000611074' | 2017-07-18 | FDRL/INTERNAL FUND TRANSFE | Internal Transfer |
| ... | ... | ... | ... | ... |

### 3. Распределение по типам транзакций

```sql
SELECT transaction_type,
       COUNT(*) AS cnt
FROM bank_transactions
GROUP BY transaction_type
ORDER BY COUNT(*) DESC;
```

| transaction_type | cnt |
|---|---|
| Other | 54282 |
| Internal Transfer | 16871 |
| Transfer | 12110 |
| NEFT Transfer | 9155 |
| IMPS Transfer | 8959 |
| RTGS Transfer | 6562 |
| POS Payment | 2387 |
| Cheque | 2086 |
| ATM Withdrawal | 1061 |
| Interest Collection | 113 |
| ECS/ACH Payment | 70 |
| Salary | 41 |
| Interest | 5 |

Сумма всех значений = **113 702** — соответствует результату первого запроса, потерь данных нет.

---

## Выводы

- Изучен ETL-инструмент Pentaho Data Integration 9.4, освоен интерфейс Spoon, создана трансформация из пяти шагов.
- Настроено чтение данных из Excel-файла через движок Apache POI. Выявлена и исправлена несовместимость движка JXL с форматом `.xlsx`.
- Выполнена фильтрация: удалено 2 498 строк (2,1%) с пустым описанием транзакции.
- Реализована классификация типов транзакций: добавлено поле `transaction_type` с 13 категориями.
- Данные успешно выгружены в PostgreSQL: загружено 113 702 записи, корректность подтверждена SQL-запросами.
- Получены практические навыки работы с Docker и PostgreSQL как изолированной средой хранения данных.
