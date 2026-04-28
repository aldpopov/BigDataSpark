# Лабораторная работа №2: ETL, реализованный с помощью Spark

## Выполнил студент группы М8О-308Б-23 Попов Александр

## Структура проекта

```
└── LW_2/
    ├── docker-compose.yml
    ├── spark/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── jobs/
    │       ├── 01_etl_to_star_schema.py
    │       └── 02_reports_to_clickhouse.py
    ├── init_postgres/
    │   └── init.sql
    └── data/
        ├── MOCK_DATA (1).csv
        ├── MOCK_DATA (2).csv
        ├── ...
        ├── MOCK_DATA (9).csv
        └── MOCK_DATA.csv
```

## Модель данных "Звезда" (Star Schema)

### Таблица фактов (Fact Table)
- **fact_sales** - факты продаж
  - `sale_id` - идентификатор продажи
  - `customer_id` - внешний ключ к dim_customer
  - `product_id` - внешний ключ к dim_product
  - `seller_id` - внешний ключ к dim_seller
  - `store_id` - внешний ключ к dim_store
  - `supplier_id` - внешний ключ к dim_supplier
  - `time_id` - внешний ключ к dim_time
  - `quantity` - количество товара
  - `total_price` - общая стоимость
  - `sale_date` - дата продажи

### Таблицы измерений (Dimension Tables)
1. **dim_customer** - информация о покупателях
2. **dim_product** - информация о товарах
3. **dim_seller** - информация о продавцах
4. **dim_store** - информация о магазинах
5. **dim_supplier** - информация о поставщиках
6. **dim_time** - временное измерение

## Создаваемые отчеты в ClickHouse

### Витрина продаж по продуктам
| Таблица | Описание |
|---------|----------|
| `top_products` | Топ-10 самых продаваемых продуктов |
| `revenue_by_category` | Общая выручка по категориям продуктов |
| `product_ratings` | Рейтинг, количество отзывов и категория для каждого продукта |

### Витрина продаж по клиентам
| Таблица | Описание |
|---------|----------|
| `top_customers` | Топ-10 клиентов с наибольшей суммой покупок |
| `customers_by_country` | Распределение клиентов по странам |
| `customer_avg_order` | Средний чек, количество заказов и общая сумма для каждого клиента |

### Витрина продаж по времени
| Таблица | Описание |
|---------|----------|
| `monthly_trends` | Месячные тренды продаж |
| `yearly_comparison` | Сравнение выручки по годам |
| `monthly_avg_order_size` | Средний размер заказа по месяцам |

### Витрина продаж по магазинам
| Таблица | Описание |
|---------|----------|
| `top_stores` | Топ-5 магазинов с наибольшей выручкой |
| `sales_by_location` | Распределение продаж по городам и странам |
| `store_avg_check` | Средний чек для каждого магазина |

### Витрина продаж по поставщикам
| Таблица | Описание |
|---------|----------|
| `top_suppliers` | Топ-5 поставщиков с наибольшей выручкой |
| `supplier_avg_price` | Средняя цена товаров от каждого поставщика |
| `sales_by_supplier_country` | Распределение продаж по странам поставщиков |

### Витрина качества продукции
| Таблица | Описание |
|---------|----------|
| `products_highest_rating` | Продукты с наивысшим рейтингом |
| `products_lowest_rating` | Продукты с наименьшим рейтингом |
| `rating_sales_correlation` | Корреляция между рейтингом и объемом продаж |
| `products_most_reviews` | Продукты с наибольшим количеством отзывов |

## Требования к системе

- Docker и Docker Compose

## Инструкция по запуску

### 1. Клонирование репозитория

```bash
git clone https://github.com/aldpopov/BigDataSpark.git
```

### 2. Сборка и запуск контейнеров

```bash
docker-compose up -d --build
```

Будут запущены следующие сервисы:
- **PostgreSQL**
- **Spark**
- **ClickHouse**

### 3. Запуск ETL (создание модели "Звезда" в PostgreSQL)

```bash
docker exec -it spark_app spark-submit jobs/01_etl_to_star_schema.py
```

### 4. Запуск создания отчетов в ClickHouse

```bash
docker exec -it spark_app spark-submit jobs/02_reports_to_clickhouse.py
```

## Выводы

В ходе лабораторной работы были успешно реализованы модель "Звезда" в PostgreSQL, ETL-пайплайн на Spark и система аналитических отчетов в ClickHouse.
