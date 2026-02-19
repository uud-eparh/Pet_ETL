-- init_db/03_create_dm_tables.sql
-- Витрина данных для аналитики курсов валют

-- ============================================
-- 1. ТАБЛИЦЫ ФАКТОВ И ИЗМЕРЕНИЙ
-- ============================================

-- Таблица измерений "Валюты"
CREATE TABLE IF NOT EXISTS cbr_dm.dim_currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL UNIQUE,
    currency_name VARCHAR(100) NOT NULL,
    first_seen DATE DEFAULT CURRENT_DATE,
    last_seen DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Заполняем справочник валют из сырых данных
INSERT INTO cbr_dm.dim_currency (currency_code, currency_name)
SELECT DISTINCT currency_code, currency_name
FROM cbr_raw.exchange_rates
ON CONFLICT (currency_code) DO NOTHING;

-- Таблица фактов "Курсы валют" (денормализованная для простоты)
CREATE TABLE IF NOT EXISTS cbr_dm.fact_exchange_rates (
    fact_id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL,
    exchange_rate DECIMAL(12,6) NOT NULL,
    rate_date DATE NOT NULL,
    rate_type VARCHAR(10) NOT NULL,
    nominal INTEGER DEFAULT 1,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Уникальный ключ для UPSERT
    UNIQUE(currency_code, rate_date, rate_type)
);

-- Индексы для фактов
CREATE INDEX IF NOT EXISTS idx_fact_currency_date ON cbr_dm.fact_exchange_rates(currency_code, rate_date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_date ON cbr_dm.fact_exchange_rates(rate_date);
CREATE INDEX IF NOT EXISTS idx_fact_type ON cbr_dm.fact_exchange_rates(rate_type);

-- Копируем данные из сырой схемы в факты
INSERT INTO cbr_dm.fact_exchange_rates (currency_code, exchange_rate, rate_date, rate_type, nominal, load_timestamp)
SELECT currency_code, exchange_rate, rate_date, rate_type, nominal, load_timestamp
FROM cbr_raw.exchange_rates
ON CONFLICT (currency_code, rate_date, rate_type) DO NOTHING;

-- ============================================
-- 2. АГРЕГАТЫ И АНАЛИТИЧЕСКИЕ ПРЕДСТАВЛЕНИЯ
-- ============================================

-- 1. ДНЕВНАЯ ДИНАМИКА (изменение за день)
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_daily_changes;
CREATE MATERIALIZED VIEW cbr_dm.mv_daily_changes AS
SELECT 
    currency_code,
    rate_date,
    exchange_rate,
    LAG(exchange_rate) OVER (PARTITION BY currency_code ORDER BY rate_date) as prev_rate,
    exchange_rate - LAG(exchange_rate) OVER (PARTITION BY currency_code ORDER BY rate_date) as abs_change,
    ROUND((exchange_rate / LAG(exchange_rate) OVER (PARTITION BY currency_code ORDER BY rate_date) - 1) * 100, 4) as pct_change
FROM cbr_raw.exchange_rates
WHERE rate_type = 'daily'
ORDER BY currency_code, rate_date;

-- 2. НЕДЕЛЬНАЯ ДИНАМИКА
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_weekly_stats;
CREATE MATERIALIZED VIEW cbr_dm.mv_weekly_stats AS
WITH weekly_base AS (
    SELECT 
        currency_code,
        DATE_TRUNC('week', rate_date)::DATE as week_start,
        AVG(exchange_rate) as avg_rate,
        MIN(exchange_rate) as min_rate,
        MAX(exchange_rate) as max_rate,
        STDDEV(exchange_rate) as volatility,
        COUNT(*) as days_count
    FROM cbr_raw.exchange_rates
    WHERE rate_type = 'daily'
    GROUP BY currency_code, DATE_TRUNC('week', rate_date)
)
SELECT 
    *,
    avg_rate - LAG(avg_rate) OVER (PARTITION BY currency_code ORDER BY week_start) as abs_change,
    ROUND((avg_rate / LAG(avg_rate) OVER (PARTITION BY currency_code ORDER BY week_start) - 1) * 100, 4) as pct_change
FROM weekly_base
ORDER BY currency_code, week_start;

-- 3. МЕСЯЧНАЯ ДИНАМИКА
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_monthly_stats;
CREATE MATERIALIZED VIEW cbr_dm.mv_monthly_stats AS
WITH monthly_base AS (
    SELECT 
        currency_code,
        DATE_TRUNC('month', rate_date)::DATE as month_start,
        AVG(exchange_rate) as avg_rate,
        MIN(exchange_rate) as min_rate,
        MAX(exchange_rate) as max_rate,
        STDDEV(exchange_rate) as volatility,
        COUNT(*) as days_count
    FROM cbr_raw.exchange_rates
    WHERE rate_type = 'daily'
    GROUP BY currency_code, DATE_TRUNC('month', rate_date)
)
SELECT 
    *,
    avg_rate - LAG(avg_rate) OVER (PARTITION BY currency_code ORDER BY month_start) as abs_change,
    ROUND((avg_rate / LAG(avg_rate) OVER (PARTITION BY currency_code ORDER BY month_start) - 1) * 100, 4) as pct_change
FROM monthly_base
ORDER BY currency_code, month_start;

-- 4. КВАРТАЛЬНАЯ ДИНАМИКА
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_quarterly_stats;
CREATE MATERIALIZED VIEW cbr_dm.mv_quarterly_stats AS
WITH quarterly_base AS (
    SELECT 
        currency_code,
        DATE_TRUNC('quarter', rate_date)::DATE as quarter_start,
        AVG(exchange_rate) as avg_rate,
        MIN(exchange_rate) as min_rate,
        MAX(exchange_rate) as max_rate,
        STDDEV(exchange_rate) as volatility,
        COUNT(*) as days_count
    FROM cbr_raw.exchange_rates
    WHERE rate_type = 'daily'
    GROUP BY currency_code, DATE_TRUNC('quarter', rate_date)
)
SELECT 
    *,
    avg_rate - LAG(avg_rate) OVER (PARTITION BY currency_code ORDER BY quarter_start) as abs_change,
    ROUND((avg_rate / LAG(avg_rate) OVER (PARTITION BY currency_code ORDER BY quarter_start) - 1) * 100, 4) as pct_change
FROM quarterly_base
ORDER BY currency_code, quarter_start;

-- 5. ТОП-10 ВОЛАТИЛЬНЫХ ВАЛЮТ (за последние 30 дней)
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_top_volatile;
CREATE MATERIALIZED VIEW cbr_dm.mv_top_volatile AS
SELECT 
    currency_code,
    ROUND(STDDEV(exchange_rate)::numeric, 4) as volatility,
    ROUND(AVG(exchange_rate)::numeric, 2) as avg_rate,
    ROUND(MIN(exchange_rate)::numeric, 2) as min_rate,
    ROUND(MAX(exchange_rate)::numeric, 2) as max_rate,
    ROUND(MAX(exchange_rate)::numeric - MIN(exchange_rate)::numeric, 2) as range,
    ROUND((MAX(exchange_rate) - MIN(exchange_rate)) / AVG(exchange_rate) * 100, 2) as pct_range
FROM cbr_raw.exchange_rates
WHERE rate_type = 'daily'
AND rate_date > CURRENT_DATE - INTERVAL '30 days'
GROUP BY currency_code
ORDER BY volatility DESC
LIMIT 10;

-- 6. КОРРЕЛЯЦИЯ С USD
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_correlation_with_usd;
CREATE MATERIALIZED VIEW cbr_dm.mv_correlation_with_usd AS
WITH usd_rates AS (
    SELECT rate_date, exchange_rate as usd_rate
    FROM cbr_raw.exchange_rates
    WHERE currency_code = 'USD' AND rate_type = 'daily'
),
other_rates AS (
    SELECT currency_code, rate_date, exchange_rate
    FROM cbr_raw.exchange_rates
    WHERE currency_code != 'USD' AND rate_type = 'daily'
)
SELECT 
    o.currency_code,
    ROUND(CORR(o.exchange_rate, u.usd_rate)::numeric, 4) as correlation_with_usd,
    COUNT(*) as days_count
FROM other_rates o
JOIN usd_rates u ON o.rate_date = u.rate_date
GROUP BY o.currency_code
HAVING COUNT(*) > 10
ORDER BY correlation_with_usd DESC;

-- 7. СТАТИСТИКА ПО ЗАГРУЗКАМ
DROP MATERIALIZED VIEW IF EXISTS cbr_dm.mv_load_stats;
CREATE MATERIALIZED VIEW cbr_dm.mv_load_stats AS
SELECT 
    DATE_TRUNC('day', load_timestamp)::DATE as load_date,
    COUNT(*) as records_loaded,
    COUNT(DISTINCT currency_code) as currencies_count,
    COUNT(DISTINCT rate_date) as dates_count,
    MIN(rate_date) as min_date,
    MAX(rate_date) as max_date,
    MIN(load_timestamp) as first_load,
    MAX(load_timestamp) as last_load
FROM cbr_raw.exchange_rates
GROUP BY DATE_TRUNC('day', load_timestamp)
ORDER BY load_date DESC;

-- ============================================
-- 3. УНИКАЛЬНЫЕ ИНДЕКСЫ ДЛЯ CONCURRENTLY REFRESH
-- ============================================
-- Эти индексы необходимы для использования REFRESH MATERIALIZED VIEW CONCURRENTLY

-- Уникальный индекс для mv_daily_changes
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_daily_changes 
ON cbr_dm.mv_daily_changes (currency_code, rate_date);

-- Уникальный индекс для mv_weekly_stats
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_weekly_stats 
ON cbr_dm.mv_weekly_stats (currency_code, week_start);

-- Уникальный индекс для mv_monthly_stats
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_monthly_stats 
ON cbr_dm.mv_monthly_stats (currency_code, month_start);

-- Уникальный индекс для mv_quarterly_stats
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_quarterly_stats 
ON cbr_dm.mv_quarterly_stats (currency_code, quarter_start);

-- Уникальный индекс для mv_top_volatile
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_top_volatile 
ON cbr_dm.mv_top_volatile (currency_code);

-- Уникальный индекс для mv_correlation_with_usd
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_correlation 
ON cbr_dm.mv_correlation_with_usd (currency_code);

-- Уникальный индекс для mv_load_stats
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_load_stats 
ON cbr_dm.mv_load_stats (load_date);

-- ============================================
-- 4. ДОПОЛНИТЕЛЬНЫЕ ИНДЕКСЫ ДЛЯ УСКОРЕНИЯ
-- ============================================

CREATE INDEX IF NOT EXISTS idx_mv_daily_changes ON cbr_dm.mv_daily_changes(currency_code, rate_date);
CREATE INDEX IF NOT EXISTS idx_mv_weekly ON cbr_dm.mv_weekly_stats(currency_code, week_start);
CREATE INDEX IF NOT EXISTS idx_mv_monthly ON cbr_dm.mv_monthly_stats(currency_code, month_start);
CREATE INDEX IF NOT EXISTS idx_mv_quarterly ON cbr_dm.mv_quarterly_stats(currency_code, quarter_start);
CREATE INDEX IF NOT EXISTS idx_mv_correlation ON cbr_dm.mv_correlation_with_usd(correlation_with_usd DESC);

-- ============================================
-- 5. ФУНКЦИИ ДЛЯ ОБНОВЛЕНИЯ
-- ============================================

-- Функция для обновления всех материализованных представлений с CONCURRENTLY
CREATE OR REPLACE FUNCTION cbr_dm.refresh_all_mviews()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_daily_changes;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_weekly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_monthly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_quarterly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_top_volatile;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_correlation_with_usd;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_load_stats;
    
    RAISE NOTICE '✅ Все материализованные представления обновлены CONCURRENTLY';
END;
$$ LANGUAGE plpgsql;

-- Функция для обновления таблиц фактов из сырых данных
CREATE OR REPLACE FUNCTION cbr_dm.sync_facts_from_raw()
RETURNS void AS $$
BEGIN
    -- Обновляем справочник валют
    INSERT INTO cbr_dm.dim_currency (currency_code, currency_name)
    SELECT DISTINCT currency_code, currency_name
    FROM cbr_raw.exchange_rates
    ON CONFLICT (currency_code) DO UPDATE SET
        last_seen = CURRENT_DATE,
        currency_name = EXCLUDED.currency_name;
    
    -- Обновляем факты
    INSERT INTO cbr_dm.fact_exchange_rates (currency_code, exchange_rate, rate_date, rate_type, nominal, load_timestamp)
    SELECT currency_code, exchange_rate, rate_date, rate_type, nominal, load_timestamp
    FROM cbr_raw.exchange_rates
    ON CONFLICT (currency_code, rate_date, rate_type) DO NOTHING;
    
    RAISE NOTICE '✅ Таблицы фактов синхронизированы';
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- 6. ПРОВЕРКА И СТАТИСТИКА
-- ============================================

DO $$
DECLARE
    view_count integer;
    index_count integer;
BEGIN
    SELECT COUNT(*) INTO view_count
    FROM pg_matviews
    WHERE schemaname = 'cbr_dm';
    
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes
    WHERE schemaname = 'cbr_dm'
    AND indexname LIKE 'idx_unique%';
    
    RAISE NOTICE '✅ Создано % материализованных представлений в схеме cbr_dm', view_count;
    RAISE NOTICE '✅ Создано % уникальных индексов для CONCURRENTLY обновления', index_count;
END $$;