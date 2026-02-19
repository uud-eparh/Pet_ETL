-- Таблица для хранения курсов валют в схеме cbr_raw

-- Создаем таблицу в схеме cbr_raw
CREATE TABLE IF NOT EXISTS cbr_raw.exchange_rates (
    id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(100) NOT NULL,
    exchange_rate DECIMAL(12,6) NOT NULL,
    rate_date DATE NOT NULL,
    rate_type VARCHAR(10) NOT NULL CHECK (rate_type IN ('daily', 'monthly')),
    nominal INTEGER DEFAULT 1,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Уникальный ключ, чтобы избежать дублирования данных
    UNIQUE(currency_code, rate_date, rate_type)
);

-- Индексы для ускорения поиска
CREATE INDEX IF NOT EXISTS idx_exchange_rates_date ON cbr_raw.exchange_rates(rate_date);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_currency ON cbr_raw.exchange_rates(currency_code);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_type ON cbr_raw.exchange_rates(rate_type);

-- Комментарии к таблице и полям
COMMENT ON TABLE cbr_raw.exchange_rates IS 'Хранение курсов валют ЦБ РФ (ежедневные и ежемесячные)';
COMMENT ON COLUMN cbr_raw.exchange_rates.rate_type IS 'Тип курса: daily - ежедневный, monthly - среднемесячный';
COMMENT ON COLUMN cbr_raw.exchange_rates.nominal IS 'Номинал валюты (например, 100 для японских йен)';

-- Сообщение о завершении
DO $$
BEGIN
    RAISE NOTICE 'Таблица cbr_raw.exchange_rates успешно создана';
END $$;