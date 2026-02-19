-- Создание схем для проекта ETL

-- Схема для сырых данных (raw data)
CREATE SCHEMA IF NOT EXISTS cbr_raw;

-- Схема для витрины данных (data mart)
CREATE SCHEMA IF NOT EXISTS cbr_dm;

-- Даем права пользователю admin на все схемы
GRANT ALL ON SCHEMA cbr_raw TO admin;
GRANT ALL ON SCHEMA cbr_dm TO admin;

-- Сообщение о завершении
DO $$
BEGIN
    RAISE NOTICE 'Схемы cbr_raw и cbr_dm успешно созданы';
END $$;