const { Pool } = require('pg');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

// Workaround for Supabase self-signed certs
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const rawUrl = process.env.POSTGRES_URL_NON_POOLING || process.env.POSTGRES_URL;
const connectionString = rawUrl ? rawUrl.replace(/^["'](.*)["']$/, '$1') : undefined;

const pool = new Pool({
    connectionString,
    ssl: {
        rejectUnauthorized: false
    }
});

console.log("DB Config - Connection String exists:", !!(process.env.POSTGRES_URL_NON_POOLING || process.env.POSTGRES_URL));
console.log("DB Config - Using Non-Pooling:", !!process.env.POSTGRES_URL_NON_POOLING);

async function initDB() {
    const client = await pool.connect();
    try {
        console.log("Initializing database...");

        // 1. Agmark Sales Data (Raw)
        await client.query(`
            CREATE TABLE IF NOT EXISTS agmark_sales_data (
                id SERIAL PRIMARY KEY,
                report_date DATE NOT NULL,
                commodity_id INTEGER NOT NULL,
                cmdt_name TEXT,
                cmdt_grp_name TEXT,
                market_name TEXT,
                district_name TEXT,
                state_name TEXT,
                grade_name TEXT,
                variety_name TEXT,
                unit_name_price TEXT,
                min_price NUMERIC,
                max_price NUMERIC,
                model_price NUMERIC,
                arrival_date TEXT,
                commodity_uuiq TEXT,
                record_uuiq TEXT,
                is_normalized BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, commodity_id, market_name, variety_name, grade_name)
            );
        `);

        // 2. eNAM Sales Data (Raw)
        await client.query(`
            CREATE TABLE IF NOT EXISTS enam_sales_data (
                id SERIAL PRIMARY KEY,
                enam_id TEXT,
                state_name TEXT,
                apmc_name TEXT,
                commodity_name TEXT,
                min_price NUMERIC,
                modal_price NUMERIC,
                max_price NUMERIC,
                commodity_arrivals NUMERIC,
                commodity_traded NUMERIC,
                created_at_api DATE,
                status TEXT,
                unit_name_price TEXT,
                report_date DATE NOT NULL,
                commodity_uuiq TEXT,
                record_uuiq TEXT,
                is_normalized BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, enam_id, apmc_name, commodity_name)
            );
        `);

        // 3. Common Market Prices Table (Staging)
        await client.query(`
            CREATE TABLE IF NOT EXISTS market_prices_common (
                id SERIAL PRIMARY KEY,
                state_name TEXT,
                district_name TEXT,
                market_name TEXT,
                commodity_name TEXT,
                commodity_uuiq TEXT,
                min_price NUMERIC,
                max_price NUMERIC,
                model_price NUMERIC,
                unit TEXT,
                source TEXT,
                report_date DATE NOT NULL,
                record_uuiq TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, source, state_name, market_name, commodity_name)
            );
        `);

        // 4. Common Market Arrivals Table (Staging)
        await client.query(`
            CREATE TABLE IF NOT EXISTS market_arrivals_common (
                id SERIAL PRIMARY KEY,
                state_name TEXT,
                market_name TEXT,
                commodity_name TEXT,
                commodity_uuiq TEXT,
                arrival_quantity NUMERIC,
                arrival_unit TEXT,
                source TEXT,
                report_date DATE NOT NULL,
                record_uuiq TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, source, state_name, market_name, commodity_name)
            );
        `);

        // 5. Market Trends Summary Table (Pre-Aggregated)
        await client.query(`
            CREATE TABLE IF NOT EXISTS market_trends_summary (
                id SERIAL PRIMARY KEY,
                commodity_uuiq TEXT NOT NULL,
                report_date DATE NOT NULL,
                avg_model_price NUMERIC,
                unit TEXT,
                period_type TEXT, -- 'daily', 'weekly', 'monthly'
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_uuiq, report_date, period_type)
            );
        `);

        // 6. Data Normalization Stats (Health Dashboard)
        await client.query(`
            CREATE TABLE IF NOT EXISTS data_normalization_stats (
                id SERIAL PRIMARY KEY,
                report_date DATE NOT NULL,
                source TEXT,
                raw_count INTEGER,
                processed_count INTEGER,
                yield_percentage NUMERIC,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, source)
            );
        `);

        // 7. Explicitly add record_uuiq and is_normalized columns if they don't exist (safety for existing tables)
        await client.query('ALTER TABLE agmark_sales_data ADD COLUMN IF NOT EXISTS record_uuiq TEXT');
        await client.query('ALTER TABLE agmark_sales_data ADD COLUMN IF NOT EXISTS is_normalized BOOLEAN DEFAULT FALSE');
        await client.query('ALTER TABLE enam_sales_data ADD COLUMN IF NOT EXISTS record_uuiq TEXT');
        await client.query('ALTER TABLE enam_sales_data ADD COLUMN IF NOT EXISTS is_normalized BOOLEAN DEFAULT FALSE');
        await client.query('ALTER TABLE market_prices_common ADD COLUMN IF NOT EXISTS record_uuiq TEXT');
        await client.query('ALTER TABLE market_arrivals_common ADD COLUMN IF NOT EXISTS record_uuiq TEXT');

        // 8. Indexes for optimization
        await client.query('CREATE INDEX IF NOT EXISTS idx_prices_uuiq_date ON market_prices_common(commodity_uuiq, report_date)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_arrivals_uuiq_date ON market_arrivals_common(commodity_uuiq, report_date)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_agmark_uuiq_date ON agmark_sales_data(commodity_uuiq, report_date)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_enam_uuiq_date ON enam_sales_data(commodity_uuiq, report_date)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_agmark_record_uuiq ON agmark_sales_data(record_uuiq)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_enam_record_uuiq ON enam_sales_data(record_uuiq)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_prices_record_uuiq ON market_prices_common(record_uuiq)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_arrivals_record_uuiq ON market_arrivals_common(record_uuiq)');
        await client.query('CREATE INDEX IF NOT EXISTS idx_agmark_is_normalized ON agmark_sales_data(is_normalized) WHERE is_normalized = FALSE');
        await client.query('CREATE INDEX IF NOT EXISTS idx_enam_is_normalized ON enam_sales_data(is_normalized) WHERE is_normalized = FALSE');
        await client.query('CREATE INDEX IF NOT EXISTS idx_trends_uuiq_date ON market_trends_summary(commodity_uuiq, report_date)');

        console.log("All tables and indexes initialized successfully.");
    } catch (err) {
        console.error("Error initializing database:", err);
    } finally {
        client.release();
    }
}

module.exports = { pool, initDB };
