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

        // Create table for storing fetched price records
        // Using explicit columns as requested
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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, commodity_id, market_name, variety_name, grade_name)
            );
        `);

        console.log("Database initialized successfully (agmark_sales_data).");

        // 2. Create table for eNAM data
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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, enam_id, apmc_name, commodity_name)
            );
        `);
        console.log("Database initialized successfully (enam_sales_data).");

        // 3. Common Market Prices Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS market_prices_common (
                id SERIAL PRIMARY KEY,
                state_name TEXT,
                district_name TEXT,
                market_name TEXT,
                commodity_name TEXT,
                min_price NUMERIC,
                max_price NUMERIC,
                model_price NUMERIC,
                unit TEXT,
                source TEXT,
                report_date DATE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, source, state_name, market_name, commodity_name)
            );
        `);
        console.log("Database initialized successfully (market_prices_common).");

        // 4. Common Market Arrivals Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS market_arrivals_common (
                id SERIAL PRIMARY KEY,
                state_name TEXT,
                market_name TEXT,
                commodity_name TEXT,
                arrival_quantity NUMERIC,
                arrival_unit TEXT,
                source TEXT,
                report_date DATE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, source, state_name, market_name, commodity_name)
            );
        `);
        console.log("Database initialized successfully (market_arrivals_common).");

        console.log("All tables initialized successfully.");
    } catch (err) {
        console.error("Error initializing database:", err);
    } finally {
        client.release();
    }
}

module.exports = { pool, initDB };
