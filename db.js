const { Pool } = require('pg');
require('dotenv').config();

// Workaround for Supabase self-signed certs
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const pool = new Pool({
    connectionString: process.env.POSTGRES_URL,
    ssl: {
        rejectUnauthorized: false
    }
});

async function initDB() {
    const client = await pool.connect();
    try {
        console.log("Initializing database...");

        // Create table for storing fetched price records
        // We use JSONB for the 'record' column to store the full object returned by Agmarknet API
        // This provides flexibility and matches the 'raw_data' approach
        await client.query(`
            CREATE TABLE IF NOT EXISTS goods_price_registry (
                id SERIAL PRIMARY KEY,
                report_date DATE NOT NULL,
                commodity_id INTEGER NOT NULL,
                record JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, commodity_id, record)
            );
        `);
        // Note: The unique constraint is a bit loose here by including 'record', 
        // ideally we'd key off market+commodity+date but the record structure is complex inside.
        // For simple de-duplication of exact same rows, this works. 
        // If we fetch again for same day and data varies slightly, we might get duplicates if we don't clear old data.
        // A safer approach for "Refresh" is to DELETE for the date before inserting, 
        // OR simpler: Just append and let filtering handle it. 
        // Let's rely on app filtering unique? No, better to manage duplicates.
        // Let's assume on re-fetch we might want to overwrite or ignore existing.
        // For simplicity: We will rely on the fetcher logic to handle this.

        console.log("Database initialized successfully.");
    } catch (err) {
        console.error("Error initializing database:", err);
    } finally {
        client.release();
    }
}

module.exports = { pool, initDB };
