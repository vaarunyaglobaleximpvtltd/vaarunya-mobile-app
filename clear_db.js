const { pool } = require('./db');

async function clearDatabase() {
    const client = await pool.connect();
    try {
        console.log("Clearing all market data tables...");
        await client.query('TRUNCATE TABLE agmark_sales_data RESTART IDENTITY CASCADE');
        await client.query('TRUNCATE TABLE enam_sales_data RESTART IDENTITY CASCADE');
        await client.query('TRUNCATE TABLE market_prices_common RESTART IDENTITY CASCADE');
        await client.query('TRUNCATE TABLE market_arrivals_common RESTART IDENTITY CASCADE');
        console.log("Database cleared successfully.");
    } catch (err) {
        console.error("Error clearing database:", err);
    } finally {
        client.release();
        pool.end();
    }
}

clearDatabase();
