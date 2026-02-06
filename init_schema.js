const { initDB, pool } = require('./db');

async function resetSchema() {
    try {
        await initDB();
        console.log("Database schema reset and initialized.");
    } catch (err) {
        console.error("Schema reset failed:", err);
    } finally {
        await pool.end();
    }
}

resetSchema();
