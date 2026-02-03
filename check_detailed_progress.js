const { pool } = require('./db');

async function checkDetailedProgress() {
    try {
        console.log('--- Agmark Records per Date ---');
        const agmarkRes = await pool.query(`
            SELECT report_date, COUNT(*) 
            FROM agmark_sales_data 
            WHERE report_date >= '2026-01-05'
            GROUP BY report_date 
            ORDER BY report_date DESC
            LIMIT 10
        `);
        console.table(agmarkRes.rows);

        console.log('--- eNAM Records per Date ---');
        const enamRes = await pool.query(`
            SELECT report_date, COUNT(*) 
            FROM enam_sales_data 
            WHERE report_date >= '2026-01-05'
            GROUP BY report_date 
            ORDER BY report_date DESC
            LIMIT 10
        `);
        console.table(enamRes.rows);

        console.log('--- Common Market Records per Date ---');
        const commonRes = await pool.query(`
            SELECT report_date, COUNT(*) 
            FROM market_prices_common 
            WHERE report_date >= '2026-01-05'
            GROUP BY report_date 
            ORDER BY report_date DESC
            LIMIT 10
        `);
        console.table(commonRes.rows);

    } catch (err) {
        console.error('Error:', err.message);
    } finally {
        await pool.end();
    }
}

checkDetailedProgress();
