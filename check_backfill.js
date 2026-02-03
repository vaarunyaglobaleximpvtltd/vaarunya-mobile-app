const { pool } = require('./db');

async function checkProgress() {
    try {
        const agmarkRes = await pool.query('SELECT MAX(report_date) as max_date FROM agmark_sales_data');
        const enamRes = await pool.query('SELECT MAX(report_date) as max_date FROM enam_sales_data');
        const commonRes = await pool.query('SELECT MAX(report_date) as max_date FROM market_prices_common');

        console.log('Agmark Latest Date:', agmarkRes.rows[0].max_date);
        console.log('eNAM Latest Date:', enamRes.rows[0].max_date);
        console.log('Common Market Latest Date:', commonRes.rows[0].max_date);

        const countsRes = await pool.query(`
            SELECT 
                (SELECT COUNT(*) FROM agmark_sales_data) as agmark_count,
                (SELECT COUNT(*) FROM enam_sales_data) as enam_count,
                (SELECT COUNT(*) FROM market_prices_common) as common_count
        `);
        console.log('Record Counts:', countsRes.rows[0]);

    } catch (err) {
        console.error('Error checking progress:', err.message);
    } finally {
        await pool.end();
    }
}

checkProgress();
