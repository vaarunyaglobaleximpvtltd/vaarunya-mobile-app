const dayjs = require('dayjs');
const { runDailyFetch } = require('./fetcher');
const { runEnamFetch } = require('./fetcher_enam');
const { runNormalization } = require('./normalizer');
const { pool } = require('./db');

async function backfill() {
    const startDate = dayjs('2026-01-14'); // Resuming from Jan 14th
    const endDate = dayjs(); // Today

    let currentDate = startDate;

    console.log(`Starting/Resuming backfill from ${startDate.format('YYYY-MM-DD')} to ${endDate.format('YYYY-MM-DD')}`);

    while (currentDate.isBefore(endDate) || currentDate.isSame(endDate)) {
        const dateStr = currentDate.format('YYYY-MM-DD');
        console.log(`\n=== Processing ${dateStr} ===`);

        try {
            console.log(`[1/3] Fetching Agmark Data for ${dateStr}...`);
            await runDailyFetch(dateStr);

            console.log(`[2/3] Fetching eNAM Data for ${dateStr}...`);
            await runEnamFetch(dateStr);

            console.log(`[3/3] Normalizing Data for ${dateStr}...`);
            await runNormalization(dateStr);
        } catch (error) {
            console.error(`ERROR processing ${dateStr}:`, error.message);
            console.log(`Wait 5s and try next day...`);
            await new Promise(resolve => setTimeout(resolve, 5000));
        }

        currentDate = currentDate.add(1, 'day');
    }

    console.log('\nBackfill Complete.');
    try {
        await pool.end();
    } catch (e) {
        console.error("Error closing pool:", e.message);
    }
}

backfill();
