const dayjs = require('dayjs');
const { runDailyFetch } = require('./fetcher');
const { runEnamFetch } = require('./fetcher_enam');
const { runNormalization } = require('./normalizer');
const { pool } = require('./db');

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function runBackfill() {
    const today = dayjs();

    // Find last processed date from DB
    let lastDateRes = await pool.query('SELECT MAX(report_date) as max_date FROM agmark_sales_data');
    let current;

    if (lastDateRes.rows[0].max_date) {
        current = dayjs(lastDateRes.rows[0].max_date).add(1, 'day');
        console.log(`Resuming backfill from last DB date: ${current.format('YYYY-MM-DD')}`);
    } else {
        current = dayjs('2026-01-01');
        console.log(`Starting fresh backfill from: ${current.format('YYYY-MM-DD')}`);
    }

    console.log(`Global backfill target range: ${current.format('YYYY-MM-DD')} to ${today.format('YYYY-MM-DD')}...`);

    while (current.isBefore(today) || current.isSame(today, 'day')) {
        const dateStr = current.format('YYYY-MM-DD');
        console.log(`\n=== Processing Date: ${dateStr} ===`);

        let success = false;
        let attempts = 0;
        const maxAttempts = 3;

        while (!success && attempts < maxAttempts) {
            attempts++;
            try {
                // 1. Fetch Agmark
                console.log(`  [Agmark] Starting (Attempt ${attempts})...`);
                await runDailyFetch(dateStr);

                // 2. Fetch eNAM
                console.log(`  [eNAM] Starting...`);
                await runEnamFetch(dateStr);

                // 3. Normalize
                console.log(`  [Normalization] Starting...`);
                await runNormalization(dateStr);

                success = true;
                console.log(`--- Finished Date: ${dateStr} ---`);
            } catch (err) {
                console.error(`!!! Error on ${dateStr} (Attempt ${attempts}):`, err.message);
                if (attempts < maxAttempts) {
                    console.log(`Waiting 10s before retry...`);
                    await sleep(10000);
                } else {
                    console.error(`!!! FAILED all attempts for ${dateStr}. Moving to next day.`);
                }
            }
        }

        // Small break between days to avoid overwhelming the DB/APIs
        await sleep(5000);
        current = current.add(1, 'day');
    }

    console.log("Global backfill completed.");
    await pool.end();
}

runBackfill();
