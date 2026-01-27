const { runDailyFetch } = require('./fetcher');
const dayjs = require('dayjs');

async function backfill(days) {
    const today = dayjs();
    for (let i = 1; i <= days; i++) {
        const date = today.subtract(i, 'day').format('YYYY-MM-DD');
        console.log(`Backfilling for ${date}...`);
        await runDailyFetch(date);
    }
}

const daysArg = process.argv[2] ? parseInt(process.argv[2]) : 7;
backfill(daysArg).catch(console.error);
