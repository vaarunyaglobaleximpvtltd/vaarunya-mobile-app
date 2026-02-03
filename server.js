const express = require('express'); // Updated for Vercel deployment
const cron = require('node-cron');
const morgan = require('morgan');
const fs = require('fs-extra');
const path = require('path');
const { runDailyFetch } = require('./fetcher');
const cors = require('cors');
const { initDB, pool } = require('./db');
const dayjs = require('dayjs');

const app = express();
const PORT = process.env.PORT || 5050;

app.use(cors());
app.use(morgan('dev'));
app.use(express.json());

// Initialize DB
initDB();

// const PRICES_PATH = path.join(__dirname, 'prices.json'); // Removed
const DATA_PATH = path.join(__dirname, 'data.json');

// API Endpoints

// 1. Get metadata (groups, commodities)
app.get('/api/metadata', async (req, res) => {
    try {
        const data = await fs.readJson(DATA_PATH);
        res.json(data);
    } catch (error) {
        res.status(500).json({ error: 'Failed to read metadata' });
    }
});

// 2. Get prices for a specific date ( Unified from market_prices_common )
app.get('/api/prices', async (req, res) => {
    const { date, commodityId } = req.query;
    if (!date) {
        return res.status(400).json({ error: 'Date parameter (YYYY-MM-DD) is required' });
    }

    const client = await pool.connect();
    try {
        // Load metadata to map names to IDs
        const metadata = await fs.readJson(DATA_PATH);
        const nameToIdMap = {};
        if (metadata.data && metadata.data.cmdt_data) {
            metadata.data.cmdt_data.forEach(c => {
                nameToIdMap[c.cmdt_name.toLowerCase()] = c.cmdt_id;
            });
        }

        let query = `
            SELECT 
                p.commodity_name, 
                p.state_name, 
                p.market_name, 
                p.min_price, 
                p.max_price, 
                p.model_price, 
                p.unit as unit_name_price, 
                p.source,
                p.report_date as arrival_date,
                e.commodity_arrivals,
                e.commodity_traded
            FROM market_prices_common p
            LEFT JOIN enam_sales_data e 
                ON p.source = 'eNAM' 
                AND p.report_date = e.report_date 
                AND p.state_name = e.state_name 
                AND p.market_name = e.apmc_name 
                AND p.commodity_name = e.commodity_name
            WHERE p.report_date = $1
        `;
        let params = [date];

        // Note: Filtering by commodityId in params is tricky if we don't have ID in table.
        // We will fetch all and filter in JS if commodityId is present, or join if we had IDs.
        // Given specific request usually fetches all for the day, fetching all is fine.

        const result = await client.query(query, params);

        if (result.rows.length === 0) {
            return res.json({});
        }

        const dayData = {};
        for (const row of result.rows) {
            const lowerName = (row.commodity_name || '').toLowerCase();
            const cid = nameToIdMap[lowerName];

            // If we find a matching ID, group it. 
            // If eNAM has a name not in Agmark metadata, we currently skip it or could put in 'others'
            if (cid) {
                if (commodityId && String(cid) !== String(commodityId)) {
                    continue;
                }

                if (!dayData[cid]) {
                    dayData[cid] = [];
                }
                dayData[cid].push({
                    ...row,
                    cmdt_name: row.commodity_name, // Ensure compat
                    cmdt_grp_name: 'Unknown', // We could map this too if needed from metadata
                });
            }
        }

        if (commodityId && dayData[commodityId]) {
            return res.json(dayData[commodityId]);
        } else if (commodityId) {
            return res.json([]);
        }

        res.json(dayData);
    } catch (error) {
        console.error("Failed to read prices data:", error);
        res.status(500).json({ error: 'Failed to read prices data' });
    } finally {
        client.release();
    }
});

// 3. Get history for a commodity (aggregated by date)
app.get('/api/history', async (req, res) => {
    const { commodity_name, duration } = req.query; // duration: '1M', '1W', 'ALL'
    if (!commodity_name) {
        return res.status(400).json({ error: 'commodity_name is required' });
    }

    const client = await pool.connect();
    try {
        let dateFilter = '';
        let params = [commodity_name];

        let days = 30;
        if (duration === '1W') days = 7;
        if (duration === 'ALL') days = 365; // Cap at 1 year for safety

        // Calculate start date
        const startDate = dayjs().subtract(days, 'day').format('YYYY-MM-DD');
        params.push(startDate);

        const query = `
            SELECT 
                report_date as date,
                AVG(model_price) as avg_model_price,
                MIN(min_price) as min_price,
                MAX(max_price) as max_price,
                COUNT(*) as market_count
            FROM market_prices_common
            WHERE 
                LOWER(commodity_name) = LOWER($1) 
                AND report_date >= $2
            GROUP BY report_date
            ORDER BY report_date ASC
        `;

        const result = await client.query(query, params);

        // Format numbers
        const history = result.rows.map(row => ({
            date: dayjs(row.date).format('YYYY-MM-DD'),
            price: Math.round(parseFloat(row.avg_model_price)),
            min: Math.round(parseFloat(row.min_price)),
            max: Math.round(parseFloat(row.max_price)),
            markets: parseInt(row.market_count)
        }));

        res.json(history);
    } catch (error) {
        console.error("Failed to fetch history:", error);
        res.status(500).json({ error: 'Failed to fetch history' });
    } finally {
        client.release();
    }
});

// Cron Job: Daily at 12:00
cron.schedule('0 12 * * *', () => {
    console.log('Running daily cron job at 12:00...');
    runDailyFetch();
});

// Manually trigger fetch (for testing)
app.post('/api/fetch/trigger', async (req, res) => {
    const { date } = req.body;
    // Run in background, don't await
    runDailyFetch(date).catch(err => console.error("Background fetch failed:", err));
    res.json({ message: 'Fetch triggered successfully' });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log('Daily cron job scheduled for 12:00 PM');
});
