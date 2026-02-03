const express = require('express'); // Updated for Vercel deployment
const cron = require('node-cron');
const morgan = require('morgan');
const fs = require('fs-extra');
const path = require('path');
const { runDailyFetch } = require('./fetcher');
const cors = require('cors');
const { initDB, pool } = require('./db');
const dayjs = require('dayjs');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 5050;

app.disable('etag');
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

// 2. Get prices for a specific date with pagination and filtering
app.get('/api/prices', async (req, res) => {
    const { fromDate, toDate, date, commodityId, page = 1, limit = 20, search = '', groupId, onlyWithPrices = 'false' } = req.query;

    const dFrom = fromDate || date;
    const dTo = toDate || date;

    if (!dFrom || !dTo) {
        return res.status(400).json({ error: 'Date range parameters (fromDate, toDate or date) are required' });
    }

    const p = parseInt(page);
    const l = parseInt(limit);
    const offset = (p - 1) * l;

    const client = await pool.connect();
    try {
        // 1. Get metadata and filter by search/groupId
        const metadata = await fs.readJson(DATA_PATH);
        let commodities = metadata.data?.cmdt_data || [];

        if (search) {
            const lowSearch = search.toLowerCase();
            commodities = commodities.filter(c => c.cmdt_name.toLowerCase().includes(lowSearch));
        }

        if (groupId) {
            commodities = commodities.filter(c => String(c.cmdt_group_id) === String(groupId));
        }

        if (commodityId) {
            commodities = commodities.filter(c => String(c.cmdt_id) === String(commodityId));
        }

        const nameToIdMap = {};
        commodities.forEach(c => {
            nameToIdMap[c.cmdt_name.toLowerCase()] = c.cmdt_id;
        });

        const commodityNames = commodities.map(c => c.cmdt_name.toLowerCase());

        // 2. Identify commodities with prices if needed
        let activeCommodityNames = commodityNames;
        if (onlyWithPrices === 'true' || search || groupId || page > 1) {
            // We need to know which ones have data to paginate correctly
            const activeRes = await client.query(
                `SELECT DISTINCT LOWER(commodity_name) as name 
                 FROM market_prices_common 
                 WHERE report_date BETWEEN $1 AND $2 AND LOWER(commodity_name) = ANY($3)`,
                [dFrom, dTo, commodityNames]
            );
            activeCommodityNames = activeRes.rows.map(r => r.name);
        }

        // 3. Final list of commodity IDs to return for this page
        // If onlyWithPrices is false, we include everything from 'commodities'
        // If true, we only include those in 'activeCommodityNames'
        let finalCommodityList = onlyWithPrices === 'true'
            ? commodities.filter(c => activeCommodityNames.includes(c.cmdt_name.toLowerCase()))
            : commodities;

        const total = finalCommodityList.length;
        const pageItems = finalCommodityList.slice(offset, offset + l);
        const pageNames = pageItems.map(c => c.cmdt_name.toLowerCase());

        // 4. Fetch the full record data for these specific commodities
        if (pageItems.length === 0) {
            return res.json({
                data: {},
                pagination: { total, page: p, limit: l, hasMore: false }
            });
        }

        const query = `
            SELECT 
                p.commodity_name, 
                p.state_name, 
                p.district_name,
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
            WHERE p.report_date BETWEEN $1 AND $2 AND LOWER(p.commodity_name) = ANY($3)
        `;

        const result = await client.query(query, [dFrom, dTo, pageNames]);

        const recordsByCid = {};
        for (const row of result.rows) {
            const lowerName = (row.commodity_name || '').toLowerCase();
            const cid = nameToIdMap[lowerName];
            if (cid) {
                if (!recordsByCid[cid]) recordsByCid[cid] = [];
                recordsByCid[cid].push({ ...row, cmdt_name: row.commodity_name });
            }
        }

        const data = pageItems.map(c => ({
            ...c,
            records: recordsByCid[c.cmdt_id] || []
        }));

        res.json({
            data,
            pagination: {
                total,
                page: p,
                limit: l,
                hasMore: offset + l < total
            }
        });
    } catch (error) {
        console.error("Failed to fetch paginated prices:", error);
        res.status(500).json({ error: 'Failed to fetch prices data' });
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

// 4. Get Exchange Rates (Live Market vs HDFC Indicative)
app.get('/api/exchange-rates', async (req, res) => {
    try {
        // Fetching real-time rates from ExchangeRate-API (Standard Source)
        // This provider is more accurate for INR/Standard benchmarks
        const response = await axios.get('https://api.exchangerate-api.com/v4/latest/USD');

        if (!response.data || !response.data.rates || !response.data.rates.INR) {
            throw new Error("Invalid response from exchange rate API");
        }

        const rbiRate = response.data.rates.INR;
        const hdfcMarkup = 0.0085; // Standard ~0.85% bank markup
        const hdfcRate = rbiRate * (1 + hdfcMarkup);

        res.json({
            data: {
                rbi: {
                    USD: parseFloat(rbiRate.toFixed(4)),
                    date: response.data.date,
                    source: 'Standard Forex Reference (Dynamic Market Rate)'
                },
                hdfc: {
                    USD: parseFloat(hdfcRate.toFixed(4)),
                    date: response.data.date,
                    source: 'HDFC Bank Indicative Forex Rates (Market + Markup)'
                },
                lastUpdated: new Date().toISOString()
            }
        });
    } catch (error) {
        console.error("Failed to fetch live exchange rates:", error.message);
        res.status(500).json({ error: 'Failed to fetch real-time exchange rates' });
    }
});

// 5. Cron Job: Daily at 12:00
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


