const express = require('express');
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

// 2. Get prices for a specific date
app.get('/api/prices', async (req, res) => {
    const { date, commodityId } = req.query;
    if (!date) {
        return res.status(400).json({ error: 'Date parameter (YYYY-MM-DD) is required' });
    }

    const client = await pool.connect();
    try {
        let query = 'SELECT commodity_id, record FROM goods_price_registry WHERE report_date = $1';
        let params = [date];

        if (commodityId) {
            query += ' AND commodity_id = $2';
            params.push(commodityId);
        }

        const result = await client.query(query, params);

        if (result.rows.length === 0) {
            return res.json({});
        }

        if (commodityId) {
            // Return array of records for this commodity
            const records = result.rows.map(row => row.record);
            return res.json(records);
        }

        // Return object keyed by commodity_id: { 1: [...], 2: [...] }
        const dayData = {};
        for (const row of result.rows) {
            const cid = row.commodity_id;
            if (!dayData[cid]) {
                dayData[cid] = [];
            }
            dayData[cid].push(row.record);
        }

        res.json(dayData);
    } catch (error) {
        console.error("Failed to read prices data:", error);
        res.status(500).json({ error: 'Failed to read prices data' });
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
