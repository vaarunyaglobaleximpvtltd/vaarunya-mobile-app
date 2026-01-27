const express = require('express');
const cron = require('node-cron');
const morgan = require('morgan');
const fs = require('fs-extra');
const path = require('path');
const { runDailyFetch } = require('./fetcher');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 5050;

app.use(cors());
app.use(morgan('dev'));
app.use(express.json());

const PRICES_PATH = path.join(__dirname, 'prices.json');
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

    try {
        const prices = await fs.readJson(PRICES_PATH);
        const dayData = prices[date];

        if (!dayData) {
            return res.status(404).json({ error: 'No data found for this date' });
        }

        if (commodityId) {
            return res.json(dayData[commodityId] || []);
        }

        res.json(dayData);
    } catch (error) {
        res.status(500).json({ error: 'Failed to read prices data' });
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
    runDailyFetch(date); // run in background
    res.json({ message: 'Fetch triggered successfully' });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log('Daily cron job scheduled for 12:00 PM');
});
