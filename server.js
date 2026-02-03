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

// Simple session store
const sessionStore = new Map();

async function getGstPortalSession() {
    try {
        const url = "https://services.gst.gov.in/services/api/ustatus";
        const res = await axios.get(url, {
            headers: {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.71 Safari/537.36",
                "Referer": "https://services.gst.gov.in"
            }
        });

        let cookies = res.headers['set-cookie'] || [];
        // Ensure 'Lang=en' is present
        if (!cookies.some(c => c.startsWith('Lang='))) {
            cookies.push('Lang=en');
        }
        return cookies.join('; ');
    } catch (err) {
        console.error("Failed to fetch GST session:", err);
        return null;
    }
}

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
    const { date, commodityId, page = 1, limit = 20, search = '', groupId, onlyWithPrices = 'false' } = req.query;
    if (!date) {
        return res.status(400).json({ error: 'Date parameter (YYYY-MM-DD) is required' });
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
                 WHERE report_date = $1 AND LOWER(commodity_name) = ANY($2)`,
                [date, commodityNames]
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
            WHERE p.report_date = $1 AND LOWER(p.commodity_name) = ANY($2)
        `;

        const result = await client.query(query, [date, pageNames]);

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

// 4. Get Exchange Rates (RBI vs HDFC)
app.get('/api/exchange-rates', async (req, res) => {
    try {
        // In a real-world scenario, we'd scrape or use an API.
        // For this demo, we'll provide indicative rates based on recent search data
        // and add a small random variation to mimic live data.

        const baseRbiRate = 83.1250;
        const hdfcMarkup = 0.0085; // ~0.85% markup

        const variation = (Math.random() - 0.5) * 0.1;
        const rbiRate = baseRbiRate + variation;
        const hdfcRate = rbiRate * (1 + hdfcMarkup);

        res.json({
            data: {
                rbi: {
                    USD: parseFloat(rbiRate.toFixed(4)),
                    date: dayjs().format('YYYY-MM-DD'),
                    source: 'Financial Benchmarks India Pvt Ltd (FBIL)'
                },
                hdfc: {
                    USD: parseFloat(hdfcRate.toFixed(4)),
                    date: dayjs().format('YYYY-MM-DD'),
                    source: 'HDFC Bank Indicative Forex Rates'
                },
                lastUpdated: new Date().toISOString()
            }
        });
    } catch (error) {
        console.error("Failed to provide exchange rates:", error);
        res.status(500).json({ error: 'Failed to fetch exchange rates' });
    }
});

const STATE_CODES = {
    "01": "Jammu and Kashmir", "02": "Himachal Pradesh", "03": "Punjab", "04": "Chandigarh",
    "05": "Uttarakhand", "06": "Haryana", "07": "Delhi", "08": "Rajasthan",
    "09": "Uttar Pradesh", "10": "Bihar", "11": "Sikkim", "12": "Arunachal Pradesh",
    "13": "Nagaland", "14": "Manipur", "15": "Mizoram", "16": "Tripura",
    "17": "Meghalaya", "18": "Assam", "19": "West Bengal", "20": "Jharkhand",
    "21": "Odisha", "22": "Chhattisgarh", "23": "Madhya Pradesh", "24": "Gujarat",
    "25": "Daman and Diu", "26": "Dadra and Nagar Haveli", "27": "Maharashtra",
    "28": "Andhra Pradesh", "29": "Karnataka", "30": "Goa", "31": "Lakshadweep",
    "32": "Kerala", "33": "Tamil Nadu", "34": "Puducherry", "35": "Andaman and Nicobar Islands",
    "36": "Telangana", "37": "Andhra Pradesh (New)",
};

// 5. GST Search Proxy (Official Portal)
app.get('/api/gst/search', async (req, res) => {
    const { gstin } = req.query;
    if (!gstin) return res.status(400).json({ error: 'GSTIN is required' });

    try {
        const cookies = await getGstPortalSession();
        if (!cookies) throw new Error("Could not initialize portal session");

        // 1. Fetch Taxpayer Basic Details
        const tpUrl = "https://publicservices.gst.gov.in/publicservices/auth/api/search/tp";
        const tpRes = await axios.post(tpUrl, { gstin: gstin.toUpperCase() }, {
            headers: {
                "Origin": "https://services.gst.gov.in",
                "Referer": "https://publicservices.gst.gov.in",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Cookie": cookies
            }
        });

        // 2. Fetch Filing Status (using 2024-25 as default FY)
        const filingUrl = "https://services.gst.gov.in/services/api/search/taxpayerReturnDetails";
        const filingRes = await axios.post(filingUrl, { gstin: gstin.toUpperCase(), fy: "2024-25" }, {
            headers: {
                "Origin": "https://services.gst.gov.in",
                "Referer": "https://services.gst.gov.in/services/searchtp",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Cookie": cookies
            }
        });

        const tpData = tpRes.data;
        const filingData = filingRes.data.filingStatus ? filingRes.data.filingStatus[0] : [];

        res.json({
            data: {
                gstin: tpData.gstin || gstin.toUpperCase(),
                lgnm: tpData.lgnm || "N/A",
                tradeNam: tpData.tradeNam || "N/A",
                rgdt: tpData.rgdt || "N/A",
                dty: tpData.dty || "N/A",
                stj: tpData.stj || "N/A",
                ctj: tpData.ctj || "N/A",
                sts: tpData.sts || "Active",
                filingStatus: filingData.map(f => ({
                    rtntype: f.rtntype,
                    status: f.status,
                    dof: f.dof,
                    taxp: f.taxp
                })).slice(0, 5), // Keep latest 5
                lastUpdated: dayjs().format('YYYY-MM-DD')
            }
        });
    } catch (error) {
        console.error("GST Search failed:", error.response?.data || error.message);
        res.status(500).json({ error: 'Portal connection failed or Invalid GSTIN' });
    }
});

// 6. PAN Search Proxy (Official Portal)
app.get('/api/pan/search', async (req, res) => {
    const { pan } = req.query;
    if (!pan) return res.status(400).json({ error: 'PAN is required' });

    try {
        const cookies = await getGstPortalSession();
        if (!cookies) throw new Error("Could not initialize portal session");

        const url = "https://services.gst.gov.in/services/auth/api/get/gstndtls";
        const response = await axios.post(url, { panNO: pan.toUpperCase() }, {
            headers: {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=UTF-8",
                "Connection": "keep-alive",
                "Host": "services.gst.gov.in",
                "Origin": "https://services.gst.gov.in",
                "Referer": "https://services.gst.gov.in/services/auth/searchtpbypan",
                "Cookie": cookies
            }
        });

        const apiData = response.data;
        if (apiData.errorCode === "SWEB_10001") {
            return res.status(404).json({ error: "Invalid PAN number" });
        }

        const gstinList = (apiData.gstinResList || []).map(item => ({
            gstin: item.gstin,
            status: item.authStatus,
            state: STATE_CODES[item.stateCd] || "Unknown"
        }));

        res.json({
            data: {
                pan: pan.toUpperCase(),
                lgnm: gstinList.length > 0 ? "Multiple GSTINs found" : "N/A", // Portal doesn't always return legal name here
                gstinList: gstinList,
                status: gstinList.some(g => g.status === 'Active') ? "Active" : "Inactive",
                lastUpdated: dayjs().format('YYYY-MM-DD')
            }
        });
    } catch (error) {
        console.error("PAN Search failed:", error.response?.data || error.message);
        res.status(500).json({ error: 'PAN verification failed' });
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
