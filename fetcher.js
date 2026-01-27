const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const dayjs = require('dayjs');

const DATA_PATH = path.join(__dirname, 'data.json');
const PRICES_PATH = path.join(__dirname, 'prices.json');

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchCommodityPrices(date, group, commodity) {
    const url = 'https://api.agmarknet.gov.in/v1/daily-price-arrival/report';
    const params = {
        from_date: date,
        to_date: date,
        data_type: 100004,
        group: group,
        commodity: commodity,
        state: '[100000]',
        district: '[100001]',
        market: '[100002]',
        grade: '[100003]',
        variety: '[100007]',
        page: 1,
        limit: 100 // increased limit to get more data in one go
    };

    try {
        const response = await axios.get(url, { params, validateStatus: () => true });
        if (response.data && response.data.status && response.data.data && response.data.data.records && response.data.data.records.length > 0) {
            return response.data.data.records[0].data;
        }
        return [];
    } catch (error) {
        console.error(`Error fetching for Group ${group}, Commodity ${commodity}: ${error.message}`);
        return [];
    }
}

async function runDailyFetch(dateOverride) {
    const date = dateOverride || dayjs().format('YYYY-MM-DD');
    console.log(`Starting fetch for ${date}...`);

    let metadata;
    try {
        metadata = await fs.readJson(DATA_PATH);
    } catch (error) {
        console.error('Failed to read data.json');
        return;
    }

    const groups = metadata.data.cmdt_group_data;
    const commodities = metadata.data.cmdt_data;

    let pricesData = {};
    if (await fs.pathExists(PRICES_PATH)) {
        try {
            pricesData = await fs.readJson(PRICES_PATH);
        } catch (e) {
            console.error('Error reading prices.json, starting fresh');
        }
    }

    if (!pricesData[date]) {
        pricesData[date] = {};
    }

    // Loop through groups and commodities
    for (const group of groups) {
        const groupItems = commodities.filter(c => c.cmdt_group_id === group.id);
        console.log(`Processing Group: ${group.cmdt_grp_name} (${groupItems.length} items)`);

        for (const item of groupItems) {
            // Check if already fetched today (optional resumes)
            if (pricesData[date][item.cmdt_id]) continue;

            console.log(`  Fetching ${item.cmdt_name}...`);
            const records = await fetchCommodityPrices(date, group.id, item.cmdt_id);

            if (records && records.length > 0) {
                pricesData[date][item.cmdt_id] = records;
                // Save after each successful fetch for real-time visibility
                await fs.writeJson(PRICES_PATH, pricesData, { spaces: 2 });
            }

            // Small delay to be polite to the API
            await sleep(200);
        }
    }

    console.log(`Finished fetch for ${date}`);
}

module.exports = { runDailyFetch };
