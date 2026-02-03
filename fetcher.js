const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const dayjs = require('dayjs');

const DATA_PATH = path.join(__dirname, 'data.json');


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

const { pool } = require('./db');

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
    const client = await pool.connect();

    try {
        // Clear existing data for this date to avoid duplicates on re-run (refresh)
        await client.query('DELETE FROM agmark_sales_data WHERE report_date = $1', [date]);

        // Loop through groups and commodities
        for (const group of groups) {
            const groupItems = commodities.filter(c => c.cmdt_group_id === group.id);
            console.log(`Processing Group: ${group.cmdt_grp_name} (${groupItems.length} items)`);

            for (const item of groupItems) {
                console.log(`  Fetching ${item.cmdt_name}...`);
                const records = await fetchCommodityPrices(date, group.id, item.cmdt_id);

                if (records && records.length > 0) {
                    // Batch insert or individual inserts?
                    // Given the volume, a loop with prepared statement is fine for now.
                    for (const record of records) {
                        try {
                            const minPrice = record.min_price ? parseFloat(record.min_price.replace(/,/g, '')) : 0;
                            const maxPrice = record.max_price ? parseFloat(record.max_price.replace(/,/g, '')) : 0;
                            const modelPrice = record.model_price ? parseFloat(record.model_price.replace(/,/g, '')) : 0;

                            await client.query(
                                `INSERT INTO agmark_sales_data (
                                    report_date, commodity_id, 
                                    cmdt_name, cmdt_grp_name, market_name, district_name, state_name,
                                    grade_name, variety_name, unit_name_price,
                                    min_price, max_price, model_price, arrival_date
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                                ON CONFLICT (report_date, commodity_id, market_name, variety_name, grade_name) DO NOTHING`,
                                [
                                    date, item.cmdt_id,
                                    record.cmdt_name, record.cmdt_grp_name, record.market_name, record.district_name, record.state_name,
                                    record.grade_name, record.variety_name, record.unit_name_price,
                                    minPrice, maxPrice, modelPrice, record.arrival_date
                                ]
                            );
                        } catch (err) {
                            console.error(`Error inserting record for ${item.cmdt_name}:`, err);
                        }
                    }
                    console.log(`    Saved ${records.length} records for ${item.cmdt_name}`);
                }

                // Small delay to be polite to the API
                await sleep(200);
            }
        }
    } catch (err) {
        console.error("Error during daily fetch:", err);
    } finally {
        client.release();
    }

    console.log(`Finished fetch for ${date}`);
}

module.exports = { runDailyFetch };
