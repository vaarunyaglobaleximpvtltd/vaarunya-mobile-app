const axios = require('axios');
const qs = require('querystring');
const { pool } = require('./db');
const dayjs = require('dayjs');

async function fetchEnamData(date) {
    const url = 'https://enam.gov.in/web/Ajax_ctrl/trade_data_list';

    // Format date as YYYY-MM-DD for payload if needed, but API usually accepts YYYY-MM-DD
    const params = {
        language: 'en',
        stateName: '-- All --',
        apmcName: '-- Select APMCs --',
        commodityName: '-- Select Commodity --',
        fromDate: date,
        toDate: date
    };

    try {
        console.log(`Fetching eNAM data for ${date}...`);
        const response = await axios.post(url, qs.stringify(params), {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        });

        if (response.data && response.data.data && Array.isArray(response.data.data)) {
            const records = response.data.data;
            console.log(`Fetched ${records.length} records from eNAM.`);
            return records;
        } else {
            console.log("No data found or invalid response format.");
            return [];
        }
    } catch (error) {
        console.error("Error fetching eNAM data:", error.message);
        return [];
    }
}

const fs = require('fs-extra');
const path = require('path');
const DATA_PATH = path.join(__dirname, 'data.json');

async function runEnamFetch(dateOverride) {
    const date = dateOverride || dayjs().format('YYYY-MM-DD');
    const records = await fetchEnamData(date);

    if (records.length === 0) return;

    let metadata;
    try {
        metadata = await fs.readJson(DATA_PATH);
    } catch (error) {
        console.error('Failed to read data.json');
        return;
    }

    const client = await pool.connect();
    try {
        console.log("Saving raw eNAM data to database...");

        // Optional: Clear existing data for this date to support re-runs
        await client.query('DELETE FROM enam_sales_data WHERE report_date = $1', [date]);

        for (const record of records) {
            try {
                // Ensure numeric values
                const minPrice = parseFloat(record.min_price) || 0;
                const maxPrice = parseFloat(record.max_price) || 0;
                const modalPrice = parseFloat(record.modal_price) || 0;
                const arrivals = parseFloat(record.commodity_arrivals) || 0;
                const traded = parseFloat(record.commodity_traded) || 0;

                // RAW Dump: No normalization, no priority, no UUID mapping here.
                const rawUnit = record.Commodity_Uom || '';
                const commodityName = record.commodity;
                const recordUuiq = require('crypto').randomUUID();

                await client.query(`
                    INSERT INTO enam_sales_data (
                        enam_id, state_name, apmc_name, commodity_name,
                        min_price, modal_price, max_price,
                        commodity_arrivals, commodity_traded, created_at_api,
                        status, unit_name_price, report_date,
                        commodity_uuiq, record_uuiq
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (report_date, enam_id, apmc_name, commodity_name) 
                    DO UPDATE SET
                        unit_name_price = EXCLUDED.unit_name_price,
                        min_price = EXCLUDED.min_price,
                        max_price = EXCLUDED.max_price,
                        modal_price = EXCLUDED.modal_price,
                        commodity_uuiq = EXCLUDED.commodity_uuiq,
                        record_uuiq = EXCLUDED.record_uuiq
                `, [
                    record.id,
                    record.state,
                    record.apmc,
                    commodityName,
                    minPrice,
                    modalPrice,
                    maxPrice,
                    arrivals,
                    traded,
                    record.created_at, // API date field
                    record.status,
                    rawUnit,
                    date, // Our report date
                    null, // UUID will be filled by normalizer
                    recordUuiq
                ]);
            } catch (e) {
                console.error(`Error processing eNAM record ${record.id}:`, e.message);
            }
        }
        console.log(`Finished saving eNAM data for ${date}.`);
    } catch (err) {
        console.error("Database error during eNAM save:", err);
    } finally {
        client.release();
    }
}

// Allow running directly if main module
if (require.main === module) {
    const date = process.argv[2];
    runEnamFetch(date).then(() => pool.end());
}

module.exports = { runEnamFetch };
