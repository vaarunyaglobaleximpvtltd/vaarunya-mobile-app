const { pool } = require('./db');
const fs = require('fs-extra');
const path = require('path');

const DATA_PATH = path.join(__dirname, 'data.json');

async function backfillUuiq() {
    let metadata;
    try {
        metadata = await fs.readJson(DATA_PATH);
    } catch (error) {
        console.error('Failed to read data.json');
        return;
    }

    const commodities = metadata.data.cmdt_data;
    // Build maps for quick lookup
    const nameToUuiq = {};
    const cleanNameToUuiq = {};

    commodities.forEach(c => {
        const lower = c.cmdt_name.toLowerCase();
        nameToUuiq[lower] = c.uuiq;
        cleanNameToUuiq[lower.replace(/\s+/g, '')] = c.uuiq;
    });

    function getUuiq(name) {
        if (!name) return null;
        const lower = name.toLowerCase().trim();
        // 1. Exact match
        if (nameToUuiq[lower]) return nameToUuiq[lower];
        // 2. Space removal match
        const noSpace = lower.replace(/\s+/g, '');
        if (cleanNameToUuiq[noSpace]) return cleanNameToUuiq[noSpace];
        // 3. Remove common suffixes
        const cleanSuffix = lower.split('(')[0].split('-')[0].trim();
        if (nameToUuiq[cleanSuffix]) return nameToUuiq[cleanSuffix];
        const cleanSuffixNoSpace = cleanSuffix.replace(/\s+/g, '');
        if (cleanNameToUuiq[cleanSuffixNoSpace]) return cleanNameToUuiq[cleanSuffixNoSpace];

        return null;
    }

    console.log(`Loaded ${commodities.length} commodity mappings from data.json`);

    const client = await pool.connect();
    try {
        const tables = [
            { name: 'agmark_sales_data', col: 'cmdt_name' },
            { name: 'enam_sales_data', col: 'commodity_name' },
            { name: 'market_prices_common', col: 'commodity_name' },
            { name: 'market_arrivals_common', col: 'commodity_name' }
        ];

        for (const table of tables) {
            console.log(`Processing table: ${table.name}...`);

            // Get unique commodity names from the table that don't have a UUIQ yet
            const res = await client.query(`SELECT DISTINCT ${table.col} FROM ${table.name} WHERE commodity_uuiq IS NULL`);
            const names = res.rows.map(r => r[table.col]);

            console.log(`  Found ${names.length} unique commodities with missing UUIQ.`);

            let updatedCount = 0;
            for (const name of names) {
                if (!name) continue;
                const uuiq = getUuiq(name);
                if (uuiq) {
                    await client.query(
                        `UPDATE ${table.name} SET commodity_uuiq = $1 WHERE ${table.col} = $2 AND commodity_uuiq IS NULL`,
                        [uuiq, name]
                    );
                    updatedCount++;
                }
            }
            console.log(`  Updated ${updatedCount} additional commodity types in ${table.name}.`);
        }

        console.log("Backfill complete.");
    } catch (err) {
        console.error("Backfill failed:", err);
    } finally {
        client.release();
    }
}

backfillUuiq().then(() => pool.end());
