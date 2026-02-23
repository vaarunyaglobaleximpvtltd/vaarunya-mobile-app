const { pool } = require('./db');
const fs = require('fs-extra');
const path = require('path');

async function exportUniqueCommodities() {
    console.log('Starting commodity export...');

    try {
        // 1. Load commodity group names from data.json
        const dataPath = path.join(__dirname, 'data.json');
        const dataJson = await fs.readJson(dataPath);
        const commoditiesMeta = dataJson.data.cmdt_data || [];
        const groupsMeta = dataJson.data.cmdt_group_data || [];

        const groupMap = {};
        groupsMeta.forEach(g => {
            groupMap[g.id] = g.cmdt_grp_name;
        });

        const commodityToGroup = {};
        commoditiesMeta.forEach(c => {
            commodityToGroup[c.uuiq] = groupMap[c.cmdt_group_id] || 'Unknown';
        });

        // 2. Query unique commodities, UUIDs, and units from DB
        const query = `
            SELECT DISTINCT commodity_name, commodity_uuiq, unit
            FROM market_prices_common
            WHERE report_date >= '2026-01-01' AND report_date <= '2026-02-09'
            ORDER BY commodity_name, unit
        `;

        console.log('Fetching unique commodities from database...');
        const res = await pool.query(query);
        console.log(`Found ${res.rowCount} unique commodity-unit combinations.`);

        // 3. Prepare CSV content
        const headers = ['commodity', 'commodity_group', 'commodity_uuid', 'units'];
        const rows = res.rows.map(row => {
            const groupName = commodityToGroup[row.commodity_uuiq] || 'Unknown';
            return [
                `"${row.commodity_name.replace(/"/g, '""')}"`,
                `"${groupName.replace(/"/g, '""')}"`,
                `"${row.commodity_uuid || row.commodity_uuiq}"`, // Using uuiq as uuid
                `"${(row.unit || '').replace(/"/g, '""')}"`
            ].join(',');
        });

        const csvContent = [headers.join(','), ...rows].join('\n');

        // 4. Save to CSV file
        const outputPath = path.join(__dirname, 'unique_commodities_jan_feb.csv');
        await fs.writeFile(outputPath, csvContent);
        console.log(`Export completed! File saved to: ${outputPath}`);

    } catch (err) {
        console.error('Error during commodity export:', err);
    } finally {
        await pool.end();
    }
}

exportUniqueCommodities();
