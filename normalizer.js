const { pool } = require('./db');
const dayjs = require('dayjs');

function toTitleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/(?:^|\s)\w/g, (match) => match.toUpperCase());
}

async function normalizeAgmarkData(date, client) {
    console.log(`Normalizing Agmark data for ${date}...`);

    // 1. Fetch from Agmark Source
    const res = await client.query(`
        SELECT * FROM agmark_sales_data WHERE report_date = $1
    `, [date]);

    const records = res.rows;
    if (records.length === 0) {
        console.log("No Agmark data found for this date.");
        return;
    }

    console.log(`Processing ${records.length} Agmark records...`);

    for (const row of records) {
        // --- Populate Market Prices Common ---
        // Agmark unit is typically "Rs./Quintal"
        // We will store as is or standardized. Let's keep it as is ("Rs./Quintal") for now and source as "AGMARK"
        try {
            await client.query(`
                INSERT INTO market_prices_common (
                    state_name, district_name, market_name, commodity_name,
                    commodity_uuiq, min_price, max_price, model_price,
                    unit, source, report_date
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (report_date, source, state_name, market_name, commodity_name) 
                DO UPDATE SET
                    district_name = EXCLUDED.district_name,
                    commodity_uuiq = EXCLUDED.commodity_uuiq,
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price,
                    model_price = EXCLUDED.model_price,
                    unit = EXCLUDED.unit
            `, [
                toTitleCase(row.state_name),
                toTitleCase(row.district_name),
                toTitleCase(row.market_name),
                row.cmdt_name, // Commodity
                row.commodity_uuiq,
                row.min_price,
                row.max_price,
                row.model_price,
                row.unit_name_price, // Unit
                'AGMARK',
                date
            ]);
        } catch (err) {
            console.error("Error inserting Agmark price common:", err.message);
        }

        // --- Populate Market Arrivals Common ---
        // Agmark API response often doesn't contain arrival quantity clearly in the 'record' object we saw earlier.
        // It has `arrival_date`. 
        // If `agmark_sales_data` does not have arrival quantity, we skip it.
        // Looking at the schema we created: we did NOT add arrival_quantity/volume to agmark_sales_data 
        // because the sample JSON didn't show it explicitly as a volume number, only prices.
        // eNAM has it. Agmark data in previous conversation steps didn't show "Arrival Quantity".
        // So we SKIP Agmark for `market_arrivals_common` unless we find that field.
    }
}

async function normalizeEnamData(date, client) {
    console.log(`Normalizing eNAM data for ${date}...`);

    // 1. Fetch from eNAM Source
    const res = await client.query(`
        SELECT * FROM enam_sales_data WHERE report_date = $1
    `, [date]);

    const records = res.rows;
    if (records.length === 0) {
        console.log("No eNAM data found for this date.");
        return;
    }

    console.log(`Processing ${records.length} eNAM records...`);

    for (const row of records) {
        // --- Populate Market Prices Common ---
        try {
            await client.query(`
                INSERT INTO market_prices_common (
                    state_name, market_name, commodity_name,
                    commodity_uuiq, min_price, max_price, model_price,
                    unit, source, report_date
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (report_date, source, state_name, market_name, commodity_name) 
                DO UPDATE SET
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price,
                    model_price = EXCLUDED.model_price,
                    unit = EXCLUDED.unit,
                    commodity_uuiq = EXCLUDED.commodity_uuiq
            `, [
                toTitleCase(row.state_name),
                toTitleCase(row.apmc_name), // eNAM calls it 'apmc' or 'apmc_name'
                row.commodity_name,
                row.commodity_uuiq,
                row.min_price,
                row.max_price,
                row.modal_price, // eNAM uses 'modal_price'
                'Rs./Quintal',
                'eNAM',
                date
            ]);
        } catch (err) {
            console.error("Error inserting eNAM price common:", err.message);
        }

        // --- Populate Market Arrivals Common ---
        // Requirement: Maintain one unit (MT) for consistency.
        // eNAM `commodity_arrivals` is just a number. `unit_name_price` (Commodity_Uom) tells the unit (e.g., 'Quintal', 'Nos').
        // We need to convert to MT.

        let quantityMT = 0;
        const rawQty = parseFloat(row.commodity_arrivals) || 0;
        const uom = (row.unit_name_price || '').toLowerCase(); // e.g. "Quintal", "Nos", "Tonne"

        if (uom.includes('quintal')) {
            quantityMT = rawQty / 10; // 10 Quintal = 1 MT
        } else if (uom.includes('tonne') || uom.includes('mt')) {
            quantityMT = rawQty;
        } else if (uom.includes('kg') || uom.includes('kilogram')) {
            quantityMT = rawQty / 1000;
        } else {
            // "Nos" or others - hard to convert without weight per unit.
            // We store raw if we can't convert, or store 0?
            // User asked: "maintain one unit for consistency (MT)"
            // If we can't convert perfectly, we might leave it or map 1-to-1 if unknown.
            // For now, let's keep it as raw if unknown but mark unit as MT? No that's lying.
            // Let's store calculated MT. If 'Nos', maybe we skip or store 0.
            if (rawQty > 0) {
                // Fallback: If unknown unit, assume it's NOT MT effectively. 
                // Whatever logic, let's just try basic conversion.
            }
        }

        // Only insert if we have a valid quantity logic or just raw?
        // Let's allow non-MT if we handle column `arrival_unit` correctly.
        // But user said "maintain one unit". So we should convert where possible.
        // eNAM sample had "Nos". 

        try {
            await client.query(`
                INSERT INTO market_arrivals_common (
                    state_name, market_name, commodity_name,
                    commodity_uuiq, arrival_quantity, arrival_unit,
                    source, report_date
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (report_date, source, state_name, market_name, commodity_name) 
                DO UPDATE SET
                    arrival_quantity = EXCLUDED.arrival_quantity,
                    arrival_unit = EXCLUDED.arrival_unit,
                    commodity_uuiq = EXCLUDED.commodity_uuiq
            `, [
                toTitleCase(row.state_name),
                toTitleCase(row.apmc_name),
                row.commodity_name,
                row.commodity_uuiq,
                quantityMT > 0 ? quantityMT : rawQty, // If converted use MT, else raw
                quantityMT > 0 ? 'MT' : row.unit_name_price, // 'MT' or original
                'eNAM',
                date
            ]);
        } catch (err) {
            console.error("Error inserting eNAM arrival common:", err.message);
        }
    }
}

async function runNormalization(dateOverride) {
    const date = dateOverride || dayjs().format('YYYY-MM-DD');
    const client = await pool.connect();

    try {
        await normalizeAgmarkData(date, client);
        await normalizeEnamData(date, client);
        console.log(`Normalization completed for ${date}.`);
    } catch (err) {
        console.error("Normalization failed:", err);
    } finally {
        client.release();
    }
}

// Allow running directly
if (require.main === module) {
    const date = process.argv[2];
    runNormalization(date).then(() => pool.end());
}

module.exports = { runNormalization };
