const { pool } = require('./db');
const dayjs = require('dayjs');
const { getOrGenerateUuiq } = require('./uuiq_helper');

function toTitleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/(?:^|\s)\w/g, (match) => match.toUpperCase());
}

function getStandardUnit(unit) {
    if (!unit) return 'Unit';
    const u = unit.toLowerCase();
    if (u.includes('qui') || u.includes('quintal')) return 'Rs./Quintal';
    if (u.includes('nos') || u.includes('number')) return 'Rs./Unit';
    if (u.includes('kg') || u.includes('kilogram')) return 'Rs./Kg';
    if (u.includes('bundle')) return 'Bundle';
    return unit; // Leave as is
}

function getUnitPriority(unit) {
    const std = getStandardUnit(unit);
    if (std.includes('Quintal')) return 1;
    if (std.includes('Unit')) return 2;
    if (std.includes('NOS') || std.toLowerCase().includes('nos')) return 3;
    if (std.includes('Bundle')) return 4;
    return 10;
}

async function runNormalization(dateOverride) {
    const date = dateOverride || dayjs().format('YYYY-MM-DD');
    const client = await pool.connect();

    try {
        console.log(`[Normalizer] Starting Incremental Processing for ${date}...`);

        // 1. Fetch only NON-NORMALIZED raw records for this date
        const agmarkRes = await client.query('SELECT * FROM agmark_sales_data WHERE report_date = $1 AND is_normalized = FALSE', [date]);
        const enamRes = await client.query('SELECT * FROM enam_sales_data WHERE report_date = $1 AND is_normalized = FALSE', [date]);

        const rawRecords = [
            ...agmarkRes.rows.map(r => ({ ...r, _src: 'AGMARK', _name: r.cmdt_name, _unit: r.unit_name_price, _recordUuiq: r.record_uuiq })),
            ...enamRes.rows.map(r => ({ ...r, _src: 'eNAM', _name: r.commodity_name, _unit: r.unit_name_price, _recordUuiq: r.record_uuiq }))
        ];

        if (rawRecords.length === 0) {
            console.log(`[Normalizer] No new raw data to process for ${date}.`);
            return;
        }

        console.log(`[Normalizer] Processing ${rawRecords.length} fresh records (${agmarkRes.rows.length} Agmark, ${enamRes.rows.length} eNAM)`);

        // 2. Resolve UUIDs
        for (const r of rawRecords) {
            if (!r.commodity_uuiq) {
                const uuiq = await getOrGenerateUuiq(r._name);
                r.commodity_uuiq = uuiq;

                // Back-fill raw table for traceability
                if (uuiq) {
                    if (r._src === 'AGMARK') {
                        await client.query('UPDATE agmark_sales_data SET commodity_uuiq = $1 WHERE id = $2', [uuiq, r.id]);
                    } else {
                        await client.query('UPDATE enam_sales_data SET commodity_uuiq = $1 WHERE enam_id = $2 AND report_date = $3', [uuiq, r.enam_id, r.report_date]);
                    }
                }
            }
        }

        // 3. Group and Priority Filter
        const grouped = {};
        rawRecords.forEach(r => {
            const uuiq = r.commodity_uuiq || 'UNMAPPED';
            if (!grouped[uuiq]) grouped[uuiq] = [];
            grouped[uuiq].push(r);
        });

        let processedCount = 0;
        const affectedUuiqs = new Set();

        for (const uuiq of Object.keys(grouped)) {
            if (uuiq === 'UNMAPPED') continue;
            affectedUuiqs.add(uuiq);

            const records = grouped[uuiq];
            const uniqueRawUnits = Array.from(new Set(records.map(r => r._unit)));
            uniqueRawUnits.sort((a, b) => getUnitPriority(a) - getUnitPriority(b));

            const winningRawUnit = uniqueRawUnits[0];
            const winningRecords = records.filter(r => r._unit === winningRawUnit);
            const stdUnit = getStandardUnit(winningRawUnit);

            for (const row of winningRecords) {
                if (row._src === 'AGMARK') {
                    await client.query(`
                        INSERT INTO market_prices_common (
                            state_name, district_name, market_name, commodity_name,
                            commodity_uuiq, min_price, max_price, model_price,
                            unit, source, report_date, record_uuiq
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (report_date, source, state_name, market_name, commodity_name) DO UPDATE SET
                            commodity_uuiq = EXCLUDED.commodity_uuiq,
                            min_price = EXCLUDED.min_price, max_price = EXCLUDED.max_price, model_price = EXCLUDED.model_price,
                            unit = EXCLUDED.unit, record_uuiq = EXCLUDED.record_uuiq
                    `, [toTitleCase(row.state_name), toTitleCase(row.district_name), toTitleCase(row.market_name), row._name, uuiq, row.min_price, row.max_price, row.model_price, stdUnit, 'AGMARK', date, row._recordUuiq]);
                } else {
                    await client.query(`
                        INSERT INTO market_prices_common (
                            state_name, market_name, commodity_name,
                            commodity_uuiq, min_price, max_price, model_price,
                            unit, source, report_date, record_uuiq
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (report_date, source, state_name, market_name, commodity_name) DO UPDATE SET
                            commodity_uuiq = EXCLUDED.commodity_uuiq,
                            min_price = EXCLUDED.min_price, max_price = EXCLUDED.max_price, model_price = EXCLUDED.model_price,
                            unit = EXCLUDED.unit, record_uuiq = EXCLUDED.record_uuiq
                    `, [toTitleCase(row.state_name), toTitleCase(row.apmc_name), row._name, uuiq, row.min_price, row.max_price, row.modal_price, stdUnit, 'eNAM', date, row._recordUuiq]);

                    await client.query(`
                        INSERT INTO market_arrivals_common (
                            state_name, market_name, commodity_name,
                            commodity_uuiq, arrival_quantity, arrival_unit,
                            source, report_date, record_uuiq
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (report_date, source, state_name, market_name, commodity_name) DO UPDATE SET
                            arrival_quantity = EXCLUDED.arrival_quantity, arrival_unit = EXCLUDED.arrival_unit,
                            commodity_uuiq = EXCLUDED.commodity_uuiq, record_uuiq = EXCLUDED.record_uuiq
                    `, [toTitleCase(row.state_name), toTitleCase(row.apmc_name), row._name, uuiq, parseFloat(row.commodity_arrivals) || 0, stdUnit, 'eNAM', date, row._recordUuiq]);
                }
                processedCount++;
            }
        }

        // 4. Mark raw as normalized
        await client.query('UPDATE agmark_sales_data SET is_normalized = TRUE WHERE report_date = $1 AND is_normalized = FALSE', [date]);
        await client.query('UPDATE enam_sales_data SET is_normalized = TRUE WHERE report_date = $1 AND is_normalized = FALSE', [date]);

        // 5. Calculate Health Stats (Yield)
        const totalRaw = rawRecords.length;
        const yieldPerc = totalRaw > 0 ? (processedCount / totalRaw) * 100 : 0;
        await client.query(`
            INSERT INTO data_normalization_stats (report_date, source, raw_count, processed_count, yield_percentage)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (report_date, source) DO UPDATE SET
                raw_count = EXCLUDED.raw_count, processed_count = EXCLUDED.processed_count, yield_percentage = EXCLUDED.yield_percentage
        `, [date, 'GLOBAL', totalRaw, processedCount, yieldPerc]);

        // 6. Pre-Aggregate Trends (Daily)
        for (const uuiq of affectedUuiqs) {
            const trendRes = await client.query(`
                SELECT AVG(model_price) as avg_price, unit
                FROM market_prices_common
                WHERE report_date = $1 AND commodity_uuiq = $2
                GROUP BY unit
            `, [date, uuiq]);

            if (trendRes.rows.length > 0) {
                const trend = trendRes.rows[0];
                await client.query(`
                    INSERT INTO market_trends_summary (commodity_uuiq, report_date, avg_model_price, unit, period_type)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (commodity_uuiq, report_date, period_type) DO UPDATE SET
                        avg_model_price = EXCLUDED.avg_model_price, unit = EXCLUDED.unit
                `, [uuiq, date, trend.avg_price, trend.unit, 'daily']);
            }
        }

        console.log(`[Normalizer] Completed for ${date}. Processed: ${processedCount}/${totalRaw} (${yieldPerc.toFixed(1)}% yield)`);
    } catch (err) {
        console.error("[Normalizer] Failed:", err);
    } finally {
        client.release();
    }
}

if (require.main === module) {
    const date = process.argv[2];
    runNormalization(date).then(() => pool.end());
}

module.exports = { runNormalization };
