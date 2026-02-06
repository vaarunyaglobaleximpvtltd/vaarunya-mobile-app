const fs = require('fs-extra');
const path = require('path');

const DATA_PATH = path.join(__dirname, 'data.json');

async function getOrGenerateUuiq(commodityName) {
    if (!commodityName) return null;

    let metadata;
    try {
        metadata = await fs.readJson(DATA_PATH);
    } catch (error) {
        console.error('Failed to read data.json in uuiq_helper');
        return null;
    }

    const commodities = metadata.data.cmdt_data;
    const lowerName = commodityName.toLowerCase().trim();
    const cleanName = lowerName.replace(/\s+/g, '');

    // 1. Exact Match
    let found = commodities.find(c => c.cmdt_name.toLowerCase() === lowerName);
    if (found) return found.uuiq;

    // 2. Clean Match (no spaces)
    found = commodities.find(c => c.cmdt_name.toLowerCase().replace(/\s+/g, '') === cleanName);
    if (found) return found.uuiq;

    // 3. Partial Match (e.g. Suffixes or Prefixes)
    const cleanSuffix = lowerName.split('(')[0].split('-')[0].trim();
    found = commodities.find(c => c.cmdt_name.toLowerCase().startsWith(cleanSuffix) || cleanSuffix.startsWith(c.cmdt_name.toLowerCase()));
    if (found) return found.uuiq;

    // 4. Generate New UUID
    console.log(`[uuiq_helper] Generating new UUID for: ${commodityName}`);

    // Find max numerical ID and VAAR number
    let maxId = 0;
    let maxVaarNum = 0;

    commodities.forEach(c => {
        if (c.cmdt_id > maxId) maxId = c.cmdt_id;
        if (c.uuiq && c.uuiq.startsWith('VAAR')) {
            const num = parseInt(c.uuiq.replace('VAAR', ''));
            if (!isNaN(num) && num > maxVaarNum) maxVaarNum = num;
        }
    });

    const newId = maxId + 1;
    const newUuiq = `VAAR${maxVaarNum + 1}`;

    // Create new entry
    const newEntry = {
        cmdt_id: newId,
        cmdt_name: commodityName, // Keep original casing for name
        cmdt_group_id: 99, // "Other" or default group
        uuiq: newUuiq
    };

    commodities.push(newEntry);

    try {
        await fs.writeJson(DATA_PATH, metadata, { spaces: 4 });
        console.log(`[uuiq_helper] Saved new commodity ${commodityName} as ${newUuiq}`);
        return newUuiq;
    } catch (err) {
        console.error('Failed to save data.json with new UUID:', err.message);
        return newUuiq; // Return it anyway so it can be used for the current run
    }
}

module.exports = { getOrGenerateUuiq };
