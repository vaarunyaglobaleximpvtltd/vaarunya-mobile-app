const axios = require('axios');

async function testApi() {
    try {
        // Need to run server first. Assuming server running on 5050.
        // Or if not running, I should start it. BUT run_command usually runs in separate process/shell.
        // Let's assume user or I start server.
        // I will try to start server in background? 
        // Or better, I can require the app and use supertest? Or just mocking.
        // Simplest: use basic axios call if server is running.
        // I see server.js binds to 5050.

        // Wait, I can't guarantee server is running. 
        // I will try to start it for a few seconds?
        // Let's just create a script that user can run or I run which starts server briefly?

        // Actually, let's just inspect the code logic via 'node' directly importing handling function? 
        // No, `server.js` starts listening on load.

        console.log("Please ensure server is running on port 5050.");
        const res = await axios.get('http://localhost:5050/api/prices?date=2026-01-28');
        console.log("Status:", res.status);
        console.log("Keys:", Object.keys(res.data));
        const firstKey = Object.keys(res.data)[0];
        if (firstKey) {
            console.log("Sample Data for ID", firstKey, ":", res.data[firstKey][0]);
        } else {
            console.log("No data returned.");
        }
    } catch (err) {
        console.error("API Error:", err.message);
    }
}

testApi();
