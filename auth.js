const express = require('express');
const { OAuth2Client } = require('google-auth-library');
const jwt = require('jsonwebtoken');
const { pool } = require('./db');

const router = express.Router();
const client = new OAuth2Client();

const JWT_SECRET = process.env.JWT_SECRET || 'fallback-secret-key';
const VALID_CLIENT_IDS = [
    process.env.GOOGLE_CLIENT_ID_WEB,
    process.env.GOOGLE_CLIENT_ID_IOS,
    process.env.GOOGLE_CLIENT_ID_ANDROID,
].filter(Boolean);

// POST /api/auth/google
router.post('/google', async (req, res) => {
    const { idToken, role } = req.body;

    if (!idToken) {
        return res.status(400).json({ error: 'idToken is required' });
    }

    try {
        // Verify Google ID Token
        const ticket = await client.verifyIdToken({
            idToken,
            audience: VALID_CLIENT_IDS,
        });

        const payload = ticket.getPayload();
        const googleId = payload['sub'];
        const email = payload['email'];
        const name = payload['name'];
        const picture = payload['picture'];

        // Auto-detect Vaarunya role by email domain
        const userRole = email.endsWith('@vaarunyaglobalexim.com') ? 'vaarunya' : (role || 'importer_exporter');

        const dbClient = await pool.connect();
        try {
            // Check if user exists, otherwise create
            let result = await dbClient.query('SELECT * FROM users WHERE google_id = $1', [googleId]);
            let user = result.rows[0];

            if (!user) {
                result = await dbClient.query(
                    `INSERT INTO users (google_id, email, name, picture, role)
                     VALUES ($1, $2, $3, $4, $5) RETURNING *`,
                    [googleId, email, name, picture, userRole]
                );
                user = result.rows[0];
            } else {
                // Update name/picture/role if changed
                await dbClient.query(
                    `UPDATE users SET name = $1, picture = $2, role = $3 WHERE google_id = $4`,
                    [name, picture, userRole, googleId]
                );
                user.role = userRole;
            }

            // Generate application JWT
            const token = jwt.sign(
                { id: user.id, email: user.email, name: user.name, picture: user.picture, role: user.role },
                JWT_SECRET,
                { expiresIn: '30d' }
            );

            res.json({ token, user: { id: user.id, email: user.email, name: user.name, picture: user.picture, role: user.role } });
        } finally {
            dbClient.release();
        }
    } catch (error) {
        console.error('Google Auth Error:', error.message);
        res.status(401).json({ error: 'Invalid Google Identity Token' });
    }
});

// Middleware to protect routes
const authMiddleware = (req, res, next) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Unauthorized: Missing or invalid token format' });
    }

    const token = authHeader.split(' ')[1];
    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        req.user = decoded; // Attach user to request
        next();
    } catch (err) {
        console.error('JWT Verification Error:', err.message);
        return res.status(401).json({ error: 'Unauthorized: Invalid token' });
    }
};

module.exports = { router, authMiddleware };
