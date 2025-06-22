const express = require('express');
const multer = require('multer');
const fs = require('fs');
const cors = require("cors");
const csvParser = require('csv-parser');
const mysql = require('mysql2/promise');
const { db } = require('./db');

const router = express.Router();
const uploadDir = 'template-uploads';
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir);
}

const upload = multer({ dest: uploadDir });

router.use(cors());
router.use(express.json());

router.post('/upload', upload.single('csvFile'), async (req, res) => {
    const filePath = req.file.path;

    try {
        const connection = await db.getConnection();


        await connection.execute(`
      CREATE TABLE IF NOT EXISTS AU_templates_bh (
        tempid VARCHAR(20),
        sender VARCHAR(10),
        count VARCHAR(225),
        text TEXT,
        type VARCHAR(50) DEFAULT 'new_temp',
        month VARCHAR(20)
      )
    `);


        await connection.execute(`
      CREATE TABLE IF NOT EXISTS cm_GUJ_templates (
        tempid VARCHAR(20),
        sender VARCHAR(10),
        count VARCHAR(225),
        text TEXT,
        type VARCHAR(50) DEFAULT 'new_temp',
        month VARCHAR(20)
      )
    `);

        await connection.execute(`
      CREATE TABLE IF NOT EXISTS cc_trans_templates (
        tempid VARCHAR(20),
        sender VARCHAR(10),
        count VARCHAR(225),
        text TEXT,
        type VARCHAR(50) DEFAULT 'new_temp',
        month VARCHAR(20)
      )
    `);

        await connection.execute('TRUNCATE TABLE AU_templates_bh');

        const rows = [];
        fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('error', (err) => {
                console.error('CSV parse error:', err);
                connection.release();
                res.status(500).json({ message: 'CSV parsing failed.' });
            })
            .on('data', (row) => rows.push(row))
            .on('end', async () => {
                const keys = Object.keys(rows[0]);
                const placeholders = keys.map(() => '?').join(',');
                const insertQuery = `
                    INSERT INTO AU_templates_bh (tempid, sender, count, text, type, month)
                    VALUES (?, ?, ?, ?, ?, ?)
                `;

                for (const row of rows) {
                    await connection.execute(insertQuery, [
                        row.TemplateId ?? '',
                        row.Sender ?? '',
                        row.Count ?? '',
                        row.Text ?? '',
                        'new_temp',
                        'init_month'
                    ]);
                }
                console.log('First row parsed:', rows[0]);


                await connection.execute(`UPDATE AU_templates_bh SET month = TRIM(REPLACE(REPLACE(REPLACE(month,'\t',''),'\n',''),'\r',''))`);
                await connection.execute(`UPDATE AU_templates_bh SET month = 'apr25'`);
                await connection.execute(`UPDATE AU_templates_bh SET type = 'HSCC'`);
                await connection.execute(`DELETE a FROM cm_GUJ_templates a INNER JOIN AU_templates_bh b ON a.tempid = b.tempid`);
                await connection.execute(`INSERT INTO cm_GUJ_templates SELECT * FROM AU_templates_bh`);
                await connection.execute(`DELETE a FROM cm_GUJ_templates a INNER JOIN cc_trans_templates b ON a.tempid = b.tempid`);
                await connection.execute(`UPDATE cm_GUJ_templates SET tempid = REPLACE(tempid, "'", '') WHERE tempid LIKE "'%"`);

                connection.release();
                res.json({ message: 'CSV uploaded and processed successfully.' });
            });
    } catch (err) {
        console.error(err);
        connection.release();
        res.status(500).json({ message: 'Database operation failed.' });
    }
});
 
module.exports = router;
