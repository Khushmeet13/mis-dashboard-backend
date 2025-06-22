const express = require("express");
const { db } = require("./db");
const cors = require("cors");

const router = express.Router();

router.use(cors());
router.use(express.json());

router.get("/today-logs-status", async (req, res) => {
    try {
        const today = new Date().toISOString().split("T")[0];
        const date = new Date();
        const year = date.getFullYear().toString().slice(2);
        const month = date.toLocaleString('default', { month: 'short' }).toLowerCase();
        const tableName = `tbl_pm_promotions_logs_${month}${year}`;

        const query1 = `
            SELECT source FROM ${tableName} 
            WHERE DATE(date) = ? 
            GROUP BY source
        `;
        const [rows] = await db.execute(query1, ["2025-05-05"]);

        const query2 = `
            SELECT * FROM ${tableName}
            WHERE DATE(date) = ? 
        `;

        const [result] = await db.execute(query2, ["2025-05-05"]);

        const estimatedSizeBytes = result.length * 1024;

        res.json({
            uploadedToday: rows.length > 0,
            date: today,
            count: rows.length,
            sizeInBytes: estimatedSizeBytes,
            files: rows.map(row => row.source)
        });
    } catch (error) {
        console.error("Error fetching today's logs:", error);
        res.status(500).json({ error: "Internal server error" });
    }
});

router.get("/statistics", async (req, res) => {
    try {
        const client = req.query.client || "";
        const today = new Date().toISOString().split("T")[0];
        const date = new Date();
        const year = date.getFullYear().toString().slice(2);
        const month = date.toLocaleString("default", { month: "short" }).toLowerCase();


        let formattedSenderName = "clubM";
        if (client) {
            const [rows] = await db.query("SELECT mapping FROM product_mapping WHERE client = ?", [client]);
            if (rows.length > 0 && rows[0].mapping) {
                formattedSenderName = rows[0].mapping;
            } else {
                formattedSenderName = client.replace(/\s+/g, "_");
            }
        }

        const tableName = `tbl_sms_promotional_logs_${formattedSenderName}_${month}${year}`;

        const query = `
        SELECT 
            SUM(submitted_count) AS total_submitted,
            SUM(delivered_count) AS total_delivered,
            SUM(notsent_count) AS total_notsent,
            SUM(failed_count) AS total_failed
        FROM (
            SELECT 
                COUNT(Status) AS submitted_count,
                SUM(CASE WHEN Status IN ('Delivered', '000', '0') THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN Cause = 'NCPR Fail' THEN 1 ELSE 0 END) AS notsent_count,
                SUM(CASE WHEN Cause NOT IN ('Delivered', '000', '0', 'NCPR Fail') THEN 1 ELSE 0 END) AS failed_count
            FROM ${tableName}
            WHERE DATE(submit_time) = ?
        ) AS sub;

      `;

        const [stats] = await db.execute(query, ["2025-05-05"]);

        res.json(stats[0]);
    } catch (error) {
        console.error("Error fetching statistics:", error);
        res.status(500).json({ error: "Internal server error" });
    }
});

router.get("/performance-analytics", async (req, res) => {
    try {
        const client = req.query.client || "";
        const today = new Date().toISOString().split("T")[0];
        const date = new Date();
        const year = date.getFullYear().toString().slice(2);
        const month = date.toLocaleString("default", { month: "short" }).toLowerCase();


        let formattedSenderName = "clubM";
        if (client) {
            const [rows] = await db.query("SELECT mapping FROM product_mapping WHERE client = ?", [client]);
            if (rows.length > 0 && rows[0].mapping) {
                formattedSenderName = rows[0].mapping;
            } else {
                formattedSenderName = client.replace(/\s+/g, "_");
            }
        }

        const tableName = `tbl_sms_promotional_logs_${formattedSenderName}_${month}${year}`;
        const query1 = `
            SELECT count(status) as count, circle 
            FROM ${tableName} 
            WHERE status = 'delivered' AND DATE(submit_time) = ?
            GROUP BY circle;
      `;
        const [result1] = await db.execute(query1, ["2025-05-05"]);

        const [tables] = await db.query(`
            SHOW TABLES LIKE 'tbl_sms_promotional_logs_${formattedSenderName}_%'
        `);
        //console.log("Tables matching the pattern:", tables.map(t => Object.values(t)[0]));
        if (!tables.length) {
            return res.json({
                circleData: result1,
                monthlyTrend: 0,
            });
        }
        const unionParts = tables.map((table) => {
            return `
                SELECT 
                    MONTH(submit_time) AS month,
                    YEAR(submit_time) AS year,
                    SUM(CASE WHEN status IN ('Delivered', '000', '0') THEN 1 ELSE 0 END) AS delivered_count 
                FROM ??
                WHERE status = 'delivered'
                GROUP BY MONTH(submit_time), YEAR(submit_time)
            `;
        });
        const finalQuery = `
            SELECT month, year, SUM(delivered_count) AS total_delivered 
            FROM (
                ${unionParts.join(" UNION ALL ")}
            ) AS combined
            GROUP BY year, month
            ORDER BY year, month;
        `;
        const tableNames = tables.map(t => Object.values(t)[0]);
        const [monthlyData] = await db.query(finalQuery, tableNames);

        const query2 = `
            SELECT 
                DATE(submit_time) AS date,
                MONTH(submit_time) AS month,
                YEAR(submit_time) AS year,
                SUM(CASE WHEN status IN ('Delivered', '000', '0') THEN 1 ELSE 0 END) AS delivered_count 
            FROM ${tableName}
            WHERE status = 'delivered'
            GROUP BY DATE(submit_time), MONTH(submit_time), YEAR(submit_time)
        `;

        const [result2] = await db.execute(query2);

        res.json({
            circleData: result1,
            monthlyTrend: monthlyData || [],
            dailyTrend: result2 || [],
        });
    } catch (error) {
        console.error("Error fetching performance data:", error);
        res.status(500).json({ error: "Internal server error" });
    }
});

router.get("/critical-data", async (req, res) => {
    try {
        const client = req.query.client || "";
        const today = new Date().toISOString().split("T")[0];
        const yesterday = new Date(Date.now() - 86400000).toISOString().split("T")[0];
        const date = new Date();
        const year = date.getFullYear().toString().slice(2);
        const month = date.toLocaleString("default", { month: "short" }).toLowerCase();


        let formattedSenderName = "clubM";
        if (client) {
            const [rows] = await db.query("SELECT log_mapping FROM product_mapping WHERE client = ?", [client]);
            //const formattedName = rows.length > 0 ? rows[0].log_mapping : "undefined";
            if (rows.length > 0 && rows[0].log_mapping) {
                formattedSenderName = rows[0].log_mapping;
            } else {
                formattedSenderName = client.replace(/\s+/g, "_");
            }
        }

        const tableName = `${formattedSenderName}_rcs_sms_cmp_done_${month}${year}`;

        const query = `
            SELECT 
                substr(date,1,10) as date, 
                channel, base, product
            FROM ${tableName} 
            WHERE client = ? 
                AND substr(date, 1, 10) >= ? 
                AND substr(date, 1, 10) <= ?
            GROUP BY substr(date, 1, 10), channel, base, product 
            ORDER BY substr(date, 1, 10) DESC

        `;

        const [result] = await db.execute(query, ["Club Mahindra Dentsu", "2025-05-05", "2025-05-05"]);

        res.json(result);
    } catch (error) {
        console.error("Error fetching statistics:", error);
        res.status(500).json({ error: "Internal server error" });
    }
});

module.exports = router;
