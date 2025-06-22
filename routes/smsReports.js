const express = require("express");
const multer = require("multer");
const mysql = require("mysql2/promise");
const path = require("path");
const fs = require("fs");
const cors = require("cors");
const csv = require("csv-parser");
const { db, product_db, server_db, product_db_name, db_name, server_db_name } = require("./db");
const { console } = require("inspector");

const router = express.Router();

const uploadDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir);
}

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, "uploads/");
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + "-" + file.originalname);
    },
});

const upload = multer({ storage: storage });

function logToFile(message) {
    const currentDate = new Date().toISOString().split("T")[0];
    const logFileName = `logs_${currentDate}.txt`;
    const logFilePath = path.join(__dirname, logFileName);

    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}\n`;

    fs.appendFileSync(logFilePath, logMessage, "utf8");
}

router.use(cors());
router.use(express.json());

router.get("/api/product_sms", async (req, res) => {
    try {
        const [results] = await db.query("SELECT * FROM product_mapping where channel = 'sms'");
        console.log("Products Fetched Successfully");
        const clientNames = results.map(item => item.client).join(", ");
        console.log("Clients Name:", clientNames);
        logToFile(`Clients Name: ${clientNames}`);
        res.json(results);
    } catch (err) {
        console.error("Error fetching products:", err);
        res.status(500).json({ error: err.message });
    }
});

router.get("/api/product_rcs", async (req, res) => {
    try {
        const [results] = await db.query("SELECT * FROM product_mapping where channel = 'rcs'");
        const clientNames = results.map(item => item.client).join(", ");
        console.log("Clients Name:", clientNames);
        logToFile(`Clients Name: ${clientNames}`);
        res.json(results);
    } catch (err) {
        console.error("Error fetching products:", err);
        res.status(500).json({ error: err.message });
    }
});

router.get("/api/sender_sms", async (req, res) => {
    try {
        const [results] = await db.query("SELECT DISTINCT(sender) FROM product_mapping where channel = 'sms'");
        const sender = results.map(item => item.client).join(", ");
        console.log("sender Name:", sender);
        logToFile(`sender Name: ${sender}`);
        res.json(results);
    } catch (err) {
        console.error("Error fetching sender:", err);
        res.status(500).json({ error: err.message });
    }
});

router.get("/api/clients", async (req, res) => {
    try {
        const { fromDate, toDate } = req.query;

        if (!fromDate || !toDate) {
            return res.status(400).json({ error: "Both fromDate and toDate are required!" });
        }

        const fromDateObj = new Date(fromDate);
        const toDateObj = new Date(toDate);

        const fromMonth = fromDateObj.toLocaleString('en-US', { month: 'short' }).toLowerCase();
        const fromYear = fromDateObj.getFullYear().toString().slice(-2);

        const toMonth = toDateObj.toLocaleString('en-US', { month: 'short' }).toLowerCase();
        const toYear = toDateObj.getFullYear().toString().slice(-2);

        let tables = [`tbl_pm_promotions_logs_${fromMonth}${fromYear}`];
        if (fromMonth !== toMonth || fromYear !== toYear) {
            tables.push(`tbl_pm_promotions_logs_${toMonth}${toYear}`);
        }

        let queryParts = [];
        const queryParams = [];

        tables.forEach(table => {
            queryParts.push(`
                SELECT DISTINCT client
                FROM \`${table}\`
                WHERE DATE(date) BETWEEN ? AND ?
            `);
            queryParams.push(fromDate, toDate);
        });


        if (tables.length === 0) {
            return res.status(400).json({ error: "No valid tables found for the given date range." });
        }
        logToFile(`Client tables: ${tables}`);

        const query = queryParts.join(" UNION ");
        logToFile(`Clients Fetch query: ${query}`);

        const [results] = await db.query(query, queryParams);
        console.log("Clients Fetched Successfully:", results);
        logToFile(`Clients Fetched Successfully: ${JSON.stringify(results)}`);
        res.json(results);
    } catch (err) {
        console.error("Error fetching clients:", err);
        res.status(500).json({ error: err.message });
    }
});


router.get("/api/client-data", async (req, res) => {
    try {
        const { fromDate, toDate, client, limit = 10, cursor, direction } = req.query;

        if (!client) {
            return res.status(400).json({ error: "Client is required!" });
        }
        if (!fromDate || !toDate) {
            return res.status(400).json({ error: "Both fromDate and toDate are required!" });
        }

        const fromDateObj = new Date(fromDate);
        const toDateObj = new Date(toDate);

        const fromMonth = fromDateObj.toLocaleString('en-US', { month: 'short' }).toLowerCase();
        const fromYear = fromDateObj.getFullYear().toString().slice(-2);

        const toMonth = toDateObj.toLocaleString('en-US', { month: 'short' }).toLowerCase();
        const toYear = toDateObj.getFullYear().toString().slice(-2);

        let tableNames = [`tbl_pm_promotions_logs_${fromMonth}${fromYear}`];

        if (fromMonth !== toMonth || fromYear !== toYear) {
            tableNames.push(`tbl_pm_promotions_logs_${toMonth}${toYear}`);
        }

        //const offset = (page - 1) * limit;

        let queryParts = [];
        let queryParams = [];

        tableNames.forEach(table => {
            queryParts.push(`
                 SELECT COUNT(*) AS distinctSourceCount FROM (
                    SELECT DISTINCT source 
                    FROM \`${table}\`
                    WHERE DATE(date) BETWEEN ? AND ? AND client LIKE ?
                 ) AS distinct_sources
            `);
            queryParams.push(fromDate, toDate, `%${client}%`);
        });

        const totalDistinctSourcesQuery = queryParts.join("UNION ALL");
        const [distinctSourceResult] = await db.query(totalDistinctSourcesQuery, queryParams);

        const distinctSourceCount = distinctSourceResult.reduce((sum, row) => sum + (row.distinctSourceCount || 0), 0);

        queryParts = [];
        queryParams = [];

        //let whereCondition = cursor ? `AND source > ?` : ``;
        let orderDirection = direction === 'prev' ? 'DESC' : 'ASC';
        const cursorCondition = cursor
            ? direction === 'prev'
                ? `AND source < ?`
                : `AND source > ?`
            : '';

        tableNames.forEach(table => {
            queryParts.push(`
                SELECT 
                    COUNT(mobileNo) AS mobile_count, 
                    vendor, 
                    product, 
                    SUBSTR(date, 1, 10) AS date, 
                    source, 
                    channel, 
                    client
                FROM \`${table}\`
                WHERE DATE(date) BETWEEN ? AND ?
                AND client LIKE ?
                 ${cursor ? cursorCondition : ''}
                GROUP BY vendor, product, SUBSTR(date, 1, 10), source, channel, client
                
            `);

            /*if (cursor) {
                queryParams.push(fromDate, toDate, `%${client}%`, cursor, parseInt(limit));
            } else {
                queryParams.push(fromDate, toDate, `%${client}%`, parseInt(limit));
            }*/
            queryParams.push(fromDate, toDate, `%${client}%`);
            if (cursor) queryParams.push(cursor);
        });

        const dataQuery = queryParts.join(" UNION ALL ") + ` ORDER BY source ${orderDirection} LIMIT ?`;
        queryParams.push(parseInt(limit));
        const [results] = await db.query(dataQuery, queryParams);

        //const nextCursor = results.length > 0 ? results[results.length - 1].source : null;
        //const previousCursor = results.length > 0 ? results[0].source : null;
        const sortedResults = direction === 'prev' ? results.reverse() : results;

        const nextCursor = sortedResults.length > 0 ? sortedResults[sortedResults.length - 1].source : null;
        const previousCursor = sortedResults.length > 0 ? sortedResults[0].source : null;

        let firstCursor = null;
        let lastCursor = null;

        if (results.length > 0) {

            firstCursor = results[0].source;
            lastCursor = results[results.length - 1].source;
        }
        logToFile(`view all uploads query: ${dataQuery}`);

        res.json({
            data: results,
            totalRecords: distinctSourceCount,
            totalPages: Math.ceil(distinctSourceCount / limit),
            currentPage: cursor ? (direction === 'prev' ? 'Previous' : 'Next') : 1,
            firstPageCursor: firstCursor,
            lastPageCursor: lastCursor,
            previousCursor,
            nextCursor,
            hasNext: sortedResults.length === parseInt(limit),
            hasPrevious: !!cursor,
        });

    } catch (err) {
        console.error("Error fetching client data:", err);
        res.status(500).json({ error: err.message });
    }
});

router.get("/api/promo_msg_client", async (req, res) => {
    const { client, month } = req.query;


    if (!client) {
        return res.status(400).json({ error: "Client name is required" });
    }

    let currentMonth;
    if (month) {
        const dateFromQuery = new Date(`${month}-01`);
        const monthStr = dateFromQuery.toLocaleString('default', { month: 'short' }).toLowerCase();
        const yearStr = dateFromQuery.getFullYear().toString().slice(-2);
        currentMonth = `${monthStr}${yearStr}`;
    } else {

        const now = new Date();
        const monthStr = now.toLocaleString('default', { month: 'short' }).toLowerCase();
        const yearStr = now.getFullYear().toString().slice(-2);
        currentMonth = `${monthStr}${yearStr}`;
    }

    try {
        const [rows] = await db.query("SELECT mapping FROM product_mapping WHERE client = ?", [client]);

        const formattedSenderName = rows.length > 0 ? rows[0].mapping : client.replace(/\s+/g, "_");

        const now = new Date();
        /*const month = now.toLocaleString('default', { month: 'short' }).toLowerCase();
        const year = now.getFullYear().toString().slice(-2);
        const currentMonth = `${month}${year}`;*/


        const tableName = `tbl_sms_promotional_logs_${formattedSenderName}_${currentMonth}`;

        let results;

        if (client.toLowerCase().includes("club mahindra dentsu") || client.toLowerCase().includes("dentsu")) {

            [results] = await db.query(
                `SELECT 
            substr(submit_time,1,10) as date, 
            SUBSTRING_INDEX(source, '_', 2) as source,
            text, 
            product 
         FROM ${tableName} 
         GROUP BY substr(submit_time,1,10), text, source, product
         ORDER BY substr(submit_time,1,10) DESC`
            );
        } else {

            [results] = await db.query(
                `SELECT substr(submit_time,1,10) as date, text, product FROM ${tableName} 
                 GROUP BY substr(submit_time,1,10), text, product
                 ORDER BY substr(submit_time,1,10) DESC`
            );


        }

        res.json(results);
        logToFile(`Successfully executed promo msgs query: ${results}`);
    } catch (err) {
        console.error("Error fetching client data:", err);
        res.status(500).json({ error: err.message });
        logToFile(`Error fetching client data: ${err}`);
    }
});



router.post("/api/database_data", async (req, res) => {
    const { sender, client, product, channel, sms_mapping, log_mapping } = req.body;

    const query = `
        INSERT INTO product_mapping (sender, client, product, channel, mapping, log_mapping)
        SELECT ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM product_mapping 
            WHERE sender = ? AND client = ? AND product = ? AND channel = ?
        );
    `;

    const values = [
        sender, client, product, channel, sms_mapping, log_mapping,
        sender, client, product, channel
    ];

    try {
        const [result] = await db.execute(query, values);
        if (result.affectedRows === 0) {
            return res.status(200).json({ message: "Mapping already exists." });
        } else {
            return res.status(201).json({ message: "Mapping added successfully." });
        }
    } catch (error) {
        console.error("Insert Error:", error);
        return res.status(500).json({ error: "Internal Server Error" });
    }
});

router.put("/api/database_data/update", async (req, res) => {
    const { id, sender, client, product, channel, sms_mapping, rcs_mapping, log_mapping } = req.body;

    if (!id || !sender || !client || !product || !channel || !sms_mapping || !rcs_mapping || !log_mapping) {
        return res.status(400).json({ message: "All fields including ID are required." });
    }

    try {
        const updateQuery = `
            UPDATE product_mapping 
            SET sender = ?, client = ?, product = ?, channel = ?, mapping = ?, log_mapping = ?
            WHERE id = ?
        `;

        await db.query(updateQuery, [
            sender, client, product, channel, sms_mapping, log_mapping, id
        ]);

        res.json({ message: "Record updated successfully." });
    } catch (error) {
        console.error("Error updating record:", error);
        res.status(500).json({
            message: "Failed to update record.",
            error: error.message
        });
    }
});




router.get("/api/database_client", async (req, res) => {
    try {
        const [results] = await db.query("SELECT DISTINCT(client) FROM product_mapping");
        const clientNames = results.map(item => item.client).join(", ");
        console.log("Clients Name:", clientNames);
        logToFile(`Clients Name: ${clientNames}`);
        res.json(results);
    } catch (err) {
        console.error("Error fetching products:", err);
        res.status(500).json({ error: err.message });
    }
});

router.get("/api/database_mapping_data", async (req, res) => {
    const client = req.query.client;
    try {
        const [results] = await db.query("SELECT * FROM product_mapping WHERE client = ?", [client]);
        res.json(results);
    } catch (err) {
        console.error("Error fetching products:", err);
        res.status(500).json({ error: err.message });
    }
});

router.get("/api/base_check", async (req, res) => {
    const { fromDate, toDate, client } = req.query;

    if (!fromDate || !toDate || !client) {
        return res.status(400).json({ error: "Missing parameters" });
    }


    const from = new Date(fromDate);
    const to = new Date(toDate);


    if (from.getMonth() !== to.getMonth() || from.getFullYear() !== to.getFullYear()) {
        return res.status(400).json({ error: "From and To dates must be in the same month and year" });
    }


    try {
        const [rows] = await db.query("SELECT log_mapping FROM product_mapping WHERE client = ?", [client]);
        const formattedName = rows.length > 0 ? rows[0].log_mapping : "undefined";


        if (formattedName === "undefined") {
            return res.status(400).json({ error: "No mapping found for client" });
        }


        const dateObj = new Date(fromDate);
        const year = String(dateObj.getFullYear()).slice(-2);
        const month = dateObj.toLocaleString('en-US', { month: 'short' }).toLowerCase();
        const tableName = `${formattedName}_rcs_sms_cmp_done_${month}${year}`;

        const [results] = await db.query(
            `SELECT 
                substr(date,1,10) as date, 
                channel, sender, source, base, product, client 
             FROM ${tableName} 
             WHERE client = ? 
                AND substr(date, 1, 10) >= ? 
                AND substr(date, 1, 10) <= ?
                GROUP BY substr(date, 1, 10), client, channel, sender, source, base, product 
                ORDER BY substr(date, 1, 10) ASC`,
            [client, fromDate, toDate]
        );

        const [dateCircleResults] = await db.query(
            `SELECT 
                SUBSTR(date, 1, 10) AS date,
                circle,
                COUNT(*) AS total,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count,
                COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
                COUNT(CASE WHEN status = 'NOTSENT' THEN 1 END) AS notsent_count
             FROM ${tableName}
             WHERE client = ?
               AND SUBSTR(date, 1, 10) >= ?
               AND SUBSTR(date, 1, 10) <= ?
             GROUP BY SUBSTR(date, 1, 10), circle
             ORDER BY date ASC, circle ASC`,
            [client, fromDate, toDate]
        );



        if (results.length === 0) {
            return res.status(404).json({ error: "No data found for the given client and date range" });
        }

        res.json({
            results,
            dateCircleResults

        });
    } catch (err) {
        console.error("Error while fetching data:", err);
        res.status(500).json({ error: `An error occurred while fetching data: ${err.message}` });
    }
});


const formatDate = (dateStr) => {
    if (!dateStr || dateStr.toLowerCase().includes("in process") || dateStr.trim() === "") {
        return null;
    }

    dateStr = dateStr.replace(/^["']|["']$/g, "").trim();
    const matchShort = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4}) (\d{1,2}):(\d{2})$/);

    if (matchShort) {
        let [, month, day, year, hours, minutes] = matchShort;
        month = month.padStart(2, "0");
        day = day.padStart(2, "0");
        hours = hours.padStart(2, "0");
        minutes = minutes.padStart(2, "0");

        return `${year}-${month}-${day} ${hours}:${minutes}:00`;
    }

    return dateStr;
};

const formatMobile = (mobile) => {
    if (!mobile) return null;
    if (mobile.includes("E+")) {
        mobile = Number(mobile).toFixed(0);
    }
    return mobile;
}

const createDummyTable = async (source) => {
    const dummyTableName = `dummy_${source}`;
    console.log("dummy table name:", dummyTableName);

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS  \`${dummyTableName}\` (
        transaction_id VARCHAR(20),
        message_id VARCHAR(20),
        mobileNo VARCHAR(12),
        sender VARCHAR(10),
        text TEXT,
        type VARCHAR(10),
        length varchar(5),
        cost varchar(2),
        status VARCHAR(15),
        cause varchar(50),
        channel_name VARCHAR(20),
        submit_time varchar(30),
        delivered_time varchar(30),
        ip VARCHAR(10),
        entity_id VARCHAR(20),
        template_id VARCHAR(20),
        source VARCHAR(50),
        client varchar(80),
        product varchar(80),
        base varchar(50),
        circle varchar(30),
        vendor varchar(15),
        channel varchar(10)
      )`;

    try {
        await db.query(createTableQuery);
        logToFile(`Dummy table create query: ${createTableQuery}`);
        //console.log("Dummy table created successfully.");
        return dummyTableName;
    } catch (err) {
        console.error("Error creating dummy table:", err);
        logToFile(`Error creating dummy table: ${err}`);
    }
};

const insertIntoDummyTable = async (data, dummyTableName) => {
    const insertQuery = `
      INSERT INTO  \`${dummyTableName}\` 
      (transaction_id, message_id, mobileNo, sender, text, type, length, cost, status, cause, channel_name, submit_time, delivered_time, ip, entity_id, template_id, source, client, product, base, circle, vendor, channel) 
      VALUES ?
    `;

    try {
        await db.query(insertQuery, [data]);
        logToFile(`Data inserted into dummy table successfully: ${insertQuery}`);
        console.log("Data inserted into dummy table successfully:", insertQuery);
    } catch (err) {
        console.error("Error inserting data into dummy table:", err);
        logToFile(`Error inserting data into dummy table: ${err}`);
    }
};


const createTable = async (senderName = " ", month, year) => {
    try {
        const [rows] = await db.query("SELECT mapping FROM product_mapping WHERE sender = ?", [senderName]);

        const monthNames = new Date(year, month - 1).toLocaleString('en-US', { month: 'short' }).toLowerCase();
        const shortYear = year.toString().slice(-2);

        //const formattedSenderName = productMapping[senderName.trim().toLowerCase()] || senderName.replace(/\s+/g, "_");
        const formattedSenderName = rows.length > 0 ? rows[0].mapping : senderName.replace(/\s+/g, "_");
        console.log(formattedSenderName);
        const tableName = `tbl_sms_promotional_logs_${formattedSenderName}_${monthNames}${shortYear}`;
        console.log("Logs table name:", tableName);

        const createTableQuery = `
      CREATE TABLE IF NOT EXISTS \`${tableName}\` (
        transaction_id VARCHAR(20),
        message_id VARCHAR(20),
        mobileNo VARCHAR(12),
        sender VARCHAR(10),
        text TEXT,
        type VARCHAR(10),
        length varchar(5),
        cost varchar(2),
        status VARCHAR(15),
        cause varchar(50),
        channel_name VARCHAR(20),
        submit_time varchar(30),
        delivered_time varchar(30),
        ip VARCHAR(10),
        entity_id VARCHAR(20),
        template_id VARCHAR(20),
        source VARCHAR(50),
        product varchar(80),
        circle varchar(30)
      )
    `;

        await db.query(createTableQuery);
        console.log("Log table name after creation", tableName);
        logToFile(`Log table name after creation: ${tableName}`);
        return tableName;
    } catch (error) {
        console.error("Error creating table:", error);
        logToFile(`Error creating table: ${error}`);
        throw error;
    }
};

const insertIntoLogTable = async (tableName, dummyTableName) => {
    const insertQuery = `
        INSERT INTO \`${tableName}\` (transaction_id, message_id, mobileNo, sender, text, 
        type, length, cost, status, cause, channel_name, submit_time, 
        delivered_time, ip, entity_id, template_id, source, product, circle)
        SELECT transaction_id, message_id, mobileNo, sender, text, type, length, cost, status, cause, channel_name, submit_time, delivered_time, ip, entity_id, template_id, source, product, circle 
        FROM \`${dummyTableName}\` dt
        WHERE NOT EXISTS (
            SELECT 1 FROM \`${tableName}\` ft 
            WHERE ft.source = dt.source 
            LIMIT 1
        )
     `;

    try {
        await db.query(insertQuery);
        console.log("Data inserted into log table successfully:", insertQuery);
        logToFile(`Log table insert query: ${insertQuery}`);
    } catch (err) {
        console.error("Error inserting data into log table:", err);
        logToFile(`Error inserting data into log table: ${err}`);
    }
};


const createDndTable = async (senderName = " ") => {
    const [rows] = await db.query("SELECT mapping FROM product_mapping WHERE sender = ?", [senderName]);

    //const dndFormattedSenderName = productMapping[senderName.trim().toLowerCase()] || senderName.replace(/\s+/g, "_");
    const dndFormattedSenderName = rows.length > 0 ? rows[0].mapping : senderName.replace(/\s+/g, "_");
    const dndTableName = `dnd_${dndFormattedSenderName}`;

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS \`${dndTableName}\` (
        mobileNo VARCHAR(12),
        status VARCHAR(50),
        date varchar(30)
    ) 
      `;

    await db.query(createTableQuery);
    console.log("DND table name after creation:", dndTableName);
    logToFile(`DND table name after creation: ${dndTableName}`);
    return dndTableName;

}

const insertIntoDndTable = async (tableName, dummyTableName) => {

    const insertQuery = `
        INSERT INTO \`${tableName}\` (mobileNo, status, date)
        SELECT mobileNo, cause, submit_time
        FROM \`${dummyTableName}\` 
        WHERE cause LIKE '%blocked in preferences with msisdn as pk.%' 
        OR cause LIKE '%reserved for preference%' OR cause LIKE '%NCPR Fail%'
     `;

    try {
        await db.query(insertQuery);
        console.log("Data inserted into dnd table successfully:", insertQuery);
        logToFile(`DND table insert query: ${insertQuery}`);
    } catch (err) {
        console.error("Error inserting data into dnd table:", err);
        logToFile(`Error inserting data into dnd table: ${err}`);
    }
};



const createProductTable = async (client, month, year) => {
    const [rows] = await db.query("SELECT log_mapping FROM product_mapping WHERE client = ?", [client]);

    const monthNames = new Date(year, month - 1).toLocaleString('en-US', { month: 'short' }).toLowerCase();
    const shortYear = year.toString().slice(-2);

    const txtClient = rows.length > 0 ? rows[0].log_mapping : client.replace(/\s+/g, "_");
    const productTableName = `${txtClient}_rcs_sms_cmp_done_${monthNames}${shortYear}`;
    console.log("Product table Name:", productTableName);

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS \`${productTableName}\` (
        mobileNo VARCHAR(12),
        status varchar(15),
        date VARCHAR(20),
        channel varchar(10),
        sender varchar(10),
        circle varchar(30),
        vendor varchar(15),
        source varchar(50),
        base varchar(50),
        product varchar(80),
        client varchar(80),
        cause varchar(60)  
    ) 
      `;

    try {
        await product_db.query(createTableQuery);
        //console.log("Product table created or already exists.");
        console.log("Product table Name after creation:", productTableName);
        logToFile(`Product table Name after creation: ${productTableName}`);
        return productTableName;
    } catch (err) {
        console.error("Error creating table:", err);
        logToFile(`Error creating product table: ${err}`);
    }

}

const insertIntoProductTable = async (tableName, dummyTableName) => {
    const insertQuery = `
        INSERT INTO \`${tableName}\` (mobileNo, date, channel , sender, circle, vendor, product, source, status, client, base, cause) 
        SELECT mobileNo, submit_time, channel , sender, circle, vendor, product, source, status, client, base, cause
        FROM \`${db_name}\`.\`${dummyTableName}\`  dt
        WHERE NOT EXISTS (
            SELECT 1 FROM \`${tableName}\` ft 
            WHERE ft.source = dt.source 
            LIMIT 1
        )
     `;

    try {
        await product_db.query(insertQuery);
        console.log("Data inserted into product table successfully", insertQuery);
        logToFile(`Data inserted into product table successfully: ${insertQuery}`);
    } catch (err) {
        console.error("Error inserting data into product table:", err);
        logToFile(`Error inserting data into product table: ${err}`);
    }
};


const createFinalTable = async (month, year) => {
    const monthNames = new Date(year, month - 1).toLocaleString('en-US', { month: 'short' }).toLowerCase();
    const shortYear = year.toString().slice(-2);
    const tableName = `tbl_pm_promotions_logs_${monthNames}${shortYear}`;

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS \`${tableName}\` (
        mobileNo VARCHAR(12),
        circle VARCHAR(30),
        vendor VARCHAR(15),
        status VARCHAR(15),
        product VARCHAR(80),
        date VARCHAR(50),
        source VARCHAR(50),
        channel VARCHAR(10),
        cause VARCHAR(60),
        sender VARCHAR(10),
        client VARCHAR(80)
      )
    `;

    const indexesToAdd = ['mobileNo', 'source', 'vendor', 'product', 'channel', 'client'];

    try {
        await db.query(createTableQuery);
        console.log("Final Table created:", tableName);
        logToFile(`Final Table created: ${tableName}`);


        const [existingIndexes] = await db.query(`SHOW INDEX FROM \`${tableName}\``);
        const existingIndexColumns = new Set(existingIndexes.map(row => row.Column_name));

        for (const column of indexesToAdd) {
            if (!existingIndexColumns.has(column)) {
                const indexQuery = `ALTER TABLE \`${tableName}\` ADD INDEX (${column})`;
                await db.query(indexQuery);
                console.log(`Index added on column: ${column}`);
                logToFile(`Index added on column: ${column}`);
            }
        }

        return tableName;

    } catch (err) {
        console.error("Error creating table or adding indexes:", err);
        logToFile(`Error creating table or adding indexes: ${err}`);
        return null;
    }
};


const productInsertFinal = async (tableName, dummyTableName) => {
    if (!tableName) {
        console.error("Error: Table name is undefined or null. Cannot insert data.");
        return;
    }

    const insertQuery = `
        INSERT INTO \`${tableName}\` (mobileNo, circle, vendor, status, product, date, source, channel, cause, sender, client)
        SELECT dt.mobileNo, dt.circle, dt.vendor, dt.status, dt.product, dt.delivered_time, dt.source, dt.channel, dt.cause, dt.sender, dt.client
        FROM \`${db_name}\`.\`${dummyTableName}\`  dt
        WHERE NOT EXISTS (
            SELECT 1 FROM \`${tableName}\` ft 
            WHERE ft.source = dt.source 
            LIMIT 1
        )
    `;

    console.log("Product to final table insert query:", insertQuery);
    logToFile(`Product to final table insert query: ${insertQuery}`);

    try {
        await db.query(insertQuery);
        console.log(`Product table Data successfully inserted into final table: ${tableName}`);
    } catch (err) {
        console.error(`Error inserting data into final table:`, err);
        logToFile(`Error inserting data into final table: ${err}`);
    }
};

function getUpdateQuery(tableName) {
    return ` 
        update \`${db_name}\`.\`${tableName}\` as base , \`${db_name}\`.region_all  as regin set  base.circle=regin.circle where substr(base.mobileNo,3,4)=regin.series
    `;
}


const requiredColumns = ["transaction_id", "message_id", "mobile", "sender_name", "text_message", "type", "length", "cost", "status", "cause",
    "channel_name", "submitted_time",
    "delivered_time", "ip", "entity_id", "template_id"];

async function processFile(file, client, product, vendor, channel, base, duplicateFiles, newFiles, invalidFiles, connection) {
    return new Promise((resolve, reject) => {
        const filePath = path.resolve(file.path);
        const fileName = file.originalname.endsWith('.csv')
            ? file.originalname.slice(0, -4)
            : file.originalname;
        let values = [];
        let isValidFile = true;
        let missingColumns = [];
        let month = " ";
        let year = " ";

        let dummyTableName = " ";
        let logTableName = " ";
        let senderName = " ";
        let dndTableName = " ";
        let finalTableName = "";
        let productTableName = "";
        let isDuplicate = false;

        fs.createReadStream(filePath)
            .pipe(csv({ separator: ',', quote: '"', mapHeaders: ({ header }) => header.trim().replace(/\s+/g, '_').replace(/"/g, '') }))

            .on("headers", (headers) => {
                const normalizedHeaders = headers.map(h => h.trim().toLowerCase().replace(/\s+/g, '_').replace(/"/g, '').replace('entityid', 'entity_id')
                    .replace('templateid', 'template_id'));
                const normalizedColumns = requiredColumns.map(c => c.trim().toLowerCase());

                console.log("Normalized CSV Headers:", normalizedHeaders);
                console.log("Normalized DB Columns:", normalizedColumns);

                missingColumns = normalizedColumns.filter((col) => !normalizedHeaders.includes(col));

                if (missingColumns.length > 0) {
                    isValidFile = false;
                }
            })

            .on("data", async (row) => {
                if (!isValidFile) return;
                //console.log("Row Data:", row);
                senderName = row.Sender_Name;
                const submitted_date = row.Submitted_Time;
                const dateObj = new Date(submitted_date);
                month = String(dateObj.getMonth() + 1).padStart(2, "0");
                year = dateObj.getFullYear();

                values.push([
                    row.Transaction_ID?.trim().replace(/^'|'|\s+$/g, "") || null,
                    row.Message_ID?.trim(),
                    formatMobile(row.Mobile),
                    row.Sender_Name,
                    row.Text_Message,
                    row.Type,
                    row.Length ? parseInt(row.Length, 10) : null,
                    row.Cost ? parseFloat(row.Cost) : null,
                    row.Status?.trim(),
                    row.Cause,
                    row.Channel_Name,
                    formatDate(row.Submitted_Time),
                    formatDate(row.Delivered_Time),
                    row.IP?.trim(),
                    row.EntityId?.trim().replace(/`/g, '') || null,
                    row.TemplateId?.trim().replace(/`/g, '') || null,
                    fileName,
                    client,
                    product,
                    base,
                    "Unknown",
                    vendor,
                    channel
                ]);
            })

            .on("end", async () => {
                if (!isValidFile) {
                    invalidFiles.push(fileName);
                    console.log("Invalid Files:", invalidFiles);
                    logToFile(`Invalid Files: ${invalidFiles}`);
                    fs.unlinkSync(file.path);
                    resolve({ isDuplicate: false, dummyTableName: null, invalidFiles });
                    return;
                }

                try {
                    logTableName = await createTable(senderName, month, year);
                    dndTableName = await createDndTable(senderName);
                    productTableName = await createProductTable(client, month, year);
                    finalTableName = await createFinalTable(month, year);

                    /*const [rows] = await db.query(
                        `SELECT COUNT(*) AS count FROM \`${logTableName}\` WHERE source = ?`,
                        [fileName]
                    );*/
                    const query = `SELECT COUNT(*) AS count FROM \`${logTableName}\` WHERE source = ?`;
                    const [rows] = await db.query(query, [fileName]);
                    logToFile(`Executing Duplicate Query: ${query} | Source: ${fileName}`);
                    logToFile(`duplicate data: ${JSON.stringify(rows, null, 2)}`);

                    if (rows[0].count > 0) {
                        duplicateFiles.push(fileName);
                        console.log("duplicate files: ", duplicateFiles);
                        logToFile(`duplicate files: ${duplicateFiles}`);
                        isDuplicate = true
                        if (fs.existsSync(file.path)) {
                            fs.unlinkSync(file.path);

                        }
                        resolve({ isDuplicate: true, dummyTableName: null, invalidFiles });
                        return;
                    } else {
                        newFiles.push(file);
                        dummyTableName = await createDummyTable(fileName);
                        await insertIntoDummyTable(values, dummyTableName);
                        const updateQuery = getUpdateQuery(dummyTableName);
                        logToFile(`Update Query: ${updateQuery}`);
                        await db.query(updateQuery);

                        await insertIntoLogTable(logTableName, dummyTableName);
                        await insertIntoDndTable(dndTableName, dummyTableName);


                        const fileNames = [...new Set(newFiles.map(f => f.originalname))];

                        for (const file of fileNames) {
                            if (!finalTableName) {
                                console.error("Error: Final table name is undefined. Skipping insertion for", file);
                            } else {
                                await insertIntoProductTable(productTableName, dummyTableName);
                                await productInsertFinal(finalTableName, dummyTableName);
                            }
                        }

                        if (dummyTableName) {
                            try {
                                await db.query(`DROP TABLE IF EXISTS \`${dummyTableName}\``);
                                console.log(`Table ${dummyTableName} dropped successfully.`);
                                logToFile(`Table dropped successfully: ${dummyTableName}`);
                            } catch (dropError) {
                                console.error(`Error dropping table ${dummyTableName}:`, dropError);
                                logToFile(`Error dropping table: ${dropError}`);
                            }
                        }
                        resolve({ isDuplicate: false, dummyTableName, invalidFiles });
                    }

                } catch (err) {
                    console.error("Error inserting data into MySQL:", err);
                    logToFile(`Error inserting data into MySQL: ${err}`);
                    await connection.rollback();
                    reject(err);
                } finally {
                    if (fs.existsSync(file.path)) {
                        fs.unlinkSync(file.path);
                    }
                }
                resolve();
            })

            .on("error", (err) => {
                console.error("Error processing CSV:", err);
                logToFile(`Error processing CSV: ${err}`);
                reject(err);
            });
    });
}

router.post("/upload/csv", upload.array("files", 10), async (req, res) => {
    const connection = await db.getConnection();
    let dummyTableNames = [];

    try {
        await connection.beginTransaction();

        const { client, product, vendor, channel, base } = req.body;
        const files = req.files;
        if (!files || files.length === 0) {
            return res.status(400).json({ error: "No files uploaded" });
        }
        console.log("Files uploaded:", req.files);

        let duplicateFiles = [];
        let newFiles = [];
        let invalidFiles = [];
        let isDuplicateFiles = false;

        for (const file of files) {
            try {
                const { isDuplicate, dummyTableName, invalidFiles: returnedInvalidFiles } = await processFile(file, client, product, vendor, channel, base, duplicateFiles, newFiles, invalidFiles, connection);
                if (dummyTableName) {
                    dummyTableNames.push(dummyTableName);
                    console.log("dummy table names:", dummyTableNames);
                }
                if (isDuplicate) {
                    isDuplicateFiles = true;
                }
                if (returnedInvalidFiles && returnedInvalidFiles.length > 0) {
                    invalidFiles.push(...returnedInvalidFiles);
                }

            } catch (err) {
                console.error("Error processing file:", file.originalname, err);
                logToFile(`Error processing file ${file.originalname}: ${err}`);
                await connection.rollback();
                return res.status(500).json({ error: "Internal Server Error", details: err.message });
            }
        }

        console.log("isDuplicateFiles:", isDuplicateFiles);
        logToFile(`isDuplicateFiles: ${isDuplicateFiles}`);

        if (isDuplicateFiles) {
            for (const tableName of dummyTableNames) {
                try {
                    await db.query(`DROP TABLE IF EXISTS \`${tableName}\``);
                    console.log(`Table ${tableName} dropped successfully.`);
                    logToFile(`Table dropped successfully: ${tableName}`);
                } catch (dropError) {
                    console.error(`Error dropping table ${tableName}:`, dropError);
                    logToFile(`Error dropping table: ${dropError}`);
                }
            }
            await connection.rollback();
            return res.status(400).json({ error: "Duplicate files detected", duplicateFiles });
        }

        if (invalidFiles.length > 0) {
            return res.status(400).json({ error: "Invalid files detected", invalidFiles });
        }



        await connection.commit();
        res.json({ message: "All files uploaded and inserted successfully!", uploadedFiles: newFiles.map(f => f.originalname), invalidFiles });
    } catch (error) {
        console.error("Upload Error:", error);
        logToFile(`Upload Error: ${error}`);
        await connection.rollback();
        res.status(500).json({ error: "Internal Server Error", details: error.message });
    } finally {
        connection.release();
    }
});

router.post("/upload/txt", upload.single("file"), async (req, res) => {
    const connection = await product_db.getConnection();

    try {
        await connection.beginTransaction();

        const file = req.file;
        const { product, channel, date, vendor, base, client, sender } = req.body;

        if (!file) {
            return res.status(400).json({ error: "No file uploaded" });
        }

        const parsedDate = new Date(date);
        if (isNaN(parsedDate.getTime())) {
            return res.status(400).json({ error: "Invalid date format" });
        }

        const month = parsedDate.getMonth() + 1;
        const year = parsedDate.getFullYear();

        const filePath = path.resolve(file.path);
        let values = [];
        let finalTableName = " ";
        let productTxtTableName = "";
        let dummyTableName = " ";
        let duplicateFiles = [];
        let newFiles = [];
        let isDuplicateFiles = false;

        const source = path.basename(file.originalname, path.extname(file.originalname));

        const fileData = fs.readFileSync(filePath, "utf8");
        const lines = fileData.split(/\r?\n|,/);

        for (let line of lines) {
            const mobile = line.trim();

            if (mobile) {
                let mobileNo = mobile;

                if (/^91[6-9]\d{9}$/.test(mobile)) {
                    mobileNo = mobile;
                } else if (/^[6-9]\d{9}$/.test(mobile)) {
                    mobileNo = `91${mobile}`;
                } else {
                    console.log("Invalid Number:", mobile);
                    logToFile(`Invalid Number: ${mobile}`);
                    continue;
                }
                values.push(["NA", "NA", mobileNo, sender, "NA", "NA", "NA", "NA", "e", "e", "NA", date, date, "NA", "NA", "NA", source, client, product, base, "Unknown", vendor, channel]);
            }
        }

        if (values.length > 0) {
            productTxtTableName = await createProductTable(client, month, year);
            finalTableName = await createFinalTable(month, year);

            const query = `SELECT COUNT(*) AS count FROM \`${product_db_name}\`.\`${productTxtTableName}\` WHERE source = ?`;
            const [rows] = await db.query(query, [source]);

            /*const [rows] = await db.query(
                `SELECT COUNT(*) AS count FROM \`${product_db_name}\`.\`${productTxtTableName}\` WHERE source = ?`,
                [source]
            );*/
            logToFile(`Executing Duplicate Query: ${query} | Source: ${source}`);
            logToFile(`duplicate data: ${JSON.stringify(rows, null, 2)}`);

            if (rows[0].count > 0) {
                duplicateFiles.push(source);
                isDuplicateFiles = true;
                console.log("Duplicate file detected:", source);
                logToFile(`Duplicate file detected: ${source}`);
                fs.unlinkSync(file.path);
            } else {
                newFiles.push(file);
                dummyTableName = await createDummyTable(source);

                await insertIntoDummyTable(values, dummyTableName);
                const updateQuery = getUpdateQuery(dummyTableName);
                await db.query(updateQuery);

                await insertIntoProductTable(productTxtTableName, dummyTableName);
                await productInsertFinal(finalTableName, dummyTableName);

                fs.unlinkSync(filePath);
            }
        }

        if (isDuplicateFiles) {
            await connection.rollback();
            return res.status(400).json({ error: "Duplicate files detected", duplicateFiles });
        }

        if (dummyTableName) {
            try {
                await db.query(`DROP TABLE IF EXISTS \`${dummyTableName}\``);
                console.log(`Dummy table ${dummyTableName} dropped successfully.`);
                logToFile(`Dummy table dropped successfully: ${dummyTableName}`);
            } catch (dropError) {
                console.error(`Error dropping dummy table ${dummyTableName}:`, dropError);
                logToFile(`Error dropping dummy table : ${dropError}`);
            }
        }



        await connection.commit();
        res.status(200).json({ message: "File uploaded successfully", inserted: values.length, uploadedFiles: newFiles.map(f => f.originalname) });
    } catch (error) {
        console.error("Error:", error);
        await connection.rollback();
        res.status(500).json({ error: "Internal Server Error" });
    } finally {
        connection.release();
    }
});

/*async function updateStatusAndCause(dummyTableName, sourceTable, transactionId) {
    try {

        const [checkRows] = await db.query(
            `SELECT * FROM \`${dummyTableName}\` WHERE transaction_id = ?`,
            [transactionId]
        );
        logToFile(`Check if transaction_id exists: ${transactionId}`);
        
      
        const [globalCodeRows] = await server_db.query(
            `SELECT globalErrorCode FROM \`${sourceTable}\` WHERE uuId = ?`,
            [transactionId]
        );
        logToFile(`globalErrorCode: ${JSON.stringify(globalCodeRows)}`);


        if (!globalCodeRows || globalCodeRows.length === 0) {
            throw new Error("globalErrorCode not found for the given uuId.");
        }

        const globalErrorCode = globalCodeRows[0].globalErrorCode;

      
        const [mappingRows] = await db.query(
            `SELECT status, cause FROM sgc_msg_log.error_code_mapping WHERE code = ?`,
            [globalErrorCode]
        );
        logToFile(`mappingRows: ${JSON.stringify(mappingRows)}`);


        if (!mappingRows || mappingRows.length === 0) {
            throw new Error("Mapping not found for globalErrorCode: " + globalErrorCode);
        }

        const { status, cause } = mappingRows[0];

     
        const result = await db.query(
            `UPDATE \`${dummyTableName}\` SET status = ?, cause = ? WHERE transaction_id = ?`,
            [status, cause, transactionId]
        );
        
       
        logToFile(`Update Result: ${JSON.stringify(result)}`);
        if (result.affectedRows === 0) {
            console.log(`No rows updated for transaction_id: ${transactionId}`);
            logToFile(`No rows updated for transaction_id: ${transactionId}`);
        } else {
            console.log(`Updated ${result.affectedRows} row(s) in ${dummyTableName}`);
            logToFile(`Updated ${result.affectedRows} row(s) in ${dummyTableName}`);
        }

        const [updatedRows] = await db.query(
            `SELECT * FROM \`${dummyTableName}\` WHERE transaction_id = ?`,
            [transactionId]
        );

       
        logToFile(`Rows from dummy table after update for transaction_id ${transactionId}: ${JSON.stringify(updatedRows)}`);

        console.log(`Status and Cause updated in ${dummyTableName} for uuId: ${transactionId}`);
    } catch (err) {
        console.error("Error in updateStatusAndCause:", err);
        logToFile(`Error updating: ${err}`);
        throw err; 
    }
}*/

/*async function updateStatusAndCause(dummyTableName, sourceTable, transactionId) {
    try {
        // Get rows from dummy table where transaction_id matches
        const [checkRows] = await db.query(
            `SELECT * FROM \`${dummyTableName}\` WHERE transaction_id = ?`,
            [transactionId]
        );
        logToFile(`Check if transaction_id exists: ${JSON.stringify(checkRows)}`);

        if (!checkRows || checkRows.length === 0) {
            throw new Error("No rows found for the given transaction_id.");
        }

        // Loop through each row in dummy table
        for (let row of checkRows) {
            const rowGlobalErrorCode = row.status;

            // Get mapping of status and cause from error_code_mapping table
            const [mappingRows] = await db.query(
                `SELECT status, cause FROM sgc_msg_log.error_code_mapping WHERE code = ?`,
                [rowGlobalErrorCode]
            );

            logToFile(`Mapping for code ${rowGlobalErrorCode}: ${JSON.stringify(mappingRows)}`);

            if (!mappingRows || mappingRows.length === 0) {
                console.warn(`Mapping not found for globalErrorCode: ${rowGlobalErrorCode}`);
                logToFile(`Mapping not found for globalErrorCode: ${rowGlobalErrorCode}`);
                continue;
            }

            const { status, cause } = mappingRows[0];

            // Update the dummy table row with new status and cause
            const [result] = await db.query(
                `UPDATE \`${dummyTableName}\` 
                 SET status = ?, cause = ? 
                 WHERE transaction_id = ? AND status = ?`,
                [status, cause, transactionId, rowGlobalErrorCode]
            );

            logToFile(`Update Result for globalErrorCode ${rowGlobalErrorCode}: ${JSON.stringify(result)}`);

            if (result.affectedRows === 0) {
                console.log(`No rows updated for transaction_id: ${transactionId} with globalErrorCode: ${rowGlobalErrorCode}`);
                logToFile(`No rows updated for transaction_id: ${transactionId} with globalErrorCode: ${rowGlobalErrorCode}`);
            } else {
                console.log(`Updated ${result.affectedRows} row(s) for globalErrorCode ${rowGlobalErrorCode}`);
                logToFile(`Updated ${result.affectedRows} row(s) for globalErrorCode ${rowGlobalErrorCode}`);
            }
        }

        console.log(`Status and Cause updated for transaction_id: ${transactionId}`);
    } catch (err) {
        console.error("Error in updateStatusAndCause:", err);
        logToFile(`Error updating: ${err}`);
        throw err;
    }
}*/

async function updateStatusAndCause(dummyTableName, sourceTable, transactionId) {
    try {
        const updateQuery = `
            UPDATE \`${dummyTableName}\` AS dummy
            JOIN sgc_msg_log.error_code_mapping AS map
            ON dummy.status = map.code
            SET dummy.status = map.status,
                dummy.cause = map.cause
            WHERE dummy.transaction_id = ?
        `;

        const [result] = await db.query(updateQuery, [transactionId]);

        logToFile(`Batch update result: ${JSON.stringify(result)}`);
        console.log(`Updated ${result.affectedRows} rows for transaction_id: ${transactionId}`);
    } catch (err) {
        console.error("Error in updateStatusAndCause:", err);
        logToFile(`Error updating: ${err}`);
        throw err;
    }
}


router.post("/api/additional_data", async (req, res) => {
    const cdr_conn = await server_db.getConnection();
    const main_conn = await db.getConnection();

    try {
        await main_conn.beginTransaction();

        const { transactionId, source, base, client, product, date, vendor, channel, sender } = req.body;

        if (!transactionId || !source || !product || !date || !base || !client || !vendor || !channel) {
            return res.status(400).json({ message: "All fields are required." });
        }

        //logToFile(`transactionId: ${transactionId}`);

        const tableDate = new Date(date);
        const year = tableDate.getFullYear();
        const month = String(tableDate.getMonth() + 1).padStart(2, "0");
        const day = String(tableDate.getDate()).padStart(2, "0");

        let sourceTable = " ";
        let dummyTableName = " ";

        sourceTable = `cdr_${year}_${month}_${day}`;
        dummyTableName = await createDummyTable(source);
        const logTableName = await createTable(sender, month, year);
        const dndTableName = await createDndTable(sender);
        const productTableName = await createProductTable(client, month, year);
        const finalTableName = await createFinalTable(month, year);
        const transactionIdStr = String(transactionId);

        const [rows] = await cdr_conn.query(
            `
            SELECT
              ?, msgId, mobileNo, senderName, text, msgType, length, cost, globalErrorCode as status, globalErrorCode as cause, 'UI', 
              DATE_FORMAT(FROM_UNIXTIME(submitTime / 1000), '%Y-%m-%d %H:%i:%s') AS submitTime,
              DATE_FORMAT(FROM_UNIXTIME(deliveryTime / 1000), '%Y-%m-%d %H:%i:%s') AS deliveryTime,
              '',
              SUBSTRING_INDEX(SUBSTRING_INDEX(additionalData, ',', 10), ',', -1) as entity_id,
              SUBSTRING_INDEX(SUBSTRING_INDEX(additionalData, ',', 11), ',', -1) as template_id,
              ?, ?, ?, ?, 'NA', ?, ?
            FROM \`${sourceTable}\`
            WHERE msgId IN (
                SELECT msgId FROM \`${sourceTable}\` WHERE uuId = ?
            )
            `,
            [transactionIdStr, source, client, product, base, vendor, channel, transactionId]
        );

        if (!rows || rows.length === 0) {
            throw new Error("Transaction ID not found in CDR table.");
        }

        const allValues = rows.map(row => Object.values(row));
        logToFile(`All values: ${allValues}`);
        await insertIntoDummyTable(allValues, dummyTableName);


        const updateQuery = getUpdateQuery(dummyTableName);
        logToFile(`Update Query: ${updateQuery}`);
        await db.query(updateQuery);
        await updateStatusAndCause(dummyTableName, sourceTable, transactionId);

        await insertIntoLogTable(logTableName, dummyTableName);
        await insertIntoDndTable(dndTableName, dummyTableName);
        await insertIntoProductTable(productTableName, dummyTableName);
        await productInsertFinal(finalTableName, dummyTableName);

        if (dummyTableName) {
            try {
                await db.query(`DROP TABLE IF EXISTS \`${dummyTableName}\``);
                console.log(`Table ${dummyTableName} dropped successfully.`);
                logToFile(`Table dropped successfully: ${dummyTableName}`);
            } catch (dropError) {
                console.error(`Error dropping table ${dummyTableName}:`, dropError);
                logToFile(`Error dropping table: ${dropError}`);
            }
        }

        await main_conn.commit();
        res.status(200).json({ message: "All set! Data Added successfully", inserted: allValues.length });
    } catch (error) {
        console.error("Error:", error);
        logToFile(`Error: ${error}`);
        await main_conn.rollback();
        res.status(500).json({ error: error.message || "Internal Server Error" });
    } finally {
        cdr_conn.release();
        main_conn.release();
    }
});


router.get("/files/csv", async (req, res) => {
    try {
        const { fromDate, toDate, senderName } = req.query;

        if (!senderName) {
            return res.status(400).json({ error: "Sender name is required" });
        }

        function getMonthYearPairs(fromDate, toDate) {
            const startDate = new Date(fromDate);
            const endDate = new Date(toDate);
            const monthYearPairs = new Set();

            while (startDate <= endDate) {
                const year = startDate.getFullYear();
                const shortYear = year.toString().slice(-2);
                const month = String(startDate.getMonth() + 1).padStart(2, "0");
                const monthNames = new Date(year, month - 1).toLocaleString('en-US', { month: 'short' }).toLowerCase();
                monthYearPairs.add(`${monthNames}${shortYear}`);
                startDate.setMonth(startDate.getMonth() + 1);
            }

            return Array.from(monthYearPairs);
        }

        const monthYear = getMonthYearPairs(fromDate, toDate);
        const [rows] = await db.query("SELECT mapping FROM product_mapping WHERE sender = ?", [senderName]);

        const formattedSenderName = rows.length > 0 ? rows[0].mapping : senderName.replace(/\s+/g, "_");
        const dndFormattedSenderName = rows.length > 0 ? rows[0].mapping : senderName.replace(/\s+/g, "_");

        const tableName = `tbl_sms_promotional_logs_${formattedSenderName}_${monthYear}`;
        const dndTableName = `dnd_${dndFormattedSenderName}`;

        const checkTableQuery = `SHOW TABLES LIKE ?`;
        const [tableExists] = await db.query(checkTableQuery, [tableName]);
        const [dndTableExists] = await db.query("SHOW TABLES LIKE ?", [dndTableName]);

        if (tableExists.length === 0) {
            return res.status(404).json({ message: `Table '${tableName}' does not exist` });
        }

      
        let query = `
        SELECT 
            CASE 
                WHEN source LIKE '%CM_LB%' THEN 'LB'
                WHEN source LIKE '%CM_LH%' THEN 'LH'
                WHEN source LIKE '%CM_MX%' THEN 'MX'
                WHEN source LIKE '%HN_%' THEN 'HN'
                ELSE 'OTHERS'
            END AS product_key,
            circle,
            SUBSTRING_INDEX(source, '.', 1) AS source, 
            SUBSTR(submit_time, 1, 16) AS date, 
            DATE(submit_time) AS submitted_date, 
            sender as product_name,
            count(Status) as submitted_count, 
            SUM(CASE WHEN Status IN ('Delivered', '000', '0') THEN 1 ELSE 0 END) AS delivered_count,
            SUM(CASE WHEN Cause = 'NCPR Fail' THEN 1 ELSE 0 END) AS notsent_count,
            SUM(CASE WHEN cause NOT IN ('Delivered', '000', '0', 'NCPR Fail') THEN 1 ELSE 0 END) AS failed_count
        FROM ${tableName}
        WHERE sender = ?`;

        const params = [senderName];

        if (fromDate && toDate) {
            query += ` AND DATE(submit_time) BETWEEN ? AND ? `;
            params.push(fromDate, toDate);
        } else if (fromDate) {
            query += ` AND DATE(submit_time) = ? `;
            params.push(fromDate);
        } else if (toDate) {
            query += ` AND DATE(submit_time) = ? `;
            params.push(toDate);
        }

        query += " GROUP BY source, circle, date, DATE(Submit_Time) ORDER BY source ASC";
        console.log("Required query:", query);

        const [results] = await db.query(query, params);
        logToFile(`Query1 inside files csv: ${query}`);
        logToFile(`Query1 result inside files csv: ${JSON.stringify(results)}`);

       
        let query2 = `
        SELECT 
            source_group,
            SUM(submitted_count) AS total_submitted,
            SUM(delivered_count) AS total_delivered,
            SUM(notsent_count) AS total_notsent,
            SUM(failed_count) AS total_failed
        FROM (
            SELECT 
                SUBSTRING_INDEX(source, '_', 2) AS source_group,
                COUNT(Status) AS submitted_count,
                SUM(CASE WHEN Status IN ('Delivered', '000', '0') THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN Cause = 'NCPR Fail' THEN 1 ELSE 0 END) AS notsent_count,
                SUM(CASE WHEN Cause NOT IN ('Delivered', '000', '0', 'NCPR Fail') THEN 1 ELSE 0 END) AS failed_count
            FROM ${tableName}
            WHERE sender = ? AND DATE(submit_time) BETWEEN ? AND ? 
            GROUP BY SUBSTRING_INDEX(source, '_', 2)
        ) AS sub 
        GROUP BY source_group`;

        console.log("Required query:", query2);
        const [result2] = await db.query(query2, [senderName, fromDate, toDate]);
        logToFile(`Query2 inside files csv: ${query2}`);
        logToFile(`Query2 result inside files csv: ${JSON.stringify(result2)}`);

        
        let dndResults = [];
        if (dndTableExists.length > 0) {
            const dndQuery = `
                SELECT SUBSTRING_INDEX(source, '.', 1) AS source, cause , COUNT(*) AS total_failed 
                FROM ${tableName} 
                WHERE Status != 'delivered' 
                GROUP by cause, source`;

            [dndResults] = await db.query(dndQuery);
        }

     
        let query3 = `
            SELECT cause , COUNT(*) AS total_failed 
            FROM ${tableName} 
            WHERE DATE(submit_time) BETWEEN ? AND ?
            GROUP BY cause
            ORDER BY total_failed DESC`;

        const [result3] = await db.query(query3, [fromDate, toDate]);

        console.log("Dnd result:", dndResults);
        console.log("Query Results:", results);

        
        res.json({
            promotionalData: results,
            dndData: dndResults,
            failedData: result3,
            totalData: result2
        });

    } catch (error) {
        console.error("Error in /files/csv route:", error);
        logToFile(`Error in /files/csv route: ${error.message}`);
        res.status(500).json({ error: "Internal server error", details: error.message });
    }
});

/*router.get("/files/txt", async (req, res) => {
 
    let query = `
    SELECT SUBSTRING_INDEX(source, '.', 1) AS source, DATE(submit_time) AS submitted_date, sender as product_name,
      count(Status) as submitted_count,
      SUM(CASE WHEN Status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_count,
      SUM(CASE WHEN Status = 'Submitted' THEN 1 ELSE 0 END) AS pending_count,
      SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS failed_count
    FROM txt_table
  `;
})*/


module.exports = router;