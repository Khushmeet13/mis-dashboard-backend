const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const cors = require("cors");
const convertExcelToCSV = require("../utils/convertExcelToCSV");
const convertBajajExcelToCSV = require("../utils/convertBajajExcelToCSV");
const { db } = require("./db");
const createTableForProduct = require("../services/createTable");
const getLoadQueryForProduct = require("../services/loadQuery");

const router = express.Router();

const baseUploadDir = path.join(__dirname, "client-uploads");
if (!fs.existsSync(baseUploadDir)) {
    fs.mkdirSync(baseUploadDir);
}

const logToFile = (message) => {
    const logsDir = path.join(__dirname, "client-uploads", "logs");

    if (!fs.existsSync(logsDir)) {
        fs.mkdirSync(logsDir, { recursive: true });
    }

    const today = new Date().toISOString().split("T")[0];
    const logFilePath = path.join(logsDir, `${today}.log`);

    const timestamp = new Date().toLocaleString();
    fs.appendFileSync(logFilePath, `[${timestamp}] ${message}\n`);
};

/*const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        console.log("Multer Request Body:", req.body);
        
        const productName = req.body.productName || "default";
        console.log("Multer Received Product Name:", productName);
        const productFolder = path.join(baseUploadDir, productName);


        if (!fs.existsSync(productFolder)) {
            fs.mkdirSync(productFolder);
        }

        cb(null, productFolder);
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + "-" + file.originalname);
    },
});*/

const storage = multer.memoryStorage();
const upload = multer({ storage }).single("file");

router.use(cors());
//router.use(express.json());
router.use(express.urlencoded({ extended: true }));

router.post("/upload", upload, async (req, res) => {
    console.log("Request Body:", req.body);
    console.log("Received Product Name:", req.body.productName);

    logToFile(`Request Body: ${JSON.stringify(req.body)}`);
    logToFile(`Received Product Name: ${req.body.productName}`);


    if (!req.body.productName) {
        return res.status(400).json({ message: "Product name is missing!" });
    }

    if (!req.file) {
        return res.status(400).json({ message: "No file uploaded." });
    }

    const productName = req.body.productName;
    const productFolderBase = path.join(baseUploadDir, productName);

    if (!fs.existsSync(productFolderBase)) {
        fs.mkdirSync(productFolderBase, { recursive: true });
    }

    const filePathBase = path.join(productFolderBase, req.file.originalname);
    fs.writeFileSync(filePathBase, req.file.buffer);

    console.log("File saved to:", filePathBase);
    logToFile(`File saved to: ${filePathBase}`);

    const productFolder = path.join(path.dirname(filePathBase), productName);
    if (!fs.existsSync(productFolder)) {
        fs.mkdirSync(productFolder, { recursive: true });
    }
    //const filePath = req.file.path;
    const fileName = req.body.fileName;

    const fileNameDb = req.file.originalname;
    const filePathRelative = `/client-uploads/${productName}/${fileName}`;
    let fileDate = null;
    let match1 = fileName.match(/(\d{1,2})-([A-Za-z]{3})'(\d{2})/);


    let match2 = fileName.match(/(\d{1,2})(st|nd|rd|th)?\s+([A-Za-z]{3})/);

    if (match1) {
        const day = match1[1];
        const month = match1[2];
        const year = `20${match1[3]}`;
        const dateStr = `${day} ${month} ${year}`;
        const dateObj = new Date(dateStr);
        fileDate = dateObj.toLocaleDateString('en-GB');
    } else if (match2) {
        const day = match2[1];
        const month = match2[3];
        const year = '2025';
        const dateStr = `${day} ${month} ${year}`;
        const dateObj = new Date(dateStr);
        fileDate = dateObj.toLocaleDateString('en-GB');
    }


    try {

        let leadsTableName, salesTableName, loginTableName, approvalTableName, disbTableName;

        if (["Acko-car", "Club Mahindra"].includes(productName)) {
            ({ leadsTableName, salesTableName } = await createTableForProduct(productName, fileName));
            convertExcelToCSV(filePathBase, productFolder);
        } else if (productName === "Bajaj HL") {
            console.log("Processing for Bajaj HL...");

            ({ leadsTableName, loginTableName, approvalTableName, disbTableName } = await createTableForProduct(productName, fileName));
            convertBajajExcelToCSV(filePathBase, productFolder);
        } else {
            throw new Error("Unsupported product type");
        }

        const [existing] = await db.query(
            `SELECT * FROM \`${productName.replace(/[-\s]/g, "_")}_uploaded_files\` WHERE filename = ?`,
            [fileNameDb]
        );

        if (existing.length > 0) {
            return res.status(400).json({ message: "Whoops! This file has already been uploaded." });
        }

        const leadsCSVPath = path.join(productFolder, "leads.csv").replace(/\\/g, "/");
        const salesCSVPath = path.join(productFolder, "sales.csv").replace(/\\/g, "/");
        const loginCSVPath = path.join(productFolder, "login.csv").replace(/\\/g, "/");
        const approvalCSVPath = path.join(productFolder, "approval.csv").replace(/\\/g, "/");
        const disbCSVPath = path.join(productFolder, "disb.csv").replace(/\\/g, "/");
        console.log("salesCSVPath:", salesCSVPath);
        console.log("leadsCSVPath:", leadsCSVPath);
        console.log("loginCSVPath:", loginCSVPath);
        console.log("approvalCSVPath:", approvalCSVPath);
        console.log("disbCSVPath:", disbCSVPath);
        logToFile(`salesCSVPath: ${salesCSVPath}`);
        logToFile(`leadsCSVPath: ${leadsCSVPath}`);
        logToFile(`loginCSVPath: ${loginCSVPath}`);
        logToFile(`approvalCSVPath: ${approvalCSVPath}`);
        logToFile(`disbCSVPath: ${disbCSVPath}`);

        const loadQuery = getLoadQueryForProduct(productName, leadsCSVPath, salesCSVPath, loginCSVPath, approvalCSVPath, disbCSVPath, leadsTableName, salesTableName, loginTableName, approvalTableName, disbTableName);

        const insertFileQuery = `
            INSERT INTO \`${productName.replace(/[-\s]/g, "_")}_uploaded_files\` (date, filename, filepath)
            VALUES (?, ?, ?)
         `;

        await db.query(insertFileQuery, [fileDate, fileNameDb, filePathRelative]);

        await db.query({
            sql: loadQuery,
            values: [leadsCSVPath, salesCSVPath, loginCSVPath, approvalCSVPath, disbCSVPath],
            infileStreamFactory: (filePath) => fs.createReadStream(filePath),
        });

        return res.status(200).json({ message: "File uploaded and your data is live." });

    } catch (error) {
        console.error("Error in table creation:", error);
        logToFile(`Error in table creation: ${error.message}`);
        return res.status(500).json({ message: "Table creation failed." });
    }

});

router.get("/acko-car-count", async (req, res) => {
    const countQuery = `
        SELECT 
            SUBSTRING_INDEX(combined.utm_medium, '_', -3) AS extracted_date,  
            GROUP_CONCAT(DISTINCT combined.utm_medium ORDER BY combined.utm_medium) AS all_mediums,
            COUNT(DISTINCT l.id) AS lead_count,
            COUNT(DISTINCT s.id) AS sale_count
        FROM (
            SELECT utm_medium, id FROM acko_leads_table
            UNION ALL
            SELECT utm_medium, id FROM acko_sales_table
        ) AS combined
        LEFT JOIN acko_leads_table l 
            ON SUBSTRING_INDEX(l.utm_medium, '_', -3) = SUBSTRING_INDEX(combined.utm_medium, '_', -3)
        LEFT JOIN acko_sales_table s 
            ON SUBSTRING_INDEX(s.utm_medium, '_', -3) = SUBSTRING_INDEX(combined.utm_medium, '_', -3)
        GROUP BY extracted_date
        ORDER BY STR_TO_DATE(extracted_date, '%d_%m_%Y');
    `;


    try {
        const [results] = await db.query(countQuery);
        console.log("Successfully fetched query results");
        logToFile(`Successfully fetched query results for /acko-car-count: ${JSON.stringify(results)}`);
        res.json(results);
    } catch (err) {
        console.error("Error fetching query results:", err);
        logToFile(`Error fetching query results: ${err.message}`);
        res.status(500).json({ error: err.message });
    }

});

router.get("/acko-car", async (req, res) => {
    const { selectedDate } = req.query;

    if (!selectedDate) {
        return res.status(400).json({ error: "Please provide a valid date" });
    }


    const formattedDate = new Date(selectedDate);
    const day = ("0" + formattedDate.getDate()).slice(-2);
    const month = ("0" + (formattedDate.getMonth() + 1)).slice(-2);
    const year = formattedDate.getFullYear();
    const formattedMediumDate = `${day}_${month}_${year}`;

    console.log('Formatted Date:', formattedMediumDate);

    const query = `
    
      SELECT 
          l.id AS lead_id, 
          l.report_date AS lead_report_date, 
          l.phone AS lead_phone, 
          l.utm_campaign AS lead_campaign, 
          l.utm_medium AS lead_medium, 
          l.source AS lead_source, 
          l.utm_term AS lead_term, 
          l.product AS lead_product, 
          l.quote_load, 
          l.city AS lead_city, 
          l.city_category AS lead_city_category, 
          l.gmb_definition,
  
          s.id AS sale_id, 
          s.report_date AS sale_report_date, 
          s.phone AS sale_phone, 
          s.utm_campaign AS sale_campaign, 
          s.utm_medium AS sale_medium, 
          s.source AS sale_source, 
          s.utm_term AS sale_term, 
          s.product AS sale_product, 
          s.gwp, 
          s.city AS sale_city, 
          s.city_category AS sale_city_category, 
          s.amount,

          (SELECT COUNT(*) FROM acko_leads_table WHERE utm_medium LIKE ?) AS total_leads,
          (SELECT COUNT(*) FROM acko_sales_table WHERE utm_medium LIKE ?) AS total_sales
      FROM acko_leads_table l
      LEFT JOIN acko_sales_table s 
          ON s.utm_medium LIKE CONCAT('%', DATE_FORMAT(s.report_date, '%d_%m_%Y'), '%')
      WHERE l.utm_medium LIKE ? AND (s.utm_medium LIKE '%Acko_car%' OR s.id IS NULL);
    `;

    ///const sql = `${countquery}; ${query}`;
    try {
        const [results] = await db.query(query, [
            `%${formattedMediumDate}%`,
            `%${formattedMediumDate}%`,
            `%${formattedMediumDate}%`
        ]);

        res.json({
            detailedResults: results,
        });
        console.log("Successfully fetched table query results");
        logToFile(`Successfully fetched query results for /acko-car: ${JSON.stringify(results)}`);
    } catch (err) {
        console.error("Database Query Error:", err);
        logToFile(`Error fetching query results: ${err.message}`);
        return res.status(500).json({ error: "Database query failed." });
    }

    /*db.query(countquery, (err,results) => {
        if (err) {
            console.error(err); 
            res.status(500).send(err);
        } else {
            console.log("Successfully fetched query results"); 
            res.json(results);
        }
    })*/
});

router.get("/club-mahindra", async (req, res) => {
    try {
        const { fromDate, toDate, campType, campaignTypeFilter, refferIdFilter } = req.query;

        if (!fromDate || !toDate) {
            return res.status(400).json({ error: "fromDate and toDate are required." });
        }

        let fromMonth = new Date(fromDate).toLocaleString("en-US", { month: "short" });
        let toMonth = new Date(toDate).toLocaleString("en-US", { month: "short" });


        if (fromMonth !== toMonth) {
            return res.status(400).json({ error: "fromDate and toDate must be in the same month." });
        }


        let monthSuffix = fromMonth.toLowerCase();
        let tableName = `club_mahindra_leads_table_${monthSuffix}`;

        let query1 = `
            SELECT 
                lead_date, 
                ANY_VALUE(reffer_id_source) AS reffer_id_source,
                camp_type, 
                SUBSTRING_INDEX(ANY_VALUE(reffer_id_source), '-', -2) AS extracted_reffer_id,
                SUM(CASE WHEN lead_category = 'NQL' THEN 1 ELSE 0 END) AS NQL,
                SUM(CASE WHEN lead_category = 'QL' THEN 1 ELSE 0 END) AS QL,
                COUNT(*) AS total_leads
            FROM ${tableName} 
            WHERE lead_date BETWEEN ? AND ?`;

        let values = [fromDate, toDate];
        let conditions = [];

        if ((campaignTypeFilter && campaignTypeFilter.includes("Other Cities")) || (campType && campType.includes("Other Cities"))) {
            if (refferIdFilter && refferIdFilter !== "All") {
                conditions.push("(camp_type LIKE '%Other Cities%' AND reffer_id_source LIKE ?)");
                values.push(`%${refferIdFilter}%`);
            } else {
                conditions.push("camp_type LIKE '%Other Cities%'");
            }
        } else {
            if (campType) {
                conditions.push("TRIM(camp_type) LIKE ?");
                values.push(`%${campType}`);
            }
            if (campaignTypeFilter && campaignTypeFilter !== "All") {
                conditions.push("TRIM(camp_type) LIKE ?");
                values.push(`%${campaignTypeFilter}`);
            }
        }

        if (conditions.length > 0) {
            query1 += " AND (" + conditions.join(" OR ") + ")";
        }

        query1 += " GROUP BY lead_date, camp_type, extracted_reffer_id ORDER BY lead_date ASC";

        let query2 = `SELECT * FROM ${tableName} WHERE lead_date BETWEEN ? AND ?`;
        let values2 = [fromDate, toDate];

        if (campType) {
            query2 += " AND camp_type LIKE ?";
            values2.push(`%${campType}%`);
        }

        console.log("Query 1:", query1);
        logToFile(`Query 1: ${query1}`);

        let query3 = `SELECT lead_date, COUNT(*) AS count_of_lead_category  FROM ${tableName} 
                WHERE camp_type LIKE '%Other Cities%'
                AND tg = 'yes'
            AND (
                        lead_status LIKE 'appointment%' 
                        OR lead_status IN ('callback', 'follow up')
                    )
        or (camp_type LIKE '%Other Cities%'
                AND tg = 'yes' AND lead_status IN ('busy', 'ringing no response') AND no_of_call_attempts IN (1,2,3))
                GROUP BY lead_date;
        `;
        logToFile(`Query 3: ${query3}`);

        let query4 = `SELECT lead_date, COUNT(*) AS count_of_lead_category 
        FROM ${tableName} 
        WHERE camp_type LIKE '%Other Cities%'
        AND tg = 'yes' AND lead_status IN ('busy', 'ringing no response') AND no_of_call_attempts IN (1,2,3)
        GROUP BY lead_date;`
        logToFile(`Query 4: ${query4}`);

        const query_total_cpql = `
            SELECT lead_date, SUM(count_of_lead_category) AS total_cpql
            FROM (
                SELECT lead_date, COUNT(*) AS count_of_lead_category 
                FROM ${tableName} 
                WHERE camp_type LIKE '%Other Cities%'
                AND tg = 'yes' 
                AND (lead_status LIKE 'appointment%' OR lead_status IN ('callback', 'follow up'))
                GROUP BY lead_date

                UNION ALL

                SELECT lead_date, COUNT(*) AS count_of_lead_category 
                FROM ${tableName} 
                WHERE camp_type LIKE '%Other Cities%'
                AND tg = 'yes' 
                AND lead_status IN ('busy', 'ringing no response') 
                AND no_of_call_attempts IN (1,2,3)
                GROUP BY lead_date
            ) AS merged_data
            GROUP BY lead_date;
        `;
        logToFile(`Query total cpql: ${query_total_cpql}`);

        const connection = await db.getConnection();
        try {
            const [result1] = await connection.execute(query1, values);
            const [result2] = await connection.execute(query2, values2);
            const [result3] = await connection.execute(query3);
            const [result4] = await connection.execute(query4);
            const [result5] = await connection.execute(query_total_cpql);

            res.json({
                leadsCount: result1,
                datewiseLeads: result2,
                cpql1: result3,
                cpql2: result4,
                total: result5
            });
        } finally {
            connection.release();
        }
    } catch (error) {
        console.error("Error fetching leads:", error);
        logToFile(`Error fetching leads: ${error}`);
        res.status(500).json({ message: "Server error" });
    }
});

router.get("/club-mahindra/files-data", async (req, res) => {
    try {
        const { month } = req.query;
        console.log("month", month);

        if (!month) {
            return res.status(400).json({ error: "month is required." });
        }

        const date = new Date(`${month}-01`);
        const monthAbbreviation = date.toLocaleString('en-US', { month: 'short' });

        const query = `
            SELECT * FROM Club_Mahindra_uploaded_files
            WHERE filename LIKE ?
        `;

        const [rows] = await db.query(query, [`%${monthAbbreviation}%`]);
        console.log("files data:", [rows]);
        logToFile(`club mahindra files data: ${JSON.stringify(rows)}`);

        res.json(rows);
    } catch (error) {
        console.error("Error fetching files:", error);
        logToFile(`Error fetching files: ${error}`);
        res.status(500).json({ message: "Server error" });
    }
});



router.get("/bajaj-hl", async (req, res) => {
    const { fromDate, toDate, month, campaignTypeFilter } = req.query;
    console.log("month")

    if (!fromDate || !toDate) {
        return res.status(400).json({ error: "fromDate and toDate are required." });
    }

    if (!month) {
        return res.status(400).json({ error: "month is required." });
    }

    const date = new Date(`${month}-01`);
    const monthSuffix = date.toLocaleString('en-US', { month: 'short' });

    const formatDate = (dateStr) => {
        const date = new Date(dateStr);
        if (isNaN(date)) return null;

        let day = date.getDate().toString().padStart(2, "0");
        let month = (date.getMonth() + 1).toString().padStart(2, "0");

        return [`${day}${month}`, `${month}${day}`];
    };


    const getDateRangeFormats = (fromDate, toDate) => {
        let dates = [];
        let startDate = new Date(fromDate);
        let endDate = new Date(toDate);

        while (startDate <= endDate) {
            let formats = formatDate(startDate.toISOString().split('T')[0]);
            dates.push(...formats);
            startDate.setDate(startDate.getDate() + 1);
        }
        return dates;
    };

    const allDateFormats = getDateRangeFormats(fromDate, toDate);
    if (!allDateFormats.length) {
        return res.status(400).json({ error: "Invalid fromDate or toDate" });
    }


    const regexQuery = allDateFormats.map(date => `utm_campaign REGEXP '${date}'`).join(" OR ");

    let campaignTypeCondition = "";
    if (campaignTypeFilter) {
        if (campaignTypeFilter === "All") {
            campaignTypeCondition = "";
        } else if (campaignTypeFilter === "SMS") {
            campaignTypeCondition = `AND utm_campaign LIKE '%SM%'`;
        } else if (campaignTypeFilter === "RCS") {
            campaignTypeCondition = `AND utm_campaign LIKE '%RC%' AND utm_campaign NOT LIKE '%RC_J%'`;
        } else if (campaignTypeFilter === "JIO") {
            campaignTypeCondition = `AND utm_campaign LIKE '%RC_J%'`;
        }
    }

    let query1 = `
        WITH campaign_data AS (
            SELECT 
                utm_campaign,
                SUM(lead_bt_count + lead_fresh_count) AS lead_count,
                SUM(lead_bt_count) AS lead_bt_count,
                SUM(lead_fresh_count) AS lead_fresh_count,
                SUM(login_bt_count + login_fresh_count) AS login_count,
                SUM(login_bt_count) AS login_bt_count,
                SUM(login_fresh_count) AS login_fresh_count,
                SUM(approval_bt_count + approval_fresh_count) AS approval_count,
                SUM(approval_bt_count) AS approval_bt_count,
                SUM(approval_fresh_count) AS approval_fresh_count,
                SUM(disb_bt_count + disb_fresh_count) AS disb_count,
                SUM(disb_bt_count) AS disb_bt_count,
                SUM(disb_fresh_count) AS disb_fresh_count,
                
            
                CASE 
                    WHEN utm_campaign LIKE '%_RC_%' 
                        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(utm_campaign, '_RC_', -1), '_', 1)
                    WHEN utm_campaign LIKE '%_RC_J_%' 
                        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(utm_campaign, '_RC_J_', -1), '_', 1)
                    WHEN utm_campaign LIKE '%_SM_%' 
                        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(utm_campaign, '_SM_', -1), '_', 1)
                    ELSE RIGHT(utm_campaign, 4) 
                END AS campaign_date,

            
                CASE 
                    WHEN RIGHT(utm_campaign, 4) REGEXP '^[0-9]{4}$' 
                    THEN STR_TO_DATE(CONCAT(RIGHT(utm_campaign, 2), LEFT(RIGHT(utm_campaign, 4), 2)), '%m%d')
                    ELSE NULL 
                END AS formatted_date,

            
                CASE 
                    WHEN utm_campaign LIKE '%RC_J%' THEN 'JIO'
                    WHEN utm_campaign LIKE '%RC%' THEN 'RCS'
                    WHEN utm_campaign LIKE '%SM%' THEN 'SMS'
                    ELSE 'OTHERS' 
                END AS campaign_type

            FROM (
                SELECT utm_campaign,
                    COUNT(CASE WHEN bt_f = 'BT' THEN bt_f END) AS lead_bt_count,
                    COUNT(CASE WHEN bt_f = 'Fresh' THEN bt_f END) AS lead_fresh_count,
                    0 AS login_bt_count, 0 AS login_fresh_count,
                    0 AS approval_bt_count, 0 AS approval_fresh_count,
                    0 AS disb_bt_count, 0 AS disb_fresh_count
                FROM \`bajaj_hl_leads_table_${monthSuffix}\`
                GROUP BY utm_campaign

                UNION ALL

                SELECT utm_campaign,
                    0, 0,
                    COUNT(CASE WHEN bt = 'BT' THEN bt END), COUNT(CASE WHEN bt = 'Fresh' THEN bt END),
                    0, 0, 0, 0
                FROM \`bajaj_hl_login_table_${monthSuffix}\`
                GROUP BY utm_campaign

                UNION ALL

                SELECT utm_campaign,
                    0, 0, 0, 0,
                    COUNT(CASE WHEN bt = 'BT' THEN bt END), COUNT(CASE WHEN bt = 'Fresh' THEN bt END),
                    0, 0
                FROM \`bajaj_hl_approval_table_${monthSuffix}\`
                GROUP BY utm_campaign

                UNION ALL

                SELECT utm_campaign,
                    0, 0, 0, 0, 0, 0,
                    COUNT(CASE WHEN bt = 'BT' THEN bt END), COUNT(CASE WHEN bt = 'Fresh' THEN bt END)
                FROM \`bajaj_hl_disb_table_${monthSuffix}\`
                GROUP BY utm_campaign
            ) AS combined_data
            WHERE (${regexQuery}) ${campaignTypeCondition}
            GROUP BY utm_campaign
        )

    SELECT c.*, c.campaign_type,
            (SELECT SUM(lead_count) FROM campaign_data c2 
            WHERE c2.campaign_date = c.campaign_date 
            AND c2.campaign_type = 'RC') AS total_rc_lead_count,

            (SELECT SUM(lead_count) FROM campaign_data c3 
            WHERE c3.campaign_date = c.campaign_date 
            AND c3.campaign_type = 'SM') AS total_sm_lead_count

    FROM campaign_data c
    ORDER BY formatted_date ASC, utm_campaign;

    `;

    const tables = [
        'bajaj_hl_leads_table',
        'bajaj_hl_login_table',
        'bajaj_hl_approval_table',
        'bajaj_hl_disb_table'
    ];

    let query2 = `
        SELECT * FROM \`bajaj_hl_leads_table_${monthSuffix}\` WHERE (${regexQuery}) 
        UNION ALL
        SELECT * FROM \`bajaj_hl_login_table_${monthSuffix}\` WHERE (${regexQuery}) 
        UNION ALL
        SELECT * FROM \`bajaj_hl_approval_table_${monthSuffix}\` WHERE (${regexQuery}) 
        UNION ALL
        SELECT * FROM \`bajaj_hl_disb_table_${monthSuffix}\` WHERE (${regexQuery}) 
    `;

    const results = await Promise.all(
        tables.map(async (tableName) => {
            const query = `SELECT *, '${tableName}' AS source FROM \`${tableName}_${monthSuffix}\` WHERE (${regexQuery})`;
            const [rows] = await db.query(query);
            return { table: tableName, data: rows };
        })
    );

    const tableData = {};
    results.forEach(({ table, data }) => {
        tableData[table] = data;
    });

    try {
        const [result1] = await db.query(query1);
        //const [result2] = await db.query(query2);
        //console.log("Successfully fetched query results", result2);
        res.json({ leadsCount: result1, tableData: tableData });
    } catch (err) {
        console.error("Error fetching query results:", err);
        logToFile(`Error fetching files: ${err}`);
        res.status(500).json({ error: err.message });
    }
});

router.get("/bajaj-hl/files-data", async (req, res) => {
    try {
        const { month } = req.query;
        console.log("month", month);

        if (!month) {
            return res.status(400).json({ error: "month is required." });
        }

        const date = new Date(`${month}-01`);
        const monthAbbreviation = date.toLocaleString('en-US', { month: 'short' });

        const query = `
            SELECT * FROM bajaj_hl_uploaded_files
            WHERE filename LIKE ?
        `;

        const [rows] = await db.query(query, [`%${monthAbbreviation}%`]);
        console.log("files data:", [rows]);
        logToFile(`bajaj hl files data: ${JSON.stringify(rows)}`);

        res.json(rows);
    } catch (error) {
        console.error("Error fetching files:", error);
        logToFile(`Error fetching files: ${error}`);
        res.status(500).json({ message: "Server error" });
    }
});



module.exports = router;
