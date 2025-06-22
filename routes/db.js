const mysql = require("mysql2/promise");
const fs = require("fs");

const db = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "mysql",
    database: "sgc_msg_log",
    multipleStatements: true,
    localInfile: true,  
    streamFactory: (path) => fs.createReadStream(path)
});


const product_db = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "mysql",
    database: "sgc_msg_log", 
    multipleStatements: true,
});

const server_db = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "mysql",
    database: "Server13Logs", 
    multipleStatements: true,
});

const product_db_name = "sgc_msg_log";
const db_name = "sgc_msg_log";
const server_db_name = "Server13Logs";


async function testConnection() {
    try {
        const connection = await db.getConnection();
        console.log("Connected to MySQL db database");
        connection.release(); 

        const productConnection = await product_db.getConnection();
        console.log("Connected to MySQL product database");
        productConnection.release(); 

        const serverDbConnection = await server_db.getConnection();
        console.log("Connected to MySQL server database");
        serverDbConnection.release(); 
    } catch (err) {
        console.error("Database connection failed:", err);
    }
}


async function enableLocalInfile() {
    try {
        await db.query("SET GLOBAL local_infile = 1;");
        console.log("local_infile enabled");
    } catch (err) {
        console.error("Error enabling local_infile:", err);
    }
}


testConnection();
enableLocalInfile();

module.exports = { db, product_db, server_db, product_db_name, db_name, server_db_name};
