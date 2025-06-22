const events = require('events');
events.EventEmitter.defaultMaxListeners = 50;

const express = require("express");
const path = require('path');
const axios = require('axios');
const smsReportRoutes = require("./routes/smsReports");
const dashboard = require("./routes/dashboardReport");
const template = require("./routes/templateReport");
const clientReportRoutes = require("./routes/clientReports");

const app = express();
const PORT = 5000;
 
const getCurrentDate = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

app.get('/health', async (req, res) => {
  const baseUrl = 'http://localhost:5000';
  const today = getCurrentDate();
 
  const endpoints = [
    `${baseUrl}/sms/api/product_sms`,
    `${baseUrl}/sms/api/product_rcs`,
    `${baseUrl}/sms/api/clients?fromDate=${today}&toDate=${today}`, 
    `${baseUrl}/sms/api/client-data?client=${encodeURIComponent("Club Mahindra Dentsu")}&fromDate=${today}&toDate=${today}`,
    //`${baseUrl}/sms/api/database_data`, 
    `${baseUrl}/sms/api/database_mapping_data`,
    `${baseUrl}/sms/files/csv?fromDate=${today}&toDate=${today}&senderName=CLUBMH`, 
  ];

  try {
    const results = await Promise.all(
      endpoints.map(async (url) => {
        try {
          const response = await axios.get(url);
          return { url, status: 'UP', code: response.status };
        } catch (err) {
          console.error(`❌ ${url} DOWN:`, err.response?.status, err.message);
          return {
            url,
            status: 'DOWN',
            error: err.response?.status || err.message
          };
        }
      })
    );

    const failed = results.filter(r => r.status === 'DOWN');

    if (failed.length === 0) {
      res.status(200).json({ status: "ALL APIs UP ✅", details: results });
    } else {
      res.status(500).json({ status: "Some APIs DOWN ❌", details: results });
    }

  } catch (err) {
    console.error("❌ Critical failure in health route:", err.message);
    res.status(500).send("Health check critical failure");
  }
});

app.use("/sms", smsReportRoutes);
app.use("/dashboard", dashboard);
app.use("/client", clientReportRoutes);
app.use('/client-uploads', express.static(path.join(__dirname, 'routes', 'client-uploads')));
app.use("/template", template);


app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
