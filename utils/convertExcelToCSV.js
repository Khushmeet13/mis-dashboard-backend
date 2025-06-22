const xlsx = require("xlsx");
const fs = require("fs");
const path = require("path");

const convertExcelToCSV = (filePath, uploadFolder) => {
  
    const workbook = xlsx.readFile(filePath);
    
    const leadsSheet = workbook.Sheets[workbook.SheetNames[0]];
    let leadsCSV = xlsx.utils.sheet_to_csv(leadsSheet);
    
   
    leadsCSV = processData(leadsCSV);

  
    fs.writeFileSync(path.join(uploadFolder, "leads.csv"), leadsCSV);
    
   
    const salesSheet = workbook.Sheets[workbook.SheetNames[1]];
    let salesCSV = xlsx.utils.sheet_to_csv(salesSheet);
    
  
    salesCSV = processData(salesCSV);

  
    fs.writeFileSync(path.join(uploadFolder, "sales.csv"), salesCSV);
};


const processData = (csvData) => {
    const rows = csvData.split('\n');
    const header = rows[0]; 
    const dataRows = rows.slice(1);

  
    const processedRows = dataRows.map(row => {
        const columns = row.split(',');

      
        columns[0] = formatDate(columns[0]);

       
        return columns.join(',');
    });

   
    return [header, ...processedRows].join('\n');
};


const formatDate = (dateString) => {
    const dateParts = dateString.split('/');
    if (dateParts.length === 3) {
       
        const year = `20${dateParts[2]}`;
        const month = dateParts[0].padStart(2, '0');
        const day = dateParts[1].padStart(2, '0');
        
        return `${year}-${month}-${day}`;
    }
    return dateString; 
};

module.exports = convertExcelToCSV;
