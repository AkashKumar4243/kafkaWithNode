import generateRandomData from "./generateDemoData.js";
import mysql from "mysql"

const data = generateRandomData();

console.log(data)

// Create a connection to MySQL
const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    database: 'myNewDatabase'
});

// Connect to MySQL
connection.connect((err) => {
    if (err) {
        console.error('Error connecting to MySQL: ' + err.stack);
        return;
    }
    console.log('Connected to MySQL as id ' + connection.threadId);
});

// Function to insert data into MySQL
function insertDataIntoMySQL() {
    const sql = 'INSERT INTO products (name,chargername,chargerprice) VALUES ?';

    connection.query(sql, [data.map(obj => [obj.name, obj.chargerName, obj.price])], (err, result) => {
        if (err) {
            console.error('Error inserting data: ' + err.stack);
            return;
        }
        console.log('Data inserted successfully.');
    });
}

// Call insertDataIntoMySQL() every 2 seconds
insertDataIntoMySQL();
