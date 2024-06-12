const mysql = require('mysql');

// Create a connection to the MySQL server
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
});

// Connect to the MySQL server
connection.connect((err) => {
  if (err) {
    console.error('Error connecting to the database:', err.stack);
    return;
  }

  console.log('Connected to the database.');

  // Create a new database
  const createDatabaseQuery = 'CREATE DATABASE IF NOT EXISTS myNewDatabase';
  
  connection.query(createDatabaseQuery, (err, result) => {
    if (err) {
      console.error('Error creating the database:', err.stack);
      return;
    }

    console.log('Database created successfully:', result);

    // Close the connection
    // connection.end((err) => {
    //   if (err) {
    //     console.error('Error closing the connection:', err.stack);
    //     return;
    //   }

    //   console.log('Connection closed.');
    // });
  });
});
