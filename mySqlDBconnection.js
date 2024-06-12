import mysql from "mysql"

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

  connection.query('USE myNewDatabase', (err, result) => {
    if (err) {
      console.error('Error selecting database:', err.stack);
      return;
    }
}
  )

  const demoData = [
    ['Product 1', 'Charger A', 20.99],
    ['Product 2', 'Charger B', 15.99],
    ['Product 3', 'Charger C', 25.99]
  ];

  const insertQuery = 'INSERT INTO products (name, chargername, chargerprice) VALUES ?';

  connection.query(insertQuery, [demoData], (err, result) => {
    if (err) {
      console.error('Error inserting demo data:', err.stack);
      return;
    }

    console.log('Demo data inserted successfully:', result);
}
  )

  // Create a new database
  const createDatabaseQuery = 'CREATE DATABASE IF NOT EXISTS myNewDatabase';
  
  connection.query(createDatabaseQuery, (err, result) => {
    if (err) {
      console.error('Error creating the database:', err.stack);
      return;
    }

    console.log('Database created successfully:', result);


    // Close the connection
    connection.end((err) => {
      if (err) {
        console.error('Error closing the connection:', err.stack);
        return;
      }

      console.log('Connection closed.');
    });
  });
});
