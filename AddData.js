const addData = () => {
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
}

export default addData;