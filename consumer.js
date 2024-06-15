import { Kafka } from 'kafkajs';
import mysql from 'mysql2/promise';


// Create a pool connection to the database
const pool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    database: 'myNewDatabase',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Kafka configuration
const kafka = new Kafka({
    clientId: 'my-consumer',
    brokers: ['localhost:9092']
});

const topic = 'charger';
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const responsedata = JSON.parse(message.value.toString());
            console.log(responsedata)
            await insertData(responsedata);
        },
    });
};

const insertData = async (data) => {
    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        const query = 'INSERT INTO products (name, chargername, chargerprice) VALUES ?';
        const values = data.map(item => [item.name, item.chargername, item.chargerprice]);
        await connection.query(query, [values]);
        await connection.commit();
        console.log('Batch data inserted successfully');
    } catch (error) {
        await connection.rollback();
        console.error('Error inserting data:', error);
    } finally {
        connection.release();
    }
};

run().catch(console.error);

// Handle process termination and cleanup
process.on('SIGINT', async () => {
    console.log('Disconnecting from Kafka and closing database connection...');
    await consumer.disconnect();
    await pool.end();
    console.log('Database connection closed');
    process.exit();
});
