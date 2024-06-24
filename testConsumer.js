// import { Kafka } from 'kafkajs';

// // Kafka setup
// const kafka = new Kafka({
//   clientId: 'ev-charger-data-processor',
//   brokers: ['localhost:9092'],
// });

// // Consumer function to process onStatusData
// const runOnStatusConsumer = async () => {
//   const consumer = kafka.consumer({ groupId: 'ev-charger-on-status-group' });

//   await consumer.connect();
//   await consumer.subscribe({ topic: 'onStatusData', fromBeginning: true });

//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
//       const data = JSON.parse(message.value.toString());
//       console.log(`- ${prefix} Processing onStatusData:`, data);

//       // Add your logic to process the data
//     },
//   });

//   console.log('On status Kafka consumer is running');
// };

// // Consumer function to process offStatusData
// const runOffStatusConsumer = async () => {
//   const consumer = kafka.consumer({ groupId: 'ev-charger-off-status-group' });

//   await consumer.connect();
//   await consumer.subscribe({ topic: 'offStatusData', fromBeginning: true });

//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
//       const data = JSON.parse(message.value.toString());
//       console.log(`- ${prefix} Processing offStatusData:`, data);

//       // Add your logic to process the data
//     },
//   });

//   console.log('Off status Kafka consumer is running');
// };

// // Run both consumers
// const start = async () => {
//   try {
//     await Promise.all([runOnStatusConsumer(), runOffStatusConsumer()]);
//   } catch (error) {
//     console.error(`Error starting the consumers: ${error.message}`, error);
//   }
// };

// start().catch(console.error);

// // Graceful shutdown
// const shutdown = async () => {
//   console.log('Shutting down gracefully...');
//   await Promise.all([onStatusConsumer.disconnect(), offStatusConsumer.disconnect()]);
//   console.log('Kafka consumers closed');
// };

// process.on('SIGINT', shutdown);
// process.on('SIGTERM', shutdown);




//UPDATED VALUE

import { Kafka } from 'kafkajs';
import mysql from 'mysql2/promise';

// Kafka setup
const kafka = new Kafka({
  clientId: 'ev-charger-data-processor',
  brokers: ['localhost:9092'],
});

// MySQL setup
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  database: 'myNewDatabase',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Function to store data in MySQL database
const storeDataInDB = async (data, tableName) => {
  const connection = await pool.getConnection();
  try {
    const values = data.map(d => [d.charger_id, d.voltage, d.status, d.timestamp, d.power_usage]);
    const placeholders = values.map(() => '(?, ?, ?, ?, ?)').join(', ');
    const query = `INSERT INTO ${tableName} (charger_id, voltage, status, timestamp, power_usage) VALUES ${placeholders}`;
    await connection.query(query, values.flat());
  } finally {
    connection.release();
  }
};

// Consumer function to process onStatusData
const runOnStatusConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'ev-charger-on-status-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'onStatusData', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      const data = JSON.parse(message.value.toString());
      console.log(`- ${prefix} Processing onStatusData:`, data);

      try {
        // Store data in 'status_on' table
        await storeDataInDB(data, 'status_on');
      } catch (error) {
        console.error(`Error storing onStatusData: ${error.message}`, error);
      }
    },
  });

  console.log('On status Kafka consumer is running');
};

// Consumer function to process offStatusData
const runOffStatusConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'ev-charger-off-status-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'offStatusData', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      const data = JSON.parse(message.value.toString());
      console.log(`- ${prefix} Processing offStatusData:`, data);

      try {
        // Store data in 'status_off' table
        await storeDataInDB(data, 'status_off');
      } catch (error) {
        console.error(`Error storing offStatusData: ${error.message}`, error);
      }
    },
  });

  console.log('Off status Kafka consumer is running');
};

// Run both consumers
const start = async () => {
  try {
    await Promise.all([runOnStatusConsumer(), runOffStatusConsumer()]);
  } catch (error) {
    console.error(`Error starting the consumers: ${error.message}`, error);
  }
};

start().catch(console.error);

// Graceful shutdown
const shutdown = async () => {
  console.log('Shutting down gracefully...');
  await Promise.all([onStatusConsumer.disconnect(), offStatusConsumer.disconnect()]);
  console.log('Kafka consumers closed');
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
