import { Kafka } from 'kafkajs';

// Kafka setup
const kafka = new Kafka({
  clientId: 'ev-charger-monitor',
  brokers: ['localhost:9092'],
});

// Arrays to store data based on status
let onStatusData = [];
let offStatusData = [];

// Producer setup
const producer = kafka.producer();

// Function to send buffered data
const sendBufferedData = async () => {
  if (onStatusData.length > 0) {
    await producer.send({
      topic: 'onStatusData',
      messages: [{ value: JSON.stringify(onStatusData) }],
    });
    console.log('Sent onStatusData:', onStatusData);
    onStatusData = [];
  }
  if (offStatusData.length > 0) {
    await producer.send({
      topic: 'offStatusData',
      messages: [{ value: JSON.stringify(offStatusData) }],
    });
    console.log('Sent offStatusData:', offStatusData);
    offStatusData = [];
  }
};

// Consumer function
const runConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'ev-charger-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'charger', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      const data = message.value.toString();
      console.log(`- ${prefix} ${data}`);
      
      try {
        // Process the message (e.g., store it in different arrays)
        processMessage(data);
      } catch (error) {
        console.error(`Error processing message: ${error.message}`, error);
      }
    },
  });

  console.log('Kafka consumer is running');
};

// Function to process the message and store it in different arrays based on status
const processMessage = (message) => {
  const data = JSON.parse(message);
  console.log(`Processing message:`, data);
  
  // Filter and store data based on status
  if (data.status === 'on') {
    onStatusData.push(data);
  } else if (data.status === 'off') {
    offStatusData.push(data);
  }
};

// Run the consumer
const start = async () => {
  try {
    await producer.connect();
    await runConsumer();

    // Set interval to send buffered data every 30 seconds
    setInterval(sendBufferedData, 30000);
  } catch (error) {
    console.error(`Error starting the consumer: ${error.message}`, error);
  }
};

start().catch(console.error);

// Graceful shutdown
const shutdown = async () => {
  console.log('Shutting down gracefully...');
  await producer.disconnect();
  await consumer.disconnect();
  console.log('Kafka consumer and producer closed');
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
