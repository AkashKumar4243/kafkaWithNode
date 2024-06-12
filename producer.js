import { Kafka, Partitioners } from 'kafkajs';
import generateRandomData from './generateDemoData.js';

// Create a Kafka client and producer
const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const topic = 'charger';

// Function to produce messages
const producerMessages = async () => {
  const data = generateRandomData();
  try {
    await producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(data) // Convert the data object to a string
        }
      ]
    });
    console.log(`Sent message: ${JSON.stringify(data)}`);
  } catch (error) {
    console.error('Error sending message:', error);
  }
};

// Function to run the producer
const run = async () => {
  await producer.connect(); // Ensure the producer is connected
  // producerMessages(); // Send a message
  // Optionally send messages at intervals
  setInterval(() => {
    producerMessages();
  }, 5000);
};

// Run the producer
run().catch(console.error);
