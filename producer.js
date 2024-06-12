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

// Buffer to store data for 10 seconds
let buffer = [];

// Variable to store accumulated data for 10 seconds
let tenSecondData = [];

// Function to produce messages
const producerMessages = async (buffer) => {
  console.log(buffer)

  if (!buffer || buffer.length === 0) {
    return;
} 

  // const data = generateRandomData();
  try {
    await producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(buffer) // Convert the data object to a string
        }
      ]
    });
    console.log(`Sent message: ${JSON.stringify(buffer)}`);
  } catch (error) {
    console.error('Error sending message:', error);
  }

  // Clear the buffer after sending
  buffer = [];

};


function continuousDataSending() {
  const data = generateRandomData();

  console.log(data);
  // Add data to buffer
  buffer.push(...data);
}

// Function to handle the accumulation and storage of data every 10 seconds
function handleTenSecondInterval() {
  // Store the current buffer in the tenSecondData variable
  tenSecondData = [...buffer];

  // Optionally, send the buffered data to Kafka
  producerMessages (buffer);
}


// Function to run the producer
const run = async () => {
  await producer.connect(); // Ensure the producer is connected
  // producerMessages(); // Send a message
  // Optionally send messages at intervals
  // Call continuousDataSending() every second
  setInterval(continuousDataSending, 1000);

  // Handle the data accumulation and storage every 10 seconds
  setInterval(handleTenSecondInterval, 10000);

  setInterval(() => {
    producerMessages();
  }, 20000);
};

// Run the producer
run().catch(console.error);
