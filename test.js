import { Kafka, Partitioners } from 'kafkajs';
import generateRandomData from './generateDemoData.js';
import generateRandomData2 from './generateDemoData2.js';
import generateRandomData1 from './generateDemoData1.js';

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
let buffer1 = [];
let buffer2 = [];

// Function to produce messages
const produceMessages = async (data) => {
  if (!data || data.length === 0) {
    return;
  }

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

// Function to simulate continuous data generation
function continuousDataSending() {
  const data = generateRandomData();
  const data1 = generateRandomData1();
  const data2 = generateRandomData2();

  console.log('Generated Data:', data, data1, data2);
  buffer.push(...data);
  buffer1.push(...data1);
  buffer2.push(...data2);
}

// Function to handle the accumulation and sending of data every 10 seconds
function handleTenSecondInterval() {
  const tenSecondData = [...buffer, ...buffer1, ...buffer2];
  produceMessages(tenSecondData);

  // Clear buffers after sending
  buffer = [];
  buffer1 = [];
  buffer2 = [];
}

// Function to run continuous data generation with delays
function runContinuousDataSendingWithDelay() {
  let counter = 0;

  const runWithDelay = () => {
    if (counter < 10) {
      continuousDataSending();
      counter++;
      setTimeout(runWithDelay, 1000); // Call again in 1 second
    } else {
      console.log('Pausing for 2 seconds...');
      setTimeout(() => {
        counter = 0;
        runWithDelay(); // Restart the cycle after 2 seconds
      }, 2000);
    }
  };

  runWithDelay(); // Start the cycle
}

// Function to run the producer
const run = async () => {
  await producer.connect();

  runContinuousDataSendingWithDelay();

  // Handle the data accumulation and storage every 12 seconds
  setInterval(handleTenSecondInterval, 11000);
};

// Run the producer
run().catch(console.error);
