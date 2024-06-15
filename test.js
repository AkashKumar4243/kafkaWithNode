import { Kafka, Partitioners } from 'kafkajs';
import generateRandomData from './dataGeneration/generateDemoData.js';
import generateRandomData1 from './dataGeneration/generateDemoData1.js';
import generateRandomData2 from './dataGeneration/generateDemoData2.js';
import generateRandomData3 from './dataGeneration/generateDemoData3.js';
import generateRandomData4 from './dataGeneration/generateDemoData4.js';
import generateRandomData5 from './dataGeneration/generateDemoData5.js';

// Create a Kafka client and producer
const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const topic = 'charger';

// Buffers to store data
let buffers = Array.from({ length: 6 }, () => []);

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
    console.log("added")
    // console.log(`Sent message: ${JSON.stringify(data)}`);
  } catch (error) {
    console.error('Error sending message:', error);
  }

  // Clear the buffers after sending
  buffers = Array.from({ length: 6 }, () => []);
};

// Function to generate random data and add it to the buffers
const continuousDataSending = (generateData, index) => {
  const data = generateData();
  console.log(data);
  buffers[index].push(...data);
  console.log("Data added successfully to buffer", index);
};

// Function to handle the accumulation and storage of data every 10 seconds
const handleTenSecondInterval = () => {
  const tenSecondData = buffers.flat();
  produceMessages(tenSecondData);
};

// Function to run the producer
const run = async () => {
  await producer.connect(); // Ensure the producer is connected

  // Set intervals for each data generation function
  setInterval(() => continuousDataSending(generateRandomData, 0), 1000);
  setInterval(() => continuousDataSending(generateRandomData1, 1), 2000);
  setInterval(() => continuousDataSending(generateRandomData2, 2), 3000);
  setInterval(() => continuousDataSending(generateRandomData3, 3), 4000);
  setInterval(() => continuousDataSending(generateRandomData4, 4), 5000);
  setInterval(() => continuousDataSending(generateRandomData5, 5), 6000);

  // Send accumulated data every 10 seconds
  setInterval(handleTenSecondInterval, 10000);
};

// Run the producer
run().catch(console.error);
