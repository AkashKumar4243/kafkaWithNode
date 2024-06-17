import { Kafka, Partitioners } from 'kafkajs';
import generateRandomData from './dataGeneration/generateDemoData.js';
import generateRandomData1 from './dataGeneration/generateDemoData1.js';
import generateRandomData2 from './dataGeneration/generateDemoData2.js';
import generateRandomData3 from './dataGeneration/generateDemoData3.js';
import generateRandomData4 from './dataGeneration/generateDemoData4.js';
import generateRandomData5 from './dataGeneration/generateDemoData5.js';
import generateRandomData6 from './dataGeneration/generateDemoData6.js';
import generateRandomData7 from './dataGeneration/generateDemoData7.js';
import generateRandomData8 from './dataGeneration/generateDemoData8.js';
import generateRandomData9 from './dataGeneration/generateDemoData9.js';
import generateRandomData10 from './dataGeneration/generateDemoData10.js';
import generateRandomData11 from './dataGeneration/generateDemoData11.js';
import generateRandomData12 from './dataGeneration/generateDemoData12.js';
import generateRandomData13 from './dataGeneration/generateDemoData13.js';
import generateRandomData14 from './dataGeneration/generateDemoData14.js';
import generateRandomData15 from './dataGeneration/generateDemoData15.js';
import generateRandomData16 from './dataGeneration/generateDemoData16.js';
import generateRandomData17 from './dataGeneration/generateDemoData17.js';
import generateRandomData18 from './dataGeneration/generateDemoData18.js';
import generateRandomData19 from './dataGeneration/generateDemoData19.js';

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
let buffers = Array.from({ length: 20 }, () => []);

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
    console.log("Sent message:", JSON.stringify(data));
  } catch (error) {
    console.error('Error sending message:', error);
  }

  // Clear the buffers after sending
  buffers = Array.from({ length: 20 }, () => []);
};

// Function to generate random data and add it to the buffers
const continuousDataSending = (generateData, index) => {
  const data = generateData();
  console.log(`Generated data from source ${index}:`, data);
  buffers[index].push(...data);
  console.log(`Data added successfully to buffer ${index}`);
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
  setInterval(() => continuousDataSending(generateRandomData6, 6), 7000);
  setInterval(() => continuousDataSending(generateRandomData7, 7), 8000);
  setInterval(() => continuousDataSending(generateRandomData8, 8), 9000);
  setInterval(() => continuousDataSending(generateRandomData9, 9), 10000);
  setInterval(() => continuousDataSending(generateRandomData10, 10), 11000);
  setInterval(() => continuousDataSending(generateRandomData11, 11), 12000);
  setInterval(() => continuousDataSending(generateRandomData12, 12), 13000);
  setInterval(() => continuousDataSending(generateRandomData13, 13), 14000);
  setInterval(() => continuousDataSending(generateRandomData14, 14), 15000);
  setInterval(() => continuousDataSending(generateRandomData15, 15), 16000);
  setInterval(() => continuousDataSending(generateRandomData16, 16), 17000);
  setInterval(() => continuousDataSending(generateRandomData17, 17), 18000);
  setInterval(() => continuousDataSending(generateRandomData18, 18), 19000);
  setInterval(() => continuousDataSending(generateRandomData19, 19), 20000);

  // Send accumulated data every 10 seconds
  setInterval(handleTenSecondInterval, 20000);
};

// Run the producer
run().catch(console.error);
