import { Kafka, Partitioners } from 'kafkajs';
import generateRandomData from './dataGeneration/generateDemoData.js';
import generateRandomData2 from './dataGeneration/generateDemoData1.js';
import generateRandomData1 from './dataGeneration/generateDemoData2.js';
import generateRandomData4 from './dataGeneration/generateDemoData4.js';
import generateRandomData5 from './dataGeneration/generateDemoData5.js';
import generateRandomData3 from './dataGeneration/generateDemoData3.js';

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
let buffer3 = [];
let buffer4 = [];
let buffer5 = [];

// Variable to store accumulated data for 10 seconds
let tenSecondData = [];

// Function to produce messages
const producerMessages = async (tenSecondData) => {
  // console.log(tenSecondData)

  if (!tenSecondData || tenSecondData.length === 0) {
    return;
} 

  // const data = generateRandomData();
  try {
    await producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(tenSecondData) // Convert the data object to a string
        }
      ]
    });
    console.log(`Sent message: ${JSON.stringify(tenSecondData)}`);
    // console.log(buffer)
    buffer = [];
    buffer1 = [];
    buffer2 = [];
    buffer3 = [];
    buffer4 = [];
    buffer5 = [];
    tenSecondData = [];
  } catch (error) {
    console.error('Error sending message:', error);
  }

  // Clear the buffer after sending
  buffer = [];
  buffer1 = [];
  buffer2 = [];
  buffer3 = [];
  buffer4 = [];
  buffer5 = [];
  tenSecondData = [];

};


function continuousDataSending() {
  const data  = generateRandomData();
  const data1 = generateRandomData1();
  const data2 = generateRandomData2();
  const data3 = generateRandomData3();
  const data4 = generateRandomData4();
  const data5 = generateRandomData5();

  console.log(data);
  console.log(data1);
  console.log(data2);
  console.log(data3);
  console.log(data4);
  console.log(data5);
  console.log("data added succesfully")
  // Add data to buffer

  buffer.push(...data);
  buffer1.push(...data1);
  buffer2.push(...data2);
  buffer2.push(...data3);
  buffer2.push(...data4);
  buffer2.push(...data5);
}

// Function to handle the accumulation and storage of data every 10 seconds
function handleTenSecondInterval() {
  // Store the current buffer in the tenSecondData variable
  tenSecondData = [...buffer, ...buffer1, ...buffer2, ...buffer3, ...buffer4, ...buffer5];

  // Optionally, send the buffered data to Kafka
  producerMessages (tenSecondData);
}


// Function to run the producer
const run = async () => {
  await producer.connect(); // Ensure the producer is connected
  // producerMessages(); // Send a message
  // Optionally send messages at intervals
  // Call continuousDataSending() every second

  setInterval(continuousDataSending, 1000);


// // Function to run continuousDataSending with a delay every 10 seconds
// function runContinuousDataSendingWithDelay() {
//   let counter = 0;

//   const runWithDelay = () => {
//     if (counter < 1) {
//       setInterval(continuousDataSending, 1000);
//       counter++;
      
//     } else {
//       console.log('Pausing for 2 seconds...');
//       setTimeout(() => {
//         counter = 0;
//         runWithDelay(); // Restart the cycle after 2 seconds
//       }, 10000);
//     }
//   };

//   runWithDelay(); // Start the cycle
// }

// runContinuousDataSendingWithDelay()


  // Handle the data accumulation and storage every 10 seconds
  setInterval(handleTenSecondInterval, 10000);

  producerMessages(tenSecondData)

  // setInterval(() => {
  //   producerMessages();
  // }, 1000);
};

// Run the producer
run().catch(console.error);
