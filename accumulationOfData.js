const { Kafka } = require('kafkajs');

// Kafka client configuration
const kafka = new Kafka({
    clientId: 'my-producer',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

// Buffer to store data for 10 seconds
let buffer = [];

// Variable to store accumulated data for 10 seconds
let tenSecondData = [];

// Function to send buffered data to Kafka
async function sendBufferedMessages(producer, buffer) {
    if (buffer.length === 0) {
        return;
    }

    const messages = buffer.map(item => ({
        value: JSON.stringify(item) // Convert each item to JSON string
    }));

    try {
        await producer.send({
            topic: 'your_topic', // Replace with your topic
            messages: messages
        });
        console.log('Data sent to Kafka successfully');
    } catch (err) {
        console.error('Error sending data to Kafka:', err);
    }

    // Clear the buffer after sending
    buffer = [];
}

// Function to simulate continuous data sending
function continuousDataSending() {
    const data = [
        { name: 'example1', chargerName: 'charger1', price: Math.random() * 1000 },
        { name: 'example2', chargerName: 'charger2', price: Math.random() * 1000 },
        // Add more sample data here if needed
    ];

    // Add data to buffer
    buffer.push(...data);
}

// Function to handle the accumulation and storage of data every 10 seconds
function handleTenSecondInterval() {
    // Store the current buffer in the tenSecondData variable
    tenSecondData = [...buffer];

    // Optionally, send the buffered data to Kafka
    sendBufferedMessages(producer, buffer);
}

// Connect to Kafka and set up the data sending
async function run() {
    await producer.connect();
    console.log('Connected to Kafka');

    // Call continuousDataSending() every second
    setInterval(continuousDataSending, 1000);

    // Handle the data accumulation and storage every 10 seconds
    setInterval(handleTenSecondInterval, 10000);
}

run().catch(console.error);

// Access tenSecondData variable when needed
module.exports = { tenSecondData };
