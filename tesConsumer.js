import { Kafka } from 'kafkajs';

// Kafka setup
const kafka = new Kafka({
  clientId: 'ev-charger-monitor',
  brokers: ['localhost:9092']
});

// Consumer function
const runConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'ev-charger-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'charger', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      const data = message.value.toString();
    //   console.log(`- ${prefix} ${data}`);
      
      // Process the message (e.g., store it in a database)
      processMessage(data);
    },
  });

  console.log('Kafka consumer is running');
};

// Example function to process the message (e.g., store it in a database)
const processMessage = (message) => {
  const data = JSON.parse(message);
  console.log(`Processing message:`, data);
  // Add your logic to store the data in a database or perform other actions
};

// Run the consumer
runConsumer().catch(console.error);
