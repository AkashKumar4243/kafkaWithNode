
import { Kafka , Partitioners} from 'kafkajs';

// Example charger statuses
const chargers = {
  'charger1': 'off',
  'charger2': 'off',
  'charger3': 'off',
  'charger4': 'off',
  'charger5': 'off'
};

// Kafka setup
const kafka = new Kafka({
  clientId: 'ev-charger-monitor',
  brokers: ['localhost:9092']
});

const producers = {}; // To store active producers

// Function to monitor chargers
const monitorChargers = () => {
  setInterval(() => {
    Object.entries(chargers).forEach(async ([chargerId, status]) => {
      const newStatus = checkChargerStatus(chargerId);
      if (newStatus === 'on' && status === 'off') {
        chargers[chargerId] = 'on';
        await startKafkaProducer(chargerId);
      } else  {
        chargers[chargerId] = 'off';
        await startKafkaProducer(chargerId);
        // await stopKafkaProducer(chargerId);
      }
    });
  }, 5000); // Check every 5 seconds
};

// Example function to check charger status (mocked for demonstration)
const checkChargerStatus = (chargerId) => {
  // Replace this with actual logic to check charger status (e.g., API call to charger)
  // For demonstration purposes, simulate random status changes
  return Math.random() < 0.5 ? 'on' : 'off';
};

// Function to start Kafka producer for charger
const startKafkaProducer = async (chargerId) => {
    const producer = kafka.producer({
        createPartitioner: Partitioners.LegacyPartitioner
      });
  await producer.connect();
  console.log(`Kafka producer for ${chargerId} is ready`);
  
  // Store the producer
  producers[chargerId] = producer;
  
  // Example: Send initial message when charger starts
  await sendKafkaMessage(chargerId, producer);
};

// Function to stop Kafka producer for charger
const stopKafkaProducer = async (chargerId) => {
  if (producers[chargerId]) {
    await producers[chargerId].disconnect();
    console.log(`Kafka producer for ${chargerId} stopped`);
    delete producers[chargerId];
  }
};

// Function to send data to Kafka topic
const sendKafkaMessage = async (chargerId, producer) => {
  const message = {
    charger_id: chargerId,
    voltage : "220 Volt" ,
    status: chargers[chargerId],
    timestamp: new Date().toISOString(),
    power_usage: Math.random() * 10 // Example power usage (random)
  };

  await producer.send({
    topic: 'charger',
    messages: [{ value: JSON.stringify(message) }]
  });

  console.log(`Message sent successfully for ${chargerId} ${JSON.stringify(message)}`);
};

// Start monitoring chargers
monitorChargers();
