import { Kafka , Partitioners} from 'kafkajs';

// Example charger statuses
const chargers = {
  'charger1': 'off',
  'charger2': 'off'
};

// Kafka setup
const kafka = new Kafka({
  clientId: 'ev-charger-monitor',
  brokers: ['localhost:9092']
});

const producers = {}; // To store active producers

// Function to monitor chargers
const monitorChargers = () => {
  Object.entries(chargers).forEach(async ([chargerId, status]) => {
    const newStatus = checkChargerStatus(chargerId);
    if (newStatus !== status) {
      chargers[chargerId] = newStatus;
      if (newStatus === 'on') {
        await startKafkaProducer(chargerId);
      } else {
        await sendKafkaMessage(chargerId);
        await stopKafkaProducer(chargerId);
      }
    }
  });

  // Set a timeout to run the monitoring again after 5 seconds
  setTimeout(monitorChargers, 5000);
};

// Example function to check charger status (mocked for demonstration)
const checkChargerStatus = (chargerId) => {
  // Replace this with actual logic to check charger status (e.g., API call to charger)
  // For demonstration purposes, simulate random status changes
  return Math.random() < 0.5 ? 'on' : 'off';
};

// Function to start Kafka producer for charger
const startKafkaProducer = async (chargerId) => {
  const producer = kafka.producer();
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
    status: chargers[chargerId], // Use current status from chargers object
    timestamp: new Date().toISOString(),
    power_usage: chargers[chargerId] === 'on' ? Math.random() * 10 : 0 // Example power usage (random for 'on', 0 for 'off')
  };

  await producer.send({
    topic: 'charger',
    messages: [{ value: JSON.stringify(message) }]
  });

  console.log(`Message sent successfully for ${chargerId} with status ${message.status}`);
};

// Start monitoring chargers
monitorChargers();
