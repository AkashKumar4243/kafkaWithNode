const { Kafka ,Partitioners} = require('kafkajs')
var Chance = require('chance');

var chance = new Chance();

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092']
})

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner
  });

  const topic = 'animals'


const producerMessages = async () => {
    const value = chance.animal();
    console.log(value)
    try {
        await producer.send({
            topic,
            messages : [{value}]
        })
    } catch (error) {
        console.log(error)
    }
}

const run = async () => {
  // Producing
  await producer.connect()
  setInterval(() => {
    producerMessages();
  }, 1000); 
}

run().catch(console.error)