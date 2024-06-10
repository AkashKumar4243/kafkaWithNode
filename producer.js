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
// const consumer = kafka.consumer({ groupId: 'test-group' })

const producerMessages = async () => {
    const value = chance.animal();
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

  // Consuming
//   await consumer.connect()
//   await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       console.log({
//         partition,
//         offset: message.offset,
//         value: message.value.toString(),
//       })
//     },
//   })
}

run().catch(console.error)