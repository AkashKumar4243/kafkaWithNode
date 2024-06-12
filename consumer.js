import { Kafka } from "kafkajs"

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092']
})

  const topic = 'charger'
  const consumer = kafka.consumer({ groupId: 'test-group' })


const run = async () => {
  //Consuming
  await consumer.connect()
  await consumer.subscribe({topic})

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // console.log({
      //   partition,
      //   offset: message.offset,
      //   value: message.value.toString(),
      // })
      const responsedata = message.value.toString();
      console.log(responsedata)
    },
  })
}

run().catch(console.error)