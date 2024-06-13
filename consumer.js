import { Kafka } from "kafkajs"
import dbConnection from "./mySqlDBconnection.js"

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092']
})

  const topic = 'charger'
  const consumer = kafka.consumer({ groupId: 'test-group' })


const run = async () => {
  //Consuming
  const connection = dbConnection();
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
      // console.log(responsedata)
      await insertData(responsedata)
    },
  })

  const insertData = async (responsedata) => {
    responsedata.forEach((item) => {
        const { name, chargerName, price } = item;
        const query = 'INSERT INTO products (name, chargerName, price) VALUES (?, ?, ?)';
        connection.query(query, [name, chargerName, price], (err, results) => {
            if (err) {
                console.error('Error inserting data:', err);
                return;
            }
            console.log('Data inserted successfully:', results.insertId);
        });
    });
};
}

run().catch(console.error)