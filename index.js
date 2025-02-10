import pkg from "kafkajs";
const { Kafka } = pkg;
import dotenv from "dotenv";
dotenv.config();
import {
  KinesisClient,
  GetShardIteratorCommand,
  GetRecordsCommand,
  ListShardsCommand,
} from "@aws-sdk/client-kinesis";

// Initialize Kinesis Client
const kinesisClient = new KinesisClient({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Create the Kafka client
const kafka = new Kafka({
  clientId: "push-data-service-" + Date.now(),
  brokers: [
    process.env.KAFKA_BOOTSTRAP_SERVER_URL ||
      "my-cluster-kafka-bootstrap.kafka:9092",
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const producer = kafka.producer();

const getShardIterator = async (streamName, shardId) => {
  const command = new GetShardIteratorCommand({
    StreamName: streamName,
    ShardId: shardId,
    ShardIteratorType: "LATEST",
  });
  const response = await kinesisClient.send(command);
  return response.ShardIterator;
};

const readFromKinesis = async () => {
  await producer.connect();
  console.info("Connected to Kafka Broker.");

  const streamName = process.env.KINESIS_STREAM_NAME || "my-kinesis-stream";
  const listShardsCommand = new ListShardsCommand({ StreamName: streamName });
  const shardResponse = await kinesisClient.send(listShardsCommand);
  const shards = shardResponse.Shards;

  for (const shard of shards) {
    let shardIterator = await getShardIterator(streamName, shard.ShardId);
    while (shardIterator) {
      const recordsCommand = new GetRecordsCommand({
        ShardIterator: shardIterator,
      });
      const recordsResponse = await kinesisClient.send(recordsCommand);
      console.log("Received records from Kinesis:", recordsResponse);
      shardIterator = recordsResponse.NextShardIterator;

      if (recordsResponse.Records.length > 0) {
        for (const record of recordsResponse.Records) {
          const payload = JSON.parse(Buffer.from(record.Data).toString());

          if (payload) {
            console.log("Publishing to Kafka:", payload);
            await producer.send({
              topic: process.env.PUBLISH_TOPIC || "output",
              messages: [
                {
                  key:
                    payload.imeiNo ||
                    Math.random().toString(36).substring(2, 15),
                  value: JSON.stringify(payload),
                },
              ],
            });
          }
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
};

readFromKinesis().catch((error) =>
  console.error("Error reading from Kinesis:", error)
);
