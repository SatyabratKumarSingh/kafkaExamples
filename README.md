# kafkaExamples

The project deals with calculating average of the stock prices from a stream.

It has 2 parts.

1. Kafka Producer - A Kafka Producer which mocks some data to put into Kafka Topic.

2. Kafka Consumer - Reads the Kafka Stream, joins with a existing local price sheet,and calculate the average as the prices appear in stream.
