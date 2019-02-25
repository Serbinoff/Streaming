### Streaming

This is a simple scala application that gets messages from Kafka, enhances them via REST app (Convertor) and sends messages to another Kafka Topic.

By default fromCurrency price is in USD. Please provide toCurrency as a command line argument.

Example of spark-submit:

## Local Streaming job launch

```
./bin/spark-submit --class Streaming --master local[*]  /Users/aserbinov/IdeaProjects/Streaming/target/Streaming.jar “PLN”

```

## Yarn Streaming job launch

```
./bin/spark-submit --class Streaming --master yarn  --deploy-mode cluster --executor-memory 20G --num-executors 50 /Users/aserbinov/IdeaProjects/Streaming/target/Streaming.jar “PLN”

```

