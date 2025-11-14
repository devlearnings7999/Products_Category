# Product Categorization Service

This service consumes product messages from a Kafka topic, categorizes them, and publishes the categorized products to another Kafka topic.

## Prerequisites

*   Kafka cluster
*   Python 3.6+
*   `pip install python-dotenv kafka-python`

## Configuration

Create a `.env` file with the following variables:

```
KAFKA_BROKER=<your_kafka_broker>
INPUT_TOPIC=products_producer
OUTPUT_TOPIC=products_categorized
SECURITY_PROTOCOL=SASL_PLAINTEXT
SASL_MECHANISM=SCRAM-SHA-512
SASL_USERNAME=<your_kafka_username>
SASL_PASSWORD=<your_kafka_password>
```

## Usage

1.  Install dependencies: `pip install python-dotenv kafka-python`
2.  Run the consumer: `python product_consumer.py`

## Docker

1.  Build the image: `docker build -t product-categorization .`
2.  Run the container:
    ```
    docker run -e KAFKA_BROKER=<your_kafka_broker> \
               -e INPUT_TOPIC=products_producer \
               -e OUTPUT_TOPIC=products_categorized \
               -e SECURITY_PROTOCOL=SASL_PLAINTEXT \
               -e SASL_MECHANISM=SCRAM-SHA-512 \
               -e SASL_USERNAME=<your_kafka_username> \
               -e SASL_PASSWORD=<your_kafka_password> \
               product-categorization
    ```
