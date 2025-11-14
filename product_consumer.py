
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import json

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
INPUT_TOPIC = os.getenv('INPUT_TOPIC')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC')
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL')
SASL_MECHANISM = os.getenv('SASL_MECHANISM')
SASL_USERNAME = os.getenv('SASL_USERNAME')
SASL_PASSWORD = os.getenv('SASL_PASSWORD')

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def categorize_product(product):
    product_name = product.get('product_name', '').lower()
    if any(keyword in product_name for keyword in ['phone', 'laptop', 'headphones']):
        return 'Electronics'
    elif any(keyword in product_name for keyword in ['shirt', 'pants', 'dress']):
        return 'Clothing'
    elif any(keyword in product_name for keyword in ['fridge', 'oven', 'microwave']):
        return 'Home Appliances'
    elif any(keyword in product_name for keyword in ['book', 'novel', 'magazine']):
        return 'Books'
    else:
        return 'Others'

try:
    for message in consumer:
        product = message.value
        category = categorize_product(product)
        print(f"Product: {product['product_name']}, Category: {category}")

        # Produce to output topic
        producer.send(OUTPUT_TOPIC, {'product_id': product['product_id'], 'product_name': product['product_name'], 'category': category})
        producer.flush()

except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    producer.close()
