import pika
import json
import os

def publish_to_rabbitmq(queue_name: str, message: dict):
    try:
        # Fetch RabbitMQ connection parameters from the environment
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
        rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')

        # Set up RabbitMQ credentials using the environment values
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        connection_params = pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, '/', credentials)

        # Establish RabbitMQ connection using BlockingConnection
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Declare the queue (ensures the queue exists)
        channel.queue_declare(queue=queue_name, durable=True)

        # Publish the message to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )

        # Close the RabbitMQ connection
        connection.close()

        print(f"Message successfully published to RabbitMQ queue: {queue_name}")
    except Exception as e:
        print(f"Failed to publish message to RabbitMQ: {str(e)}")
        raise
