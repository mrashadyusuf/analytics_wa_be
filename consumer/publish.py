import pika
import json
def publish_to_rabbitmq(queue_name: str, message: dict):
    try:
        # Establish RabbitMQ connection
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Adjust as necessary
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