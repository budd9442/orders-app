import pika
import time
import json
import random
import os
import sys
import ssl

host = os.environ.get("RABBITMQ_HOST", "rabbitmq-prod.rabbitmq.svc.cluster.local")
port = int(os.environ.get("RABBITMQ_PORT", "5671"))
vhost = os.environ.get("RABBITMQ_VHOST", "vhost_orders")
cert_dir = os.environ.get("RABBITMQ_CERT_DIR", "/etc/rabbitmq-certs")

ca_cert = os.path.join(cert_dir, "ca.crt")
client_cert = os.path.join(cert_dir, "tls.crt")
client_key = os.path.join(cert_dir, "tls.key")

print(f"Connecting to RabbitMQ at {host}:{port}/{vhost} using mTLS certs...", flush=True)

context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
context.check_hostname = False
context.verify_mode = ssl.CERT_REQUIRED
context.load_cert_chain(certfile=client_cert, keyfile=client_key)

ssl_options = pika.SSLOptions(context)
credentials = pika.credentials.ExternalCredentials()

parameters = pika.ConnectionParameters(
    host=host,
    port=port,
    virtual_host=vhost,
    credentials=credentials,
    ssl_options=ssl_options,
    connection_attempts=10,
    retry_delay=3
)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
print("Connected successfully over mTLS with EXTERNAL authentication!", flush=True)

def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
        print(f"Received order: {data}", flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Order callback error: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_qos(prefetch_count=100)
channel.basic_consume(queue="order_validation_q", on_message_callback=callback)

print("Orders App listening for messages on order_validation_q...", flush=True)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
    connection.close()
