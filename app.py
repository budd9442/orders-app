import pika
import time
import json
import random
import os
import sys
import threading
import ssl

host = os.environ.get("RABBITMQ_HOST", "rabbitmq-prod.rabbitmq.svc.cluster.local")
port = int(os.environ.get("RABBITMQ_PORT", "5671"))
vhost = os.environ.get("RABBITMQ_VHOST", "vhost_orders")
cert_dir = os.environ.get("RABBITMQ_CERT_DIR", "/etc/rabbitmq-certs")

ca_cert = os.path.join(cert_dir, "ca.crt")
client_cert = os.path.join(cert_dir, "tls.crt")
client_key = os.path.join(cert_dir, "tls.key")

def get_connection_parameters():
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)

    ssl_options = pika.SSLOptions(context)
    credentials = pika.credentials.ExternalCredentials()

    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials,
        ssl_options=ssl_options,
        connection_attempts=5,
        retry_delay=3
    )

print(f"Connecting to RabbitMQ at {host}:{port}/{vhost} using mTLS certs...", flush=True)

def publisher_thread():
    time.sleep(3)
    try:
        params = get_connection_parameters()
        pub_conn = pika.BlockingConnection(params)
        pub_chan = pub_conn.channel()
        pub_chan.confirm_delivery()
        print("Publisher thread connected successfully!", flush=True)
        order_id = 1000
        while True:
            order_id += 1
            order_type = random.choice(["digital", "physical"])
            payload = json.dumps({"id": f"ORD-{order_id}", "type": order_type, "timestamp": time.time()})
            try:
                pub_chan.basic_publish(
                    exchange="order_events",
                    routing_key=f"order.{order_type}",
                    body=payload,
                    properties=pika.BasicProperties(content_type="application/json")
                )
                print(f"Published order {order_id} ({order_type})", flush=True)
            except Exception as pe:
                print(f"Publish error: {pe}", flush=True)
            time.sleep(5)
    except Exception as e:
        print(f"Publisher thread error: {e}", flush=True)

pub_t = threading.Thread(target=publisher_thread, daemon=True)
pub_t.start()

# Main Consumer loop
params = get_connection_parameters()
connection = pika.BlockingConnection(params)
channel = connection.channel()
print("Consumer connected successfully!", flush=True)

def callback(ch, method, properties, body):
    cycle_time = time.time() % 60
    if cycle_time < 30:
        time.sleep(0.04)
    else:
        time.sleep(0.001)

    try:
        data = json.loads(body.decode())
        if data.get("type") in ["digital", "physical"] and "id" in data:
            print(f"Processed order: {data.get('id')}", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            raise ValueError("Invalid order type or missing ID")
    except Exception as e:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_qos(prefetch_count=100)
channel.basic_consume(queue="order_validation_q", on_message_callback=callback)
print("Consumer starting consumption...", flush=True)
channel.start_consuming()
