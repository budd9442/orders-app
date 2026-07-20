import pika
import redis
import time
import json
import random
import os
import sys
import threading
import ssl

rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq-prod.rabbitmq.svc.cluster.local")
rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", "5671"))
rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", "vhost_orders")
cert_dir = os.environ.get("RABBITMQ_CERT_DIR", "/etc/rabbitmq-certs")

redis_host = os.environ.get("REDIS_HOST", "redis-orders.orders-system.svc.cluster.local")
redis_port = int(os.environ.get("REDIS_PORT", "6379"))

ca_cert = os.path.join(cert_dir, "ca.crt")
client_cert = os.path.join(cert_dir, "tls.crt")
client_key = os.path.join(cert_dir, "tls.key")

print(f"Connecting to Redis at {redis_host}:{redis_port}...", flush=True)
redis_client = None
for attempt in range(10):
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        redis_client.ping()
        print("Connected successfully to Redis!", flush=True)
        break
    except Exception as re:
        print(f"Redis connection attempt {attempt+1} failed: {re}. Retrying in 2s...", flush=True)
        time.sleep(2)

if not redis_client:
    print("Could not connect to Redis. Exiting.", flush=True)
    sys.exit(1)

def get_connection_parameters():
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)

    ssl_options = pika.SSLOptions(context)
    credentials = pika.credentials.ExternalCredentials()

    return pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host=rabbitmq_vhost,
        credentials=credentials,
        ssl_options=ssl_options,
        connection_attempts=10,
        retry_delay=3
    )

print(f"Connecting to RabbitMQ at {rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost} using mTLS certs...", flush=True)

def publisher_thread():
    time.sleep(5)
    try:
        params = get_connection_parameters()
        pub_conn = pika.BlockingConnection(params)
        pub_chan = pub_conn.channel()
        pub_chan.confirm_delivery()
        print("Publisher thread connected to RabbitMQ successfully!", flush=True)
        
        order_counter = 2000
        recent_orders = []

        while True:
            # Every 3rd cycle, publish an intentional duplicate if recent_orders exist
            if len(recent_orders) >= 3 and random.random() < 0.4:
                duplicate_id = random.choice(recent_orders)
                order_type = "digital"
                payload = json.dumps({"id": duplicate_id, "type": order_type, "timestamp": time.time(), "is_duplicate_test": True})
                print(f"[PUBLISHER] Emitting INTENTIONAL DUPLICATE order event: {duplicate_id}", flush=True)
            else:
                order_counter += 1
                order_id = f"ORD-{order_counter}"
                order_type = random.choice(["digital", "physical"])
                recent_orders.append(order_id)
                if len(recent_orders) > 10:
                    recent_orders.pop(0)
                payload = json.dumps({"id": order_id, "type": order_type, "timestamp": time.time()})
                print(f"[PUBLISHER] Emitting new order event: {order_id} ({order_type})", flush=True)

            try:
                pub_chan.basic_publish(
                    exchange="order_events",
                    routing_key=f"order.{order_type}",
                    body=payload,
                    properties=pika.BasicProperties(content_type="application/json")
                )
            except Exception as pe:
                print(f"Publish error: {pe}", flush=True)

            time.sleep(4)
    except Exception as e:
        print(f"Publisher thread error: {e}", flush=True)

pub_t = threading.Thread(target=publisher_thread, daemon=True)
pub_t.start()

# Main Consumer Loop
params = get_connection_parameters()
connection = pika.BlockingConnection(params)
channel = connection.channel()
print("Orders Consumer connected successfully to RabbitMQ over mTLS!", flush=True)

def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
        order_id = data.get("id")

        if not order_id:
            print(f"Missing order ID in payload: {data}", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Redis Atomic Deduplication: SETNX with 24h TTL
        is_new_order = redis_client.set(f"dedup:order:{order_id}", "1", nx=True, ex=86400)

        if is_new_order:
            print(f"[DEDUP MATCH: UNIQUE] Processed unique order {order_id} ({data.get('type')})", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print(f"[DEDUP MATCH: DUPLICATE SKIPPED] Order {order_id} already processed. Dropping duplicate.", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Callback processing error: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_qos(prefetch_count=100)
channel.basic_consume(queue="order_validation_q", on_message_callback=callback)

print("Orders App listening for messages on order_validation_q with Redis deduplication enabled...", flush=True)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
    connection.close()
