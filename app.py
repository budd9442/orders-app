import pika
import redis
import time
import json
import random
import os
import sys
import threading
import ssl
import uuid
from prometheus_client import start_http_server, Counter

rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq-prod.rabbitmq.svc.cluster.local")
rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", "5671"))
rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", "vhost_orders")
cert_dir = os.environ.get("RABBITMQ_CERT_DIR", "/etc/rabbitmq-certs")

redis_host = os.environ.get("REDIS_HOST", "redis-orders.orders-system.svc.cluster.local")
redis_port = int(os.environ.get("REDIS_PORT", "6379"))

ca_cert = os.path.join(cert_dir, "ca.crt")
client_cert = os.path.join(cert_dir, "tls.crt")
client_key = os.path.join(cert_dir, "tls.key")

# Prometheus Metrics Exporter
metrics_port = int(os.environ.get("METRICS_PORT", "8000"))
try:
    start_http_server(metrics_port)
    print(f"Prometheus metrics HTTP server started on port {metrics_port}", flush=True)
except Exception as me:
    print(f"Failed to start Prometheus metrics server: {me}", flush=True)

orders_processed_counter = Counter(
    "orders_processed_total",
    "Total number of order events processed by deduplicator",
    ["status", "type"]
)

print(f"Connecting to Redis at {redis_host}:{redis_port}...", flush=True)
redis_client = None
for attempt in range(15):
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

def build_ssl_params():
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)

    ssl_opts = pika.SSLOptions(context, server_hostname=rabbitmq_host)
    creds = pika.credentials.ExternalCredentials()

    return pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host=rabbitmq_vhost,
        credentials=creds,
        ssl_options=ssl_opts,
        connection_attempts=5,
        retry_delay=3,
        socket_timeout=10
    )

def create_connection(name="Client"):
    print(f"[{name}] Connecting to RabbitMQ at {rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost} over mTLS...", flush=True)
    for attempt in range(30):
        try:
            params = build_ssl_params()
            conn = pika.BlockingConnection(params)
            print(f"[{name}] Connected successfully over mTLS!", flush=True)
            return conn
        except Exception as e:
            print(f"[{name}] Connection attempt {attempt+1} failed: {repr(e)}. Retrying in 4s...", flush=True)
            time.sleep(4)
    print(f"[{name}] Could not establish connection. Exiting.", flush=True)
    sys.exit(1)

# Step 1: Establish Consumer Connection First
consumer_conn = create_connection("CONSUMER")
consumer_chan = consumer_conn.channel()

processed_unique = 0
processed_duplicate = 0
last_stats_time = time.time()

def callback(ch, method, properties, body):
    global processed_unique, processed_duplicate, last_stats_time
    try:
        data = json.loads(body.decode())
        order_id = data.get("id")
        order_type = data.get("type", "unknown")

        if not order_id:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        is_new = redis_client.set(f"dedup:order:{order_id}", "1", nx=True, ex=86400)

        if is_new:
            processed_unique += 1
            orders_processed_counter.labels(status="unique", type=order_type).inc()
        else:
            processed_duplicate += 1
            orders_processed_counter.labels(status="duplicate", type=order_type).inc()
            print(f"⚠️ [NATURAL DUPLICATE DETECTED!] Order ID: {order_id}", flush=True)

        now = time.time()
        if now - last_stats_time >= 5.0:
            rate = (processed_unique + processed_duplicate) / (now - last_stats_time)
            print(f"📊 [HIGH-THROUGHPUT STATS] Rate: {rate:.1f} msg/s | Unique: {processed_unique} | Duplicates: {processed_duplicate}", flush=True)
            last_stats_time = now

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Callback error: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

consumer_chan.basic_qos(prefetch_count=1000)
consumer_chan.basic_consume(queue="order_validation_q", on_message_callback=callback)
print("Orders Consumer listening on order_validation_q over mTLS!", flush=True)

# Step 2: Start Publisher Thread AFTER Consumer is connected
def publisher_loop():
    time.sleep(3)
    pub_conn = create_connection("PUBLISHER")
    pub_chan = pub_conn.channel()
    pub_chan.confirm_delivery()
    print("[PUBLISHER] High-throughput publisher connected over mTLS!", flush=True)

    order_counter = 0

    while True:
        try:
            order_counter += 1
            order_id = f"ORD-{uuid.uuid4().hex[:8]}-{order_counter}"
            order_type = "digital" if order_counter % 2 == 0 else "physical"
            payload = json.dumps({"id": order_id, "type": order_type, "timestamp": time.time()})

            pub_chan.basic_publish(
                exchange="order_events",
                routing_key=f"order.{order_type}",
                body=payload,
                properties=pika.BasicProperties(content_type="application/json")
            )
            time.sleep(0.005)
        except Exception as e:
            print(f"[PUBLISHER] Publish error: {e}. Reconnecting in 3s...", flush=True)
            time.sleep(3)
            try:
                pub_conn = create_connection("PUBLISHER")
                pub_chan = pub_conn.channel()
                pub_chan.confirm_delivery()
            except Exception:
                pass

pub_thread = threading.Thread(target=publisher_loop, daemon=True)
pub_thread.start()

try:
    consumer_chan.start_consuming()
except KeyboardInterrupt:
    consumer_chan.stop_consuming()
    consumer_conn.close()
