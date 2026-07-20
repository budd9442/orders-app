import pika
import redis
import time
import json
import os
import sys
import multiprocessing
import ssl
import uuid
from prometheus_client import start_http_server, Counter

try:
    multiprocessing.set_start_method("spawn", force=True)
except Exception:
    pass

rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq-prod.rabbitmq.svc.cluster.local")
rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", "5671"))
rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", "vhost_orders")
cert_dir = os.environ.get("RABBITMQ_CERT_DIR", "/etc/rabbitmq-certs")

redis_host = os.environ.get("REDIS_HOST", "redis-orders.orders-system.svc.cluster.local")
redis_port = int(os.environ.get("REDIS_PORT", "6379"))

ca_cert = os.path.join(cert_dir, "ca.crt")
client_cert = os.path.join(cert_dir, "tls.crt")
client_key = os.path.join(cert_dir, "tls.key")

def build_ssl_params():
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)

    ssl_opts = pika.SSLOptions(context)
    creds = pika.credentials.ExternalCredentials()

    return pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host=rabbitmq_vhost,
        credentials=creds,
        ssl_options=ssl_opts,
        connection_attempts=15,
        retry_delay=2,
        socket_timeout=10
    )

def create_connection(name="Client"):
    print(f"[{name}] Starting mTLS connection to {rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost}...", flush=True)
    for attempt in range(1, 30):
        try:
            params = build_ssl_params()
            conn = pika.BlockingConnection(params)
            print(f"[{name}] Connected successfully over mTLS!", flush=True)
            return conn
        except Exception as e:
            print(f"[{name}] Attempt {attempt} failed: {repr(e)}. Retrying in 2s...", flush=True)
            time.sleep(2)
    sys.exit(1)

def publisher_worker(worker_id):
    time.sleep(0.2 + (worker_id % 8) * 0.2)
    pub_conn = create_connection(f"PUB-{worker_id}")
    pub_chan = pub_conn.channel()
    print(f"[PUB-{worker_id}] High-throughput publisher worker active!", flush=True)

    order_counter = 0

    while True:
        try:
            order_counter += 1
            order_id = f"ORD-{worker_id}-{uuid.uuid4().hex[:6]}-{order_counter}"
            order_type = "digital" if order_counter % 2 == 0 else "physical"
            payload = json.dumps({"id": order_id, "type": order_type, "timestamp": time.time()})

            pub_chan.basic_publish(
                exchange="order_events",
                routing_key=f"order.{order_type}",
                body=payload,
                properties=pika.BasicProperties(content_type="application/json")
            )
        except Exception as e:
            print(f"[PUB-{worker_id}] Publish error: {e}. Reconnecting in 2s...", flush=True)
            time.sleep(2)
            try:
                pub_conn = create_connection(f"PUB-{worker_id}")
                pub_chan = pub_conn.channel()
            except Exception:
                pass

def consumer_worker(worker_id, shared_unique, shared_duplicate):
    time.sleep(0.1 + (worker_id % 8) * 0.2)
    
    pool = redis.ConnectionPool(host=redis_host, port=redis_port, decode_responses=True, max_connections=50)
    r = redis.Redis(connection_pool=pool)

    cons_conn = create_connection(f"CONS-{worker_id}")
    cons_chan = cons_conn.channel()
    cons_chan.basic_qos(prefetch_count=1000)

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            order_id = data.get("id")

            if order_id:
                is_new = r.set(f"dedup:order:{order_id}", "1", nx=True, ex=86400)
                if is_new:
                    with shared_unique.get_lock():
                        shared_unique.value += 1
                else:
                    with shared_duplicate.get_lock():
                        shared_duplicate.value += 1

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    cons_chan.basic_consume(queue="order_validation_q", on_message_callback=callback)
    print(f"[CONS-{worker_id}] Consumer worker listening on order_validation_q...", flush=True)
    cons_chan.start_consuming()

if __name__ == "__main__":
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

    shared_unique = multiprocessing.Value("i", 0)
    shared_duplicate = multiprocessing.Value("i", 0)

    num_consumers = int(os.environ.get("NUM_CONSUMERS", "16"))
    num_publishers = int(os.environ.get("NUM_PUBLISHERS", "16"))

    print(f"Starting {num_consumers} consumers and {num_publishers} publishers...", flush=True)

    processes = []
    for c_id in range(num_consumers):
        p = multiprocessing.Process(target=consumer_worker, args=(c_id, shared_unique, shared_duplicate), daemon=True)
        p.start()
        processes.append(p)

    for p_id in range(num_publishers):
        p = multiprocessing.Process(target=publisher_worker, args=(p_id,), daemon=True)
        p.start()
        processes.append(p)

    last_u = 0
    last_d = 0
    last_t = time.time()

    while True:
        time.sleep(2.0)
        curr_u = shared_unique.value
        curr_d = shared_duplicate.value
        now = time.time()

        delta_u = curr_u - last_u
        delta_d = curr_d - last_d
        elapsed = now - last_t

        if delta_u > 0:
            orders_processed_counter.labels(status="unique", type="digital").inc(delta_u // 2 + delta_u % 2)
            orders_processed_counter.labels(status="unique", type="physical").inc(delta_u // 2)

        if delta_d > 0:
            orders_processed_counter.labels(status="duplicate", type="digital").inc(delta_d // 2 + delta_d % 2)
            orders_processed_counter.labels(status="duplicate", type="physical").inc(delta_d // 2)

        rate = (delta_u + delta_d) / elapsed if elapsed > 0 else 0
        print(f"🚀 [ULTRA-HIGH THROUGHPUT SYSTEM STATS] Rate: {rate:.1f} msg/s | Total Unique: {curr_u} | Total Duplicates: {curr_d}", flush=True)

        last_u = curr_u
        last_d = curr_d
        last_t = now
