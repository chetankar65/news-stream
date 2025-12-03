from flask import Flask, Response, render_template
from kafka import KafkaConsumer
import threading
import queue
import json
import time
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "finance-news")
KAFKA_GROUP  = os.environ.get("KAFKA_GROUP", "flask-web-consumer")
AUTO_OFFSET = os.environ.get("KAFKA_AUTO_OFFSET", "latest")

app = Flask(__name__)

msg_queue = queue.Queue(maxsize=1000)

def kafka_consumer_thread():
    """
    Background thread that consumes from Kafka and pushes messages into msg_queue.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=KAFKA_GROUP,
        auto_offset_reset=AUTO_OFFSET,
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8", errors="ignore")
    )

    print(f"[kafka] Consumer started for topic '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP} (offset={AUTO_OFFSET})")
    for msg in consumer:
        data = msg.value
        try:
            parsed = json.loads(data)
            data = json.dumps(parsed, ensure_ascii=False)  # normalized JSON string
        except Exception:
            pass

        try:
            msg_queue.put_nowait(data)
        except queue.Full:
            try:
                _ = msg_queue.get_nowait()  # drop oldest
                msg_queue.put_nowait(data)
            except queue.Empty:
                pass

# Start consumer thread when app starts
consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
consumer_thread.start()

@app.route("/")
def index():
    return render_template("index.html")

def event_stream():
    while True:
        try:
            msg = msg_queue.get()  # blocking
            yield f"data: {msg}\n\n"
        except GeneratorExit:
            break
        except Exception as e:
            time.sleep(0.5)

@app.route("/stream")
def stream():
    # Return the stream response with the proper content type for SSE
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == "__main__":
    # Start Flask app
    app.run(host="0.0.0.0", port=5000, threaded=True)
