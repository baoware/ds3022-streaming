"""Bluesky posts (Jetstream) -> Kafka topic "bluesky_posts".

  python producer.py              # PUSH: new posts and deletes from the Jetstream WebSocket
  python producer.py history 60   # replay the last 60 minutes first (Jetstream cursor), then carry on live

Adapted from the class quakes_producer.py (itself from Professor Magee's stream-seismic.py).
"""
import asyncio
import json
import logging
import sys
import time

from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky_posts"
# posts only: likes, follows and reposts are filtered out by Jetstream before they reach me
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
LOG_EVERY = 1000  # about one log line every 20 s at 50 posts per second


# function to send one message to kafka keyed by the account id
def send(producer, message):
    """One Kafka record per message. Key = did (the account), so every event
    of one account goes to the same partition, in order: a post's create before its delete."""
    did = message["data"].get("did")  # .get(): no crash if missing
    if did is None:
        logging.warning("No did: sent without a key")
    producer.produce(topic=TOPIC, key=did, value=json.dumps(message))
    return did


# function to read the jetstream websocket forever and reconnect when it drops
async def live(producer, minutes_back=0):
    """PUSH over a WebSocket, with a reconnect loop. Jetstream can replay from a
    cursor (time_us), so after a drop I reconnect from the last message I sent."""
    started_us = int(time.time() * 1e6)
    # no cursor means start from now, otherwise start minutes_back ago
    cursor = started_us - minutes_back * 60 * 1_000_000 if minutes_back else None
    sent = skipped = 0
    while True:
        url = WSS_URL + (f"&cursor={cursor}" if cursor else "")
        try:
            ws = await websocket_connect(url, ping_interval=15)
            logging.info("Connected%s", f" from cursor {cursor}" if cursor else "")
            while True:
                text = await ws.read_message()
                if text is None:
                    logging.warning("The server closed the connection")
                    break
                try:
                    data = json.loads(text)
                    # replayed messages are older than the moment I started
                    action = "history" if data.get("time_us", started_us) < started_us else "live"
                    send(producer, {"action": action, "data": data})
                except (ValueError, KeyError, TypeError, AttributeError) as e:
                    skipped += 1
                    logging.warning("Skipped a message I could not read (%d so far): %r", skipped, e)
                    continue
                # remember where I am so a reconnect picks up here
                cursor = data.get("time_us", cursor)
                sent += 1
                if sent % LOG_EVERY == 0:
                    logging.info("%d messages sent to %s (%d skipped)", sent, TOPIC, skipped)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s (%d sent so far)", sent)
        await asyncio.sleep(5)


# function to start the producer, live or replaying the last n minutes
def main():
    # history N replays the last N minutes, anything else is live from now
    minutes = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[1] == "history" else 0
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer, minutes))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
