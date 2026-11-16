# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:** Bao Duong
**Date:** Thu 12 Nov 2026

## 1. The source
- Source and URL: Bluesky posts through Jetstream, `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: none
- Limits I know about (rate limits, paging, reconnects): no rate limit, but the stream is fast (about 50 to 67 posts per second). If the connection drops I reconnect with `?cursor=<time_us>` of the last message I sent, which replays from there. Replay only reaches back about 37 hours (I asked for 48 h and 96 h and both started 36.9 h back). Deletes arrive without a `record`, and about 0.8% of messages are not commits (no `commit` field at all).

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "did": "did:plc:jxfu5lqshqnvg6vshmzug7ge",
  "time_us": 1794489973462776,
  "kind": "commit",
  "commit": {
    "rev": "3n24l2ql7ej23",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3n24l2pwc322p",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-12T13:26:12.565Z",
      "langs": ["en"],
      "reply": {
        "parent": {"uri": "at://did:plc:vf44424hyqwlbukqzeq2ntzg/app.bsky.feed.post/3n24ktu5hc22j"},
        "root": {"uri": "at://did:plc:rq7xbu4hwwgqjbssglnjxv4h/app.bsky.feed.post/3n22kqhd7fc2z"}
      },
      "text": "(shortened)"
    },
    "cid": "bafyreifp4i7rv3bhflnoeomb6u4omhmgzjmmivon6sznr4biebobvskll4"
  }
}
```

## 3. Volume maths
- Measured rate (from the probe): 48.96 messages per second (60 s probe on Thu 12 Nov; 66.62 per second on Thu 05 Nov)
- Live capture needed for 50,000: about 17 minutes
- History / backfill available? How many days? About 1.5 days, through `?cursor=<time_us>` (tested on Thu 12 Nov, the replay starts at most 36.9 h back)
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): I started late, on Thu 12 Nov. I run the producer live for 45 minutes, which is about 130,000 messages at the measured rate, so 2.6 times the target. If the stream is slow or the producer fails, I backfill with the cursor from up to 36 hours ago.

## 4. Topic design
- Topic name: `bluesky_posts`                          (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: `did` (the account id)
- Why this key (ordering, spread across partitions): every event of one account goes to the same partition in order, so a post's create always comes before its delete. Millions of accounts post, so the keys spread evenly over the 3 partitions and no partition gets hot.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) The same partition and offset. This happens when the lander writes a file and crashes before it stores its offsets, so it reads those messages again.
- What makes two records **the same thing** (the same trade, post, aircraft position)? The same `did` + `commit.rkey` (one account, one post). This happens when the producer reconnects with a cursor and Jetstream replays events it already sent.
- If the same thing can arrive in several versions, which one do I keep? One row per `did` + `rkey`, the one with the latest `time_us` (then the latest offset). A post that was created and then deleted keeps `delete` as its latest operation, so my marts count posts with `operation = 'create'`.

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: How does the share of posts that are replies differ across the five most common languages on Bluesky? (a comparison)
- Changed on Mon 16 Nov from the 12 Nov draft ("How does the share of replies versus original posts change by hour of the day for the five most common languages?"). By hour needed more hours of data than I planned to collect, so I kept the language comparison and treat time of day as a limitation.

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
