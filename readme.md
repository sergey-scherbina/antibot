# In-Stream processing

## The idea

Ads! Ads are everywhere these days! Even though you might not like it or even use adblocker, the market is huge and so is desire to trick it. Fraud has multiple meanings, but the most common example of fraud is bot networks -- ads are being consumed (watched, clicked) by robots, not real users, but paid in full. What's worse, most bot networks are short lived, they work for several hours at most and then got blocked, but the damage is already done.

You're the new startup 'StopBot', that does blazingly fast analytics, unlike most other companies you're able to detect those bots in a matter of minutes, or even seconds. What's more you can do this accurately just by looking at [ip address, time, url of page with banner]. Companies like Woogle, Yeahoo and some others rely on you and happy to provide you with their data.

## Stack

* Kafka Connect as ingestion tool
* Apache Kafka as a messaging backbone
* Spark Streaming for processing
* Apache Ignite/Redis as lookup service to maintain all bots
* Cassandra/AWS S3 as a storage for all incoming events, including bot events as well

### Additional requirements:

Use Scala for all spark related code.

Solution should be implemented for both DStream and Structured Streaming. For DStream please pay additional attention: DStream can’t have window over event time, it should be implemented on top of DStream. For Structured Streaming you can use watermark

We operate at internet scale, so system should be able to scale to 100k-1M of inbound messages.

Faster is better, but we have SLA of 60 seconds latency (once we have all the required data from partners to make a decision, bot should be registered in cassandra within this time).

We should not block users forever, so there should be a configurable retention period (TTL), after which bot ip address is back to list of good guys.

There is no guarantee that data about some ip will come in the same order as event happened, what's more some partners may delay events for tens of seconds
 
## Design

Data flow is simple and linear:

![Design diagram](diagram.jpg "Design diagram")

Where Redis is used as a cache, Cassandra or AWS S3 is used as permanent storage for all incoming events.

Come up with schema design for Cassandra/AWS S3 and have solid reason of proposed solution. Take into account data coming in streaming manner.

## Bot detection

Bot detection algorithm: more than 20 requests per 10 seconds for single IP. IP should be whitelisted in 10 minutes if bot detection condition is not matched. It means IP should be deleted from Redis once host stops suspicious activity.

System should handle up to 200 000 events per minute and collect user click rate and transitions for the last 10 minutes.

Please pay additional attention to the way how your window is implemented on DStream. For Structured Streaming you can use watermark over event_time, while for DStream it’s not available (window is built on ingestion time), so it should be implemented on top of DStream. This is the most tricky part of the project.

## Data formats

All data is supplied in form of (multiple) files that got dumped on filesystem, each event is JSON, each JSON on it's own line, with the above mentioned fields:

```
{
"type": "click",
"ip": "127.0.0.1",
"event_time": "1500028835",
"url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"
}
```

Fields to store in Cassandra/AWS S3:

```
{
"type": "click",
"ip": "127.0.0.1",
“is_bot”: true,
"event_time": "1500028835",
"url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"
}
```
