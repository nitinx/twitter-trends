# Twitter Trends

An application to determine Twitter's trending hastags in realtime - primarliy built for familiarization with streaming data, Apache Kafka and Apache Spark.  

Components:
1. Twitter Client (connects to Twitter via the Streaming API and invokes producer)
2. Kafka Producer (produces Tweets to topic "twitter_trend")
3. Kafka Consumer #1 (consumes topic "twitter_trend" and batch writes to file)
4. Kafka Consumer #2 (consumes topic "twitter_trend" and writes to socket)
5. Spark Streaming App #1 (parse tweets and determine trending hashtags, file stream)
6. Spark Streaming App #2 (parse tweets and determine trending hashtags, socket stream)
