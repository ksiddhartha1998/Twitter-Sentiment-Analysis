import java.util.Properties
import org.apache.spark.SparkConf
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object TwitterStreaming {
  def main(args: Array[String]): Unit = {
    // Checking for arguments
    if(args.length < 5) {
      println("Arguments : 1.Kafka Topic Name 2.Twitter API Key 3.Twitter API Secret 4.access Token 5.Access Token Secret")
    }
    // Setting up Spark config
    val sparkConf = new SparkConf().setAppName("twitter-sentiment").setMaster("local[4]").set("spark.driver.host", "localhost")

    // Extracting Args
    val topicName = args(0)
    val twitterAPIKey = args(1)
    val twitterAPISecret = args(2)
    val accessToken = args(3)
    val accessTokenSecret = args(4)

    // Connecting to Twitter API
    val configurationBuilder = new ConfigurationBuilder
    configurationBuilder.setDebugEnabled(true).setOAuthConsumerKey(twitterAPIKey).setOAuthConsumerSecret(twitterAPISecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    val auth = new OAuthAuthorization(configurationBuilder.build)

    // Extracting tweets based on certain keywords
    val tweetTopicSearchKeywords = Seq("Covid", "Bitcoin", "India", "POTUS", "Vaccine")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))
    val tweetStream = TwitterUtils.createStream(streamingContext, Some(auth), tweetTopicSearchKeywords)

    // Filtering tweets based on language
    val engTweets = tweetStream.filter(_.getLang() == "en")
    val tweets = engTweets.map(x => x.getText())

    // Iterating through tweets and sending data to kafka topic
    tweets.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionIterator =>
        val properties = new Properties()
        // Kafka topic local host
        val bootstrap = "localhost:9092"
        // Setting up properties for Kafka producer
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("bootstrap.servers", bootstrap)
        val producer = new KafkaProducer[String, String](properties)
        partitionIterator.foreach { y =>
          val words = y.toString()
          // Analyzing the sentiment of the tweet
          val sentiment = AnalyzeSentiment.getSentiment(words).toString()
          // Creating data to send to kafka topic
          val data = new ProducerRecord[String, String](topicName, "sentiment", sentiment + ":" +  words)
          producer.send(data)
        }
        producer.flush()
        producer.close()
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
