# Real-Time Tweet Sentiment Analysis with Docker, Kafka and Spark Streaming
This project is the continuation of a [first one](https://github.com/Wazzabeee/twitter-sentiment-analysis-pyspark) where I compared several classification algorithms implemented in PySpark on a sentiment analysis task.

In this repository, you will find the implementation of an ETL process for sentiment analysis of tweets in real time. The idea here is to use the best model tested offline and deploy it online for real-time analysis. For this, I used Docker, Apache Kafka and Spark Streaming. The results of this analysis can be displayed in a console, saved locally, saved to a MongoDB database or saved to a data lake such as Delta Lake.

For more details on how this project works, how to create Topic Kafka, etc, I invite you to read [my article on Medium](https://medium.com/towards-artificial-intelligence/real-time-sentiment-analysis-with-docker-kafka-and-spark-streaming-952c06549de1) about this project. I detail there all the steps of the implementation and the execution of the program.

<img src="images/flow.png"/>
