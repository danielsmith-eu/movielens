package eu.danielsmith.newday

import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Movielens extends App {

  // handle input arguments
  if (args.length != 5) {
    System.err.println("Parameters: <master> <movies-input> <ratings-input> <users-input> <parquet-output>, e.g. master: 'local[8]', path to movies.dat, ratings.dat and users.dat, output path parquet files")
    System.exit(1) // error code
  }
  val master = args(0)
  val moviesCsv = args(1)
  val ratingsCsv = args(2)
  val usersCsv = args(3)
  val parquetOutput = args(4)

  // configure spark to run locally
  val conf = new SparkConf()
  conf.setMaster(master)
  conf.setAppName("MovieLens Ratings")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val moviesRDD = sc.textFile(moviesCsv).map(line => {
    // MovieID::Title::Genres
    val Array(movieId, title, genres) = line.split("::")
    Row(movieId.toInt, title, genres)
  })

  val ratingsRDD = sc.textFile(ratingsCsv).map(line => {
    // UserID::MovieID::Rating::Timestamp
    val Array(userId, movieId, rating, timestamp) = line.split("::")
    Row(userId.toInt, movieId.toInt, rating.toInt, timestamp)
  })

  val usersRDD = sc.textFile(usersCsv).map(line => {
    // UserID::Gender::Age::Occupation::Zip-code
    val Array(userId, gender, age, occupation, zipCode) = line.split("::")
    Row(userId.toInt, gender, age.toInt, occupation, zipCode)
  })

  val moviesSchema = StructType(List(
    StructField("movieId", IntegerType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("genres", StringType, nullable = false)
  ))

  val ratingsSchema = StructType(List(
    StructField("userId", IntegerType, nullable = false),
    StructField("movieId", IntegerType, nullable = false),
    StructField("rating", IntegerType, nullable = false),
    StructField("timestamp", StringType, nullable = false)
  ))

  val usersSchema = StructType(List(
    StructField("userId", IntegerType, nullable = false),
    StructField("gender", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("occupation", StringType, nullable = false),
    StructField("zipCode", StringType, nullable = false)
  ))

  sqlContext.createDataFrame(moviesRDD, moviesSchema).registerTempTable("movies")
  sqlContext.createDataFrame(ratingsRDD, ratingsSchema).registerTempTable("ratings")
  sqlContext.createDataFrame(usersRDD, usersSchema).registerTempTable("users")

  sqlContext.sql(
    """SELECT movies.movieId AS movieId,
      |       movies.title AS title,
      |       movies.genres AS genres,
      |       min(ratings.rating) AS minRating,
      |       max(ratings.rating) AS maxRating,
      |       avg(ratings.rating) AS avgRating
      |       FROM movies
      |       JOIN ratings on (movies.movieId = ratings.movieId)
      |       GROUP BY movies.movieId, movies.title, movies.genres""".stripMargin).registerTempTable("movieRatings")

  sqlContext.table("movies").write.parquet(parquetOutput + "-movies")
  sqlContext.table("ratings").write.parquet(parquetOutput + "-ratings")
  sqlContext.table("users").write.parquet(parquetOutput + "-users")
  sqlContext.table("movieRatings").write.parquet(parquetOutput + "-movieRatings")

  sc.stop
}
