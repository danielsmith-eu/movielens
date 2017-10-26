MovieLens task

Run `sbt assembly` to create fat JAR.

Parameters: <master> <movies-input> <ratings-input> <users-input> <parquet-output>, e.g. master: 'local[8]', path to movies.dat, ratings.dat and users.dat, output path parquet files


Example spark-submit command line to run locally:

./bin/spark-submit \
  --class eu.danielsmith.newday.Movielens \
  --master local[8] \
  --deploy-mode client \
  file:///User/daniel/git/newday/target/scala-2.10/newday-assembly-1.0.jar \
  local[8] ml-1m/movies.dat ml-1m/ratings.dat ml-1m/users.dat /tmp/newday-output

