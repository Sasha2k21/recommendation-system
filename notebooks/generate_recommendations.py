from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode

spark = SparkSession.builder \
    .appName("Generate Movie Recommendations") \
    .getOrCreate()

ratings = spark.read.csv("data/ratings.csv", header=True, inferSchema=True)

# Train ALS model
als = ALS(
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True,
    implicitPrefs=False
)
model = als.fit(ratings)

# Generate top 10 movie recommendations for each user
user_recs = model.recommendForAllUsers(10)

# Flatten the recommendations
flat_recs = user_recs.withColumn("rec", explode("recommendations")) \
    .select("userId", "rec.movieId", "rec.rating")

# Save to Parquet
flat_recs.write.mode("overwrite").parquet("data/recommendations.parquet")

# Load movie titles
movies = spark.read.csv("data/movies.csv", header=True, inferSchema=True)

# Join to get titles
recs_with_titles = flat_recs.join(movies, on="movieId", how="left") \
    .select("userId", "title", "rating") \
    .withColumnRenamed("rating", "prediction")

# Save to Parquet
recs_with_titles.write.mode("overwrite").parquet("data/recommendations.parquet")


# Stop Spark
spark.stop()
