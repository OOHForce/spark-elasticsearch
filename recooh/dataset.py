# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 14:45:28 2018

@author: beersoccer
"""

import warnings
import re
import tmdbsimple as tmdb

from tmdbsimple import APIKeyError
from time import time
from os import path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from IPython.display import Image
from IPython.display import display


class Dataset:

    def __init__(self, data_path=None):

        if data_path is None:
            warnings.warn("Data path not specified, using the default one.")
            self.data_path = 'd:/Machine_Learning/Dataset/ml-latest-small/'
        else:
            self.data_path = data_path

    def get_spark(self):

        spark = SparkSession.builder.\
            master('local').\
            appName('Recommendation').\
            config('spark.driver.extraClassPath', 'D:/elasticsearch/'\
                   'elasticsearch-hadoop-5.3.0/dist/'\
                   'elasticsearch-spark-20_2.11-5.3.0.jar').\
            config('spark.driver.memory', '2g').\
            getOrCreate()
        print("Get an active SparkContext instance successfully.")

        return spark

    def stop_spark(self, spark):

        if spark is not None:
            spark.stop()
        print("Stop the active SparkContext instance successfully.")

    def load_ratings(self, spark, file_name='ratings.csv'):

        struct = StructType([StructField('userId', IntegerType(), True),
                             StructField('movieId', IntegerType(), True),
                             StructField('rating', DoubleType(), True),
                             StructField('timestamp', IntegerType(), True)])
        start = time()
        ratings = self.read_csv(file_name, struct, spark)
        ratings = ratings.select(ratings.userId, ratings.movieId,
                                 ratings.rating,
                                 (ratings.timestamp.cast('long') * 1000).\
                                 alias('timestamp'))
        duration = time() - start
        print("Ratings data loaded in %.3f seconds." % duration)

        return ratings, duration

    def load_movies(self, spark, file_name='movies.csv', links_file='links.csv'):

        # define a UDF to convert the raw genres string to an array of genres
        # and lowercase
        extract_genres = udf(lambda x: x.lower().split("|"), ArrayType(StringType()))

        # define a UDF to extract the release year from the title,
        # and return the new title and year in a struct type
        def extract_year_fn(title):
            result = re.search("\(\d{4}\)", title)
            try:
                if result:
                    group = result.group()
                    year = group[1:-1]
                    start_pos = result.start()
                    title = title[:start_pos-1]
                    return (title, year)
                else:
                    return (title, 1970)
            except Exception as err:
                print("Fail to extract year from %s." % title)

        extract_year = udf(extract_year_fn,\
                           StructType([StructField("title", StringType(), True),\
                                       StructField("release_date", StringType(), True)]))

        struct = StructType([StructField('movieId', IntegerType(), True),
                             StructField('title', StringType(), True),
                             StructField('genres', StringType(), True)])
        start = time()
        movies = self.read_csv(file_name, struct, spark)
        movies = movies.select("movieId",
                               extract_year("title").title.alias("title"),
                               extract_year("title").release_date.\
                               alias("release_date"),
                               extract_genres("genres").alias("genres"))

        links, dur = self.load_links(spark, links_file)
        movies = movies.join(links, movies.movieId == links.movieId).\
            select(movies.movieId, movies.title, movies.release_date,
                   movies.genres, links.tmdbId)

        duration = time() - start
        print("Movies data loaded in %.3f seconds." % duration)

        return movies, duration

    def load_links(self, spark, file_name='links.csv'):

        struct = StructType([StructField('movieId', IntegerType(), True),
                             StructField('imdbId', IntegerType(), True),
                             StructField('tmdbId', IntegerType(), True)])
        start = time()
        links = self.read_csv(file_name, struct, spark)
        duration = time() - start

        return links, duration

    def load_tags(self, spark, file_name='tags.csv'):

        struct = StructType([StructField('userId', IntegerType(), True),
                             StructField('movieId', IntegerType(), True),
                             StructField('tag', StringType(), True),
                             StructField('timestamp', IntegerType(), True)])
        start = time()
        tags = self.read_csv(file_name, struct, spark)
        duration = time() - start
        print("Tags data loaded in %.3f seconds." % duration)

        return tags, duration

    def read_csv(self, file_name, struct=None, spark=None):

        if file_name is None:
            raise ValueError("Please specify the data file\'s name.")

        # Data file read exception.
        f = self.data_path + file_name
        if not path.isfile(path.expanduser(f)):
            raise ValueError("Data file " + str(f) + " does not exist.")

        if spark is None:
            raise ValueError("Please get the active Spark instance.")

        # Specify schema explicitly to avoid going through the entire data once.
        if struct is None:
            df = spark.read.csv(self.data_path + file_name,
                                header=True, inferSchema=True)
        else:
            df = spark.read.csv(self.data_path + file_name,
                                schema=struct, header=True)

        return df

def _tmdb_test(movies):

    try:
        # replace this variable with your actual TMdb API key
        tmdb.API_KEY = '338d7cb2fc8189133676369b3a093abc'
        print("Successfully imported tmdbsimple!")
        # base URL for TMDB poster images
        IMAGE_URL = 'https://image.tmdb.org/t/p/w500'
        tmdb_id = movies.first().tmdbId
        movie_info = tmdb.Movies(tmdb_id).info()
        movie_poster_url = IMAGE_URL + movie_info['poster_path']
        display(Image(movie_poster_url, width=200))
    except APIKeyError as err:
        print("The API Key to access TMDB is wrong.")
    except Exception as err:
        print("Can\'t display movie posters due to %s" % str(err))

def _test():

    ds = Dataset()
    spk = ds.get_spark()
    #conf = spk.conf
    #print(conf.get('spark.driver.extraClassPath'))
    #print(conf.get('spark.driver.memory'))

    r, d = ds.load_ratings(spark=spk)
    r.show(5)

    m, d = ds.load_movies(spark=spk)
    m.show(5)
    #_tmdb_test(m)

    t, d = ds.load_tags(spark=spk)
    t.show(5)

    ds.stop_spark(spk)

if __name__ == '__main__':
    _test()
