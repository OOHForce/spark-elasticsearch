# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 17:20:49 2018

@author: beersoccer
"""

from time import time
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from esoperation import ESOperation


class TrainModel:

    def __init__(self):

        pass

    def get_esoperation(self):

        esop = ESOperation()

        return esop

    def prepare_data(self, esoperation):

        ds = esoperation.get_dataset()
        esoperation.save(ds, 'ratings')
        esoperation.save(ds, 'movies')

    def train(self, esoperation, user_c='userId', item_c='movieId', rating_c='rating',
              reg_p=0.01, rank=20, seed=12):

        self.prepare_data(esoperation)
        ds = esoperation.get_dataset()
        spk = ds.get_spark()
        ratings_es, d = esoperation.read(spk, 'ratings')
        start = time()

        als = ALS(userCol=user_c, itemCol=item_c, ratingCol=rating_c,
                  regParam=reg_p, rank=rank, seed=seed)
        model = als.fit(ratings_es)

        dur = time() - start
        print("The model is trained in %3.f seconds." % dur)

        self.save_vector(model, esoperation.idx)
        ds.stop_spark(spk)

        return dur

    def save_vector(self, model, index):

        def convert_vector(x):
            '''Convert a list or numpy array to delimited token filter format'''
            return " ".join(["%s|%s" % (i, v) for i, v in enumerate(x)])

        def vector_to_struct(x, version, ts):
            '''
            Convert a vector to a SparkSQL Struct with string-format vector
            and version fields
            '''
            return (convert_vector(x), version, ts)

        vector_struct = udf(vector_to_struct,
                            StructType([StructField("factor", StringType(), True),
                                        StructField("version", StringType(), True),
                                        StructField("timestamp", LongType(), True)]))

        start = time()
        ver = model.uid
        ts = unix_timestamp(current_timestamp())

        item_vectors = model.itemFactors.\
            select("id", vector_struct("features", lit(ver), ts).alias("@model"))
        user_vectors = model.userFactors.\
            select("id", vector_struct("features", lit(ver), ts).alias("@model"))

        # write data to ES, use:
        # - "id" as the column to map to ES movie id
        # - "update" write mode for ES, since you want to update new fields only
        # - "append" write mode for Spark
        item_vectors.write.format('es').\
            option('es.mapping.id', 'id').\
            option('es.write.operation', 'update').\
            save(index + '/' + 'movies', mode='append')

        # write data to ES, use:
        # - "id" as the column to map to ES movie id
        # - "index" write mode for ES, since you have not written to the user index previously
        # - "append" write mode for Spark
        user_vectors.write.format('es').\
            option('es.mapping.id', 'id').\
            option('es.write.operation', 'index').\
            save(index + '/' + 'users', mode='append')

        dur = time() - start
        print('Save trained feature vectors into Elasticsearch in %.3f seconds.' % dur)

        return dur


def _test_train(esop):

    print("Search for a particular movie from the updated Elasticsearch index.")
    print(esop.es.search(index=esop.idx, doc_type="movies",
                         q="star wars phantom menace", size=1)['hits']['hits'][0])


def _test():

    trmd = TrainModel()
    esop = trmd.get_esoperation()
    trmd.train(esoperation=esop)

    _test_train(esop)


if __name__ == '__main__':
    _test()
