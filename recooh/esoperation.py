# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 10:44:28 2018

@author: beersoccer
"""

from time import time
from elasticsearch import Elasticsearch
from dataset import Dataset


class ESOperation:
    
    def __init__(self):
        
        self.idx = 'recooh'
        self.es = Elasticsearch()
        self.create_index()
        
    def get_dataset(self):
        
        ds = Dataset()
        
        return ds
    
    def save(self, data_set, data_type=None):
        
        if data_type is None:
            print("Please specify the data type to be saved into Elasticsearch.")
            return -1
        
        if data_set is None:
            print("Please create a Dataset instance to operate data.")
            return -2
        
        spk = data_set.get_spark()
        if data_type == 'ratings':
            r, d = data_set.load_ratings(spark=spk)
            start = time()
            r.write.format('es').save(self.idx + '/' + data_type)
        elif data_type == 'movies':
            m, d = data_set.load_movies(spark=spk)
            start = time()
            m.write.format('es').option('es.mapping.id', 'movieId').\
                save(self.idx + '/' + data_type)
        
        dur = time() - start
        print("Save %s data into Elasticsearch in %.3f seconds." % (data_type, dur))
        data_set.stop_spark(spk)
        
        return dur
    
    def read(self, spark, data_type=None):
        
        if data_type is None:
            print("Please specify the data type to be readed from Elasticsearch.")
            return -1
        
        if spark is None:
            print("Please get the active Spark instance.")
            return -2
        
        start = time()
        
        if data_type == 'ratings':
            ratings_es = spark.read.format('es').load(self.idx + '/' + data_type)
        elif data_type == 'movies':
            pass
        
        dur = time() - start
        print("Read %s data from Elasticsearch in %3.f seconds." % (data_type, dur))
        
        return ratings_es, dur
    
    def create_index(self):
        
        if self.es.indices.exists(index=self.idx):
            self.es.indices.delete(index=self.idx)
            print("Delete the exsiting recommendation index in Elasticsearch.")
        
        index_body = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        # this configures the custom analyzer we need to parse
                        # vectors such that the scoring plugin will work correctly
                        "payload_analyzer": {
                            "type": "custom",
                            "tokenizer":"whitespace",
                            "filter":"delimited_payload_filter"
                        }
                    }
                }
            },
            "mappings": {
                "ratings": {
                  # this mapping definition sets up the fields for the rating
                  # events
                  "properties": {
                        "userId": {
                            "type": "integer"
                        },
                        "movieId": {
                            "type": "integer"
                        },
                        "rating": {
                            "type": "double"
                        },
                        "timestamp": {
                            "type": "date"
                        }
                    }  
                },
                "users": {
                    # this mapping definition sets up the metadata fields for 
                    # the users
                    "properties": {
                        "userId": {
                            "type": "integer"
                        },
                        "@model": {
                            # this mapping definition sets up the fields for user
                            # factor vectors of our model
                            "properties": {
                                "factor": {
                                    "type": "text",
                                    "term_vector": "with_positions_offsets_payloads",
                                    "analyzer" : "payload_analyzer"
                                },
                                "version": {
                                    "type": "keyword"
                                },
                                "timestamp": {
                                    "type": "date"
                                }
                            }
                        }
                    }
                },
                "movies": {
                    # this mapping definition sets up the metadata fields for
                    # the movies
                    "properties": {
                        "movieId": {
                            "type": "integer"
                        },
                        "tmdbId": {
                            "type": "keyword"
                        },
                        "genres": {
                            "type": "keyword"
                        },
                        "release_date": {
                            "type": "date",
                            "format": "year"
                        },
                        "@model": {
                            # this mapping definition sets up the fields for 
                            # movie factor vectors of our model
                            "properties": {
                                "factor": {
                                    "type": "text",
                                    "term_vector": "with_positions_offsets_payloads",
                                    "analyzer" : "payload_analyzer"
                                },
                                "version": {
                                    "type": "keyword"
                                },
                                "timestamp": {
                                    "type": "date"
                                }
                            }
                        }
                    }
                }
            }
        }
        
        # create index with the settings and mappings above
        self.es.indices.create(index=self.idx, body=index_body)
        print("Create a new recommendation index in Elasticsearch.")
   
    
def _test_save(esop):
    
    dt = 'ratings'
    ds = esop.get_dataset()
    esop.save(data_set=ds, data_type=dt)
    print("Query upon the indexed %s data in Elasticsearch." % dt)
    print("Indexed %s data in Elasticsearch count: %d." %
          (dt, esop.es.count(index=esop.idx, doc_type=dt)['count']))
    print(esop.es.search(index=esop.idx, doc_type=dt, q='*', size=1))
    print(esop.es.count(index=esop.idx, doc_type=dt,
                        q='timestamp:[2016-01-01 TO 2016-02-01]'))
    
    dt = 'movies'
    ds = esop.get_dataset()
    esop.save(data_set=ds, data_type=dt)
    print("Query upon the indexed %s data in Elasticsearch." % dt)
    print("Indexed %s data in Elasticsearch count: %d." %
          (dt, esop.es.count(index=esop.idx, doc_type=dt)['count']))
    print(esop.es.search(index=esop.idx, doc_type=dt, q='*', size=1))
    print(esop.es.count(index=esop.idx, doc_type=dt, 
                        q='release_date:[2001 TO 2002]'))
    #print(esop.es.search(index=esop.idx, doc_type=dt, q="title:matrix", size=3))
    
def _test_read(esop):
    
    dt = 'ratings'
    ds = esop.get_dataset()
    spk = ds.get_spark()
    r_es, d = esop.read(spark=spk, data_type=dt)
    r_es.show(5)
    ds.stop_spark(spk)
    
def _test():
    
    esop = ESOperation()
    print("Elasticsearch\'s version is: %s." % esop.es.info(pretty=True)['version'])
    
    _test_save(esop)
    _test_read(esop)
    
if __name__ == '__main__':
    _test()
