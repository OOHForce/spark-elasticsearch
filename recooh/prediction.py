# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 10:31:11 2018

@author: beersoccer
"""

import tmdbsimple as tmdb

from tmdbsimple import APIKeyError
from IPython.display import Image
from IPython.display import HTML
from IPython.display import display
from elasticsearch import Elasticsearch


class Prediction:

    def __init__(self):

        pass

    def get_poster_url(self, id):
        """Fetch movie poster image URL from TMDb API given a tmdbId"""
        IMAGE_URL = 'https://image.tmdb.org/t/p/w500'
        try:
            movie = tmdb.Movies(id).info()
            poster_url = IMAGE_URL + \
                movie['poster_path'] if 'poster_path' in movie and \
                movie['poster_path'] is not None else ""
            return poster_url
        except APIKeyError as err:
            print("The API Key to access TMDB is wrong.")
            return "KEY_ERR"
        except Exception as err:
            print("Can\'t display movie posters due to: %s" % str(err))
            return "NA"

    def fn_query(self, query_vec, q="*", cosine=False):
        """
        Construct an Elasticsearch function score query.

        The query takes these as parameters:
            - the field in the candidate document that contains the factor vector
            - the query vector
            - a flag indicating whether to use dot product or cosine similarity (normalized dot product) for scores

        The query vector passed in will be the user factor vector (if generating recommended movies for a user)
        or movie factor vector (if generating similar movies for a given movie)
        """
        return {
            "query": {
                "function_score": {
                    "query": {
                        "query_string": {
                            "query": q
                        }
                    },
                    "script_score": {
                        "script": {
                                "inline": "payload_vector_score",
                                "lang": "native",
                                "params": {
                                    "field": "@model.factor",
                                    "vector": query_vec,
                                    "cosine": cosine
                                }
                        }
                    },
                    "boost_mode": "replace"
                }
            }
        }

    def reverse_convert(self, s):
        '''Convert a delimited token filter format string back to list format'''
        return [float(f.split("|")[1]) for f in s.split(" ")]

    def get_similar(self, es, the_id, q="*", num=10, index="recooh", dt="movies"):
        """
        Given a movie id, execute the recommendation function score query
        to find similar movies, ranked by cosine similarity
        """
        response = es.get(index=index, doc_type=dt, id=the_id)
        src = response['_source']
        if '@model' in src and 'factor' in src['@model']:
            raw_vec = src['@model']['factor']
            # our script actually uses the list form for the query vector
            # and handles conversion internally
            query_vec = self.reverse_convert(raw_vec)
            q = self.fn_query(query_vec, q=q, cosine=True)
            results = es.search(index, dt, body=q)
            hits = results['hits']['hits']
            print("Get %d similar ones as item %d." % (num, the_id))
            return src, hits[1:num+1]

    def get_user_recs(self, es, the_id, q="*", num=10, index="recooh"):
        """
        Given a user id, execute the recommendation function score query
        to find top movies, ranked by predicted rating
        """
        response = es.get(index=index, doc_type="users", id=the_id)
        src = response['_source']
        if '@model' in src and 'factor' in src['@model']:
            raw_vec = src['@model']['factor']
            # our script actually uses the list form for the query vector
            # and handles conversion internally
            query_vec = self.reverse_convert(raw_vec)
            q = self.fn_query(query_vec, q=q, cosine=False)
            results = es.search(index, "movies", body=q)
            hits = results['hits']['hits']
            print("Get %d recommendation items for user %d." % (num, the_id))
            return src, hits[:num]

    def get_movies_for_user(self, es, the_id, num=10, index="recooh"):
        """
        Given a user id, get the movies rated by that user,
        from highest- to lowest-rated.
        """
        response = es.search(index=index, doc_type="ratings",
                             q="userId:%s" % the_id, size=num, sort=["rating:desc"])
        hits = response['hits']['hits']
        ids = [h['_source']['movieId'] for h in hits]
        movies = es.mget(body={"ids": ids}, index=index, doc_type="movies",
                         _source_include=['tmdbId', 'title'])
        movies_hits = movies['docs']
        tmdbids = [h['_source'] for h in movies_hits]
        print("Get %d movie IDs from TMDB for user %d." % (num, the_id))
        return tmdbids


    def display_user_recs(self, the_id, q="*", num=10, num_last=10, index="recooh"):
        user, recs = self.get_user_recs(the_id, q, num, index)
        user_movies = self.get_movies_for_user(the_id, num_last, index)
        # check that posters can be displayed
        first_movie = user_movies[0]
        first_im_url = self.get_poster_url(first_movie['tmdbId'])
        if first_im_url == "NA":
            display(HTML("<i>Cannot import tmdbsimple. \
                         No movie posters will be displayed!</i>"))
        if first_im_url == "KEY_ERR":
            display(HTML("<i>Key error accessing TMDb API. Check your API key. \
                         No movie posters will be displayed!</i>"))

        # display the movies that this user has rated highly
        display(HTML("<h2>Get recommended movies for user id %s</h2>" % the_id))
        display(HTML("<h4>The user has rated the following movies highly:</h4>"))
        user_html = "<table border=0>"
        i = 0
        for movie in user_movies:
            movie_im_url = self.get_poster_url(movie['tmdbId'])
            movie_title = movie['title']
            user_html += "<td><h5>%s</h5><img src=%s width=150></img></td>" %\
                (movie_title, movie_im_url)
            i += 1
            if i % 5 == 0:
                user_html += "</tr><tr>"
        user_html += "</tr></table>"
        display(HTML(user_html))
        # now display the recommended movies for the user
        display(HTML("<br>"))
        display(HTML("<h2>Recommended movies:</h2>"))
        rec_html = "<table border=0>"
        i = 0
        for rec in recs:
            r_im_url = self.get_poster_url(rec['_source']['tmdbId'])
            r_score = rec['_score']
            r_title = rec['_source']['title']
            rec_html += "<td><h5>%s</h5><img src=%s width=150></img>\
                </td><td><h5>%2.3f</h5></td>" \
                % (r_title, r_im_url, r_score)
            i += 1
            if i % 5 == 0:
                rec_html += "</tr><tr>"
        rec_html += "</tr></table>"
        display(HTML(rec_html))

    def display_similar(self, es, the_id, q="*", num=10, index="recooh", dt="movies"):
        """
        Display query movie, together with similar movies and
        similarity scores in a table
        """
        movie, recs = self.get_similar(es, the_id, q, num, index, dt)
        q_im_url = self.get_poster_url(movie['tmdbId'])
        if q_im_url == "NA":
            display(HTML("<i>Cannot import tmdbsimple. \
                         No movie posters will be displayed!</i>"))
        if q_im_url == "KEY_ERR":
            display(HTML("<i>Key error accessing TMDb API. Check your API key. \
                No movie posters will be displayed!</i>"))

        display(HTML("<h2>Get similar movies for:</h2>"))
        display(HTML("<h4>%s</h4>" % movie['title']))
        if q_im_url != "NA":
            display(Image(q_im_url, width=200))
        display(HTML("<br>"))
        display(HTML("<h2>People who liked this movie also liked these:</h2>"))
        sim_html = "<table border=0>"
        i = 0
        for rec in recs:
            r_im_url = self.get_poster_url(rec['_source']['tmdbId'])
            r_score = rec['_score']
            r_title = rec['_source']['title']
            sim_html += "<td><h5>%s</h5><img src=%s width=150></img>\
                </td><td><h5>%2.3f</h5></td>" \
                % (r_title, r_im_url, r_score)
            i += 1
            if i % 5 == 0:
                sim_html += "</tr><tr>"
        sim_html += "</tr></table>"
        display(HTML(sim_html))


def _test():

    pred = Prediction()
    es = Elasticsearch()

    src, hits = pred.get_similar(es, the_id=2628, num=5)
    print(hits)
    user, recs = pred.get_user_recs(es, the_id=12,
                                    q='release_date:[2012 TO *]', num=5)
    print(recs)
    user_movies = pred.get_movies_for_user(es, the_id=12, num=5)
    print(user_movies)


if __name__ == '__main__':
    _test()
