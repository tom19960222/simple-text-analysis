from __future__ import print_function
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
import jieba
import jieba.analyse
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy import spatial
from collections import OrderedDict
import urllib

import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal


def make_news_id_pair(news_id_one, news_id_two):
    if int(news_id_one) > int(news_id_two):
        return '{}-{}'.format(news_id_two, news_id_one)
    else:
        return '{}-{}'.format(news_id_one, news_id_two)


def main(event, context):
    urllib.urlretrieve('https://s3-ap-northeast-1.amazonaws.com/python-data-analysis/idf.txt', '/tmp/idf.txt')
    urllib.urlretrieve('https://s3-ap-northeast-1.amazonaws.com/python-data-analysis/dict.txt', '/tmp/dict.txt')
    jieba.analyse.set_idf_path('/tmp/idf.txt')
    jieba.set_dictionary('/tmp/dict.txt')

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('News')
    news_ids_to_update = json.loads(event['Records'][0]['Sns']['Message']) if 'Records' in event else {}
    news_ids_to_update = news_ids_to_update['news_id_list']

    # jieba.set_dictionary('dict.txt.big')

    response = table.query(
        IndexName = 'datatype-index',
        KeyConditionExpression = Key('datatype').eq('politics')
    )

    news_list = response['Items']
    news_id_list = [news['news_id'] for news in news_list]
    news_title_list = [news['news_title'] for news in news_list]

    news_ids_index_to_update = []
    for news_id_to_update in news_ids_to_update:
        for index, news_id in enumerate(news_id_list):
            if news_id_to_update == news_id:
                news_ids_index_to_update.append(index)

    corpus = []
    for news_content in news_list:
        corpus.append(" ".join(jieba.cut_for_search(news_content['news_content'])))

    cv = CountVectorizer()
    term_doc = cv.fit_transform(corpus)

    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(term_doc)
    fv = tfidf.toarray()

    score = {}
    for i in range(len(fv)):
        for j in news_ids_index_to_update:
            print('similarity between news {} and {}: {}'.format(i+1, j+1, 1 - spatial.distance.cosine(fv[i], fv[j])))
            score['{}-{}'.format(i,j)] = 1 - spatial.distance.cosine(fv[i], fv[j])

    print()

    for index, value in list(reversed(sorted(score.items(), key= lambda a: a[1]))):
        if value == 0.0:
            continue
        index_i = int(index.split('-')[0])
        index_j = int(index.split('-')[1])
        print('{} / {} : {}'.format(news_id_list[index_i], news_id_list[index_j], value))

        table = dynamodb.Table('NewsRelated')
        table.update_item(
            Key={'news_id_pair': make_news_id_pair(news_id_list[index_i], news_id_list[index_j])},
            UpdateExpression='SET related_value = :value',
            ExpressionAttributeValues={
                ':value': Decimal(str(value))
            }
        )

if __name__ == '__main__':
    main({}, {})
