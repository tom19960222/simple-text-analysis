from requests_html import HTMLSession
import boto3
import json

dynamodb = boto3.resource('dynamodb')
client = boto3.client('sns')


def get_news_content(url):
    session = HTMLSession()
    req = session.get(url)
    content_node = req.html.find('article', first=True)
    body_node = content_node.find('.canvas-body', first=True)
    content = body_node.text
    return content


def get_news_id(url):
    url = str(url)
    tmp_list = url.split('-')
    id_with_html = tmp_list[-1]
    id_split = id_with_html.split('.')[0]
    return id_split


def main(event, context):
    table = dynamodb.Table('News')
    news_id_list = []

    session = HTMLSession()
    req = session.get('https://tw.news.yahoo.com/politics')
    title_nodes = req.html.find('.Mb\(5px\)')
    for node in title_nodes:
        news_link_node = node.find('a', first=True)
        news_link = 'https://tw.news.yahoo.com' + news_link_node.attrs['href']
        news_id = get_news_id(news_link)
        news_title = node.text
        news_content = get_news_content(news_link)
        news_id_list.append(news_id)
        table.update_item(
            Key={'news_id': news_id},
            UpdateExpression='SET news_link = :news_link, news_title = :news_title, '
                             'news_content = :news_content, datatype = :type',
            ExpressionAttributeValues={
                ':news_link': news_link,
                ':news_title': news_title,
                ':news_content': news_content,
                ':type': 'politics'
            }
        )
        print('---------------------------')
        print('News ID: {}'.format(news_id))
        print(news_title)
        print('Getting {}'.format(news_link))
        print()
        print(news_content)
        print('---------------------------')
        print()
    sns_response = client.publish(
        TopicArn='arn:aws:sns:ap-northeast-1:046512953700:Python-Data-Analysis',
        Message=json.dumps({'news_id_list': news_id_list})
    )
    print('Sent SNS notification, responded with {}'.format(sns_response))


if __name__ == '__main__':
    main()