import boto3
import os

def main(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('NewsRelated')

    response = table.scan(
        TableName = 'NewsRelated'
    )

    pair_list = response['Items']
    all_data_list = []
    all_news_id_set = set()
        
    for data in pair_list:
        news_id_pair = data['news_id_pair']
        news_id_source = news_id_pair.split('-')[0]
        news_id_target = news_id_pair.split('-')[1]
        value = data['related_value']
        all_news_id_set.add(news_id_source)
        all_news_id_set.add(news_id_target)
        all_data_list.append({
            'source': news_id_source,
            'target': news_id_target,
            'value': value
        })

    output_str = ''
    for news_id in list(all_news_id_set):
        want_data_list = []
        for data in all_data_list:
            if data['source'] == news_id or data['target'] == news_id:
                want_data_list.append(data)
        want_data_list = sorted(want_data_list, key=lambda d: d['value'], reverse=True)
        want_data_list = want_data_list[1:6]  # Remove 1.0
        
        output_str += 'Id_{}, '.format(news_id)
        for want_data in want_data_list:
            want_news_id = want_data['source'] if want_data['source'] != news_id else want_data['target']
            output_str += 'sid_{}, '.format(want_news_id)
        output_str = output_str[0:len(output_str) - 2] + os.linesep  # Remove comma and space.
    print(output_str)
    return output_str


if __name__ == '__main__':
    main({}, {})  # For testing.
