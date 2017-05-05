import boto3
import sys

"""
DynamoDB example for a "frequency cap" database.

Each session ID (string) holds a map of campaigns => frequency items.
./freq_cap.py CLEAR removes the database 
./freq_cap.py ADD session_id campaign_id adds campaign_id to the session - or increases its counter
./freq_cap.py DEL session_id campaign_id removes campaign_id from the session 


"""


class FreqCap(object):
    def __init__(self):
        self.ddb = boto3.resource('dynamodb',
                                  endpoint_url='http://localhost:8000',
                                  region_name='eu-central-1')
        self.client = self.ddb.meta.client
        try:
            self.client.describe_table(
                TableName='freq_cap'
            )
        except:
            self.setup_table()
        self.table = self.ddb.Table('freq_cap')

    def setup_table(self):
        self.ddb.create_table(
            TableName='freq_cap',
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

    def delete_table(self):
        self.table('freq_cap').delete()

    def describe_table(self):
        response = self.client.describe_table(TableName='freq_cap')
        print("AttributeDefinitions: ", response["Table"]["AttributeDefinitions"])
        print("ItemCount: ", response["Table"]["ItemCount"])

    def write_freq_cap(self, session_id, campaign):
        key = {'id': session_id}

        item = self.table.get_item(Key=key)

        if 'Item' not in item:
            self.table.put_item(Item={
                'id': session_id,
                'campaign': {
                    campaign: 1
                }
            })
        else:
            campaign_data = item['Item']['campaign']
            try:
                fcdata = campaign_data[campaign] + 1
            except KeyError:
                fcdata = 1

            self.table.update_item(
                Key=key,
                UpdateExpression='SET #campaign.#cid = :fcdata',
                ExpressionAttributeNames={
                    '#campaign': 'campaign',
                    '#cid': campaign
                },
                ExpressionAttributeValues={
                    ':fcdata': fcdata
                }
            )

        item = self.table.get_item(Key=key)

        print("Item", item["Item"])

    def expire_freq_cap(self, sessid, campaign):
        key = {
            'id': sessid,
        }
        self.table.update_item(
            Key=key,
            UpdateExpression='REMOVE #campaign.#cid',
            ExpressionAttributeNames={
                '#campaign': 'campaign',
                '#cid': campaign
            }
        )
        item = self.table.get_item(Key=key)

        print("Item", item["Item"])


def main(argv):
    command = argv[0]
    fc = FreqCap()
    if command == 'CLEAR':
        print("Deleting table")
        fc.delete_table()
    else:
        session_id = argv[1]
        campaign_id = argv[2]
        if command == 'ADD':
            fc.write_freq_cap(session_id, campaign_id)
        elif command == 'DEL':
            fc.expire_freq_cap(session_id, campaign_id)
        else:
            raise ValueError('Missing command')


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (ValueError, IndexError):
        print("Syntax: {} CLEAR|ADD|DEL [session id] [campaign id]".format(sys.argv[0], ))
