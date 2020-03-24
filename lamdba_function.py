import json
import boto3
import gzip
import io
import codecs
import time
from boto3.session import Session
from datetime import datetime

s3_bucket = 'ログを保存しているバケット'

# 最新のディレクトリを指定するために、現時刻からディレクトリの場所を探す
month = datetime.now().strftime('%m') + '/'
date = datetime.now().strftime('%d') + '/'
hour = datetime.now().strftime('%H')
s3_prefix = 'ログが保存されているディレクトリ/' + month + date + hour #S3の開きたいディレクトリ
# print("開いたディレクトリ: " + s3_prefix)

def upload_file(bucket_name, file_key, bytes):
    out_s3 = boto3.resource('s3')
    s3Obj = out_s3.Object(bucket_name, file_key)
    res = s3Obj.put(Body = bytes)
    return res

def analysis():

    s3 = boto3.resource('s3')
    s3_client = s3.meta.client
    bucket = s3.Bucket(s3_bucket)

    objs = bucket.meta.client.list_objects_v2(
        Bucket=bucket.name,
        Prefix=s3_prefix
    )
    # ディレクトリ配下のファイルについてLOOP処理
    # 最新の更新日付のファイルがダウンロードされる
    loop_first_f = True
    for o in objs.get('Contents'):
        # LOOP初回処理
        if loop_first_f:
            new_file = o.get('Key')
            modified_datetime_mid = o.get('LastModified')
            loop_first_f = False
        # 2回目以降
        else:
            # 最新の修正日時のファイル名を保持
            if modified_datetime_mid <= o.get('LastModified'):
                modified_datetime_mid = o.get('LastModified')
                new_file = o.get('Key')

　　#新しいログを代入
    obj = s3_client.get_object(Bucket=s3_bucket,Key=new_file)['Body'].read()


    file = gzip.open(io.BytesIO(obj), 'rt') #gzipを開く
    print("gzip開けたよ")

    #無理やりjsonファイルに変換, 文字列置換

    json_data = "["

    for row in file.readlines():
        json_data += row.replace('\n', '') + ","
        before_item = row.replace('\n', '') + ","
        after_item = row.replace('\n', '')

    json_data += "]"

　　#最後の行を無理やり置換する
    json_data_2 = json_data.replace(before_item, after_item)
    json_data_3 = str(json_data_2)
    # print(json_data_3)#jsonの確認が出来ます


    jsonRead = json.loads(json_data_3) #jsonファイルの読み込み
    num = int(json_data_3.count("timestamp")) #jsonファイルのtimestampの数(ログの数)を代入
    i = 0

    #ログをきれいにして保存
    log = "\n" #初めに改行しておく
    action = jsonRead[i]['action'] #actionを定義
    for i in range(num): #0から要素数回繰り返す


        if(action == 'BLOCK'): #BLOCKのものを抽出

            log += '+---------------------------------------------------+\n'
            log += str('terminatingRuleId：{}'.format(jsonRead[i]['terminatingRuleId'])) + "\n"
            log += str('action：{}'.format(jsonRead[i]['action'])) + "\n"
            log += str('clientIp：{}'.format(jsonRead[i]['httpRequest']['clientIp'])) + "\n"
            log += str('country：{}'.format(jsonRead[i]['httpRequest']['country'])) + "\n"
            log += str('headers：{}'.format(jsonRead[i]['httpRequest']['headers'][1])) + "\n"
            log += str('uri：{}'.format(jsonRead[i]['httpRequest']['uri'])) + "\n"
            log += str('httpVersion：{}'.format(jsonRead[i]['httpRequest']['httpVersion'])) + "\n"
            log += str('httpMethod：{}'.format(jsonRead[i]['httpRequest']['httpMethod'])) + "\n"
            log += '+---------------------------------------------------+\n'
            log += "\n"
            log += "\n"


        else:
            log = "ブロック履歴はありません。"

    print(log)

    if action == 'BLOCK':#BLOCK履歴があったら
        # ファイルをS3に出力
        output_key = "block_log" + "log" + datetime.now().strftime('%Y年%m月%d日 %H:%M:%S') + '.txt'
        upload_file(s3_bucket, output_key, bytes(log, 'UTF-8'))
        print("BLOCKを保存しました")


def lambda_handler(event, context):

    analysis() #アラートがなったタイミングでのログを解析
    time.sleep(300) #5分待つ
    analysis() #新しいログが生成されたタイミングで最解析
