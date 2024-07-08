import boto3
import time
from darknet import load_net, load_meta, detect
import tempfile

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("result")

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    
    print(bucket)
    print(key)

    weights_tmp = tempfile.NamedTemporaryFile()
    s3.download_file("jfyolo", "yolov3-tiny.weights", weights_tmp.name)

    # provided configuraiton and weights
    net = load_net(b"yolov3-tiny.cfg", str.encode(weights_tmp.name), 0)
    # local installation of darknet
    meta = load_meta(b"coco.data")

    tmp = tempfile.NamedTemporaryFile()
    s3.download_file(bucket, key, tmp.name)

    # use YOLOv3-tiny to detected objects
    res = detect(net, meta, str.encode(tmp.name), thresh=0.2)

    objects = {}

    for r in res:
        object = str(r[0])[2:-1]
        prob = r[1]
        objects[object] = max(objects.get(object, 0), prob)

    objects_string = ", ".join("".join([item[0], "(", str(round(item[1],5)), ")"]) for item in objects.items())
    
    print(objects_string)

    # write result to DB
    table.put_item(Item={"id": str(time.time()), "filename": key, "result": objects_string})