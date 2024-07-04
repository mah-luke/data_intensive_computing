from flask import Flask, render_template, request, url_for, jsonify
from .darknet import load_net, load_meta, detect
import os
import base64

app = Flask(__name__)

@app.route('/yolo', methods=['POST']) 
def yolo():
    input_json = request.get_json(force=True)

    id = int(input_json["id"])
    base64_image = str.encode(input_json["image_data"])

    temp_image = "./temp/image.jpg"
    os.makedirs(os.path.dirname(temp_image), exist_ok=True)

    with open(temp_image, "wb") as f:
        f.write(base64.decodebytes(base64_image))

    net = load_net(b"../../resource/yolo_tiny_configs/yolov3-tiny.cfg", b"../../resource/yolo_tiny_configs/yolov3-tiny.weights", 0)
    meta = load_meta(b"/Users/julian/darknet/cfg/coco.data")
    res = detect(net, meta, str.encode(temp_image), thresh=0.2)

    objects = {}

    for r in res:
        object = str(r[0])[2:-1]
        prob = r[1]
        objects[object] = max(objects.get(object, 0), prob)

    objects = [{"label": item[0], "accuracy": item[1]} for item in objects.items()]

    result = {"id": id,
              "objects": objects}
    return jsonify(result)
    
if __name__ == '__main__':
    app.run(debug=True)