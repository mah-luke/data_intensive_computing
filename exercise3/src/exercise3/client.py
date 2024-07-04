import requests
from random import randint
import cv2
import base64
import os
import sys

def detect_all_in_folder(dir_str, url):
    directory = os.fsencode(dir_str)
        
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".jpg"): 
            img = cv2.imread(os.path.join(dir_str, filename))
            jpg_img = cv2.imencode('.jpg', img)
            b64_string = base64.b64encode(jpg_img[1]).decode('utf-8')

            myobj = {'id': filename[:-4], "image_data": b64_string}

            x = requests.post(url, json = myobj)

            print(x.text)
        else:
            continue

if __name__ == "__main__":
    dir_str = "../../resource/input_folder"
    url = 'http://127.0.0.1:5000/yolo'
    if len(sys.argv) > 1:
        dir_str = sys.argv[1]
    if len(sys.argv) > 2:
        url = sys.argv[2]

    detect_all_in_folder(dir_str, url)


