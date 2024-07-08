import requests
from random import randint
import cv2
import base64
import os
import sys
import time
import json

def detect_all_in_folder(dir_str, url):
    directory = os.fsencode(dir_str)
    results = []

    # iterate over all files in the directory
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        # only send jpeg images
        if filename.endswith(".jpg"): 
            # open the image and convert it to base64
            img = cv2.imread(os.path.join(dir_str, filename))
            jpg_img = cv2.imencode('.jpg', img)
            b64_string = base64.b64encode(jpg_img[1]).decode('utf-8')

            # create dict for sending to flask server
            myobj = {'id': filename[:-4], "image_data": b64_string}

            # connect to falsk server and await response
            start = time.time()
            x = requests.post(url, json = myobj)
            run_time = time.time() - start

            # format result and append to list
            result = json.loads(x.text)
            result["run time"] = run_time
            results.append(result)
            print(result)

        else:
            continue
        
    return results

if __name__ == "__main__":
    # default locations
    dir_str = "../../resource/input_folder"
    url = 'http://127.0.0.1:5000/yolo'
    if len(sys.argv) > 1:
        dir_str = sys.argv[1]
    if len(sys.argv) > 2:
        url = sys.argv[2]

    results = detect_all_in_folder(dir_str, url)

    sum = 0
    for result in results:
        sum += result["run time"]

    print("average response time: ", sum/len(results))


