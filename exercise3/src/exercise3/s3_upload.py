from random import randint
import os
import sys
import time
import boto3

ACCESS_KEY = "ASIATJOTZGEL3LX577MK"
SECRET_KEY = "0eDrewnk+ojUtXJEr1Gj4cqZX7kC8eYWwlJke3fn"
SESSION_TOKEN = "IQoJb3JpZ2luX2VjEEkaCXVzLXdlc3QtMiJHMEUCIHB9PIeX0L0sN//igLIHmgFrOdxKyfX27/3BLV1FdRU1AiEAtxU5RfcpI3IhoNdJ6SVjrtmYfHEgCnH0FgkNi40vg64qugIIEhAAGgwyMjY0NjY4MDM5OTEiDK6q+rd6hCnUGIDKfyqXAiAYE+YLAgOKts6imYiJYb/qUCkSt62zL6RY8cxTo/opKq4fsW6TrD99BncpGqM5SMVsulHgvxbbeNrP8Em1W7U1f8lOe93vTWcbbnNyLDp48dRsdgLepYIEPX1zlMpafBPpVtv9bcAKPQJj2w0T/v7WOekysiNb4p2u/2I0soV+ep8ERCtZH6I2+rMXX6Rgt5Wm3i3nvwo41iV3uGaIICln/2jFun+EMWNUUM+0dkJLvnehbBfNpBA+zAFqt3qxjQipZVKIGvPHSaT77UxUIiwF0jSwSxcJCoxTeAOBQs6OvVS0dIrZjO05pRj7dfCksWH7fEiypsbOPxQvpxercnEIa2ApJB4vVrU9LHG+UHC9qUUD2eRYITD31q60BjqdAQHrbtVNuEJe3aN9Zcf3xJGKzZ8beS/k4Ui2qOzerLIdDfCGLnLLXg+UVn1pe26PnZN62a/Q0F7ky/BlhJrxUiNdp3RZHMI2hv16SKNtYkZZUjPovfLJZq/biiG/1LGJgrhWZhvOx+7ZZ082vxQFBscCEWDBSzAZ0EIbIyTJXm0KqOmCnGUhGva4sX9MGkyNcqjYZ0u78M98AzHF3jE="

def upload_all_to_s3(dir_str):
    directory = os.fsencode(dir_str)
    results = []

    # iterate over all files in the directory
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        # only send jpeg images
        if filename.endswith(".jpg"):
            s3 = boto3.client(
                's3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                aws_session_token=SESSION_TOKEN
            )

            start = time.time()
            s3.upload_file(dir_str+"/"+filename, "jfimages", filename)
            upload_time = time.time() - start

            results.append({"file": filename, "upload_time": upload_time})

        else:
            continue
        
    return results

if __name__ == "__main__":
    # default locations
    dir_str = "../../resource/input_folder"
    if len(sys.argv) > 1:
        dir_str = sys.argv[1]

    results = upload_all_to_s3(dir_str)

    sum = 0
    for result in results:
        sum += result["upload_time"]
    
    print("average upload time:", sum/len(results))