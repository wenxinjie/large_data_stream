from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import marshal
import time

from twitter_credentials import *

class Listener(StreamListener):
    def __init__(self):
        self.geo_count = 0
        self.total_count = 0

    def on_data(self, data):
        data = json.loads(data)
        self.total_count += 1
        if data.get("place") and data["place"] and data["place"]["bounding_box"] and data["place"]["bounding_box"]["coordinates"]:
            self.geo_count += 1
            if data.get("text"):
                print(data["text"])
            #print(data["place"]["bounding_box"]["coordinates"])
            coordinates = data["place"]["bounding_box"]["coordinates"][0]
            lon = (coordinates[0][0] + coordinates[2][0]) / 2
            lat = (coordinates[0][1] + coordinates[1][1]) / 2
            t = time.time()

            with open("./data/logs/access.log", "a+") as fd:
                fd.write("{:0.2f},{:0.2f} {}".format(lon, lat, int(t)))
                fd.write("\n")

        #print("{}/{} time:{}".format(self.geo_count, self.total_count, time.time()-self.tic))
        return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    
    listener = Listener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = Stream(auth, listener)
    GEOBOX_GERMANY = [5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757]
    GEOBOX_WORLD = [-180, -90, 180, 90]
    GEOBOX_US = [-125, 27, -62, 50]
    GEOBOX_CN =[104, 25, 119, 34]

    stream.filter(locations=GEOBOX_US)
