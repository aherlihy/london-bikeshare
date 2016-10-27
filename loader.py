import json
import re
import sys
import zipfile

from datetime import datetime
from decimal import Decimal

import pymongo

time_format = "%d/%m/%Y %H:%M"

old_billingsgate = ('276',  # "Lower Thames Street, Monument"
                    '587',  # "Monument Street, Monument"
                    '199')  # "Great Tower Street, Monument"
bad_ids = set([])

station_data = 'livecyclehireupdates.json'

# Data from: https://api-portal.tfl.gov.uk/docs (requires login)


def parse_station_data(filename):
    with open(filename) as json_data:
        d = json.load(json_data)

    with pymongo.MongoClient() as client:
        client.londonbikes.stations.insert_many(d['stations']['station'])


def parse_trip_data(filename):
    documents = {"both": [], "pickup": [], "dropoff": [], "other": []}
    with pymongo.MongoClient() as client:
        db = client.londonbikes.rides
        with zipfile.ZipFile(filename) as z:
            with z.open(z.infolist()[0]) as f:
                first = True
                for line in f:
                    if first:
                        first = False
                        continue
                    line = unicode(line, 'utf-8', errors='ignore')

                    try:
                        (d1, endstation_name, d2,
                         startstation_name) = line.strip().strip(
                            ",").strip("\"").split('"')

                        (rental_id, duration, bike_id, end_date,
                         endstation_id) = re.split(r",+", d1.strip(','))
                        start_date, startstation_id = re.split(
                            r",+", d2.strip(','))
                    except:
                        print "Error parsing line: ", line
                        continue

                    pickup = (startstation_id in old_billingsgate)
                    dropoff = (endstation_id in old_billingsgate)

                    endstation_gps = client.londonbikes.stations.find_one(
                        {'id': endstation_id})
                    startstation_gps = client.londonbikes.stations.find_one(
                        {'id': startstation_id})

                    if pickup and dropoff:
                        collection = "both"
                    elif pickup and not dropoff:
                        collection = "pickup"
                    elif not pickup and dropoff:
                        collection = "dropoff"
                    else:
                        collection = "other"

                    doc = {
                        "rental_id": int(rental_id),
                        "duration": int(duration),
                        "bike_id": int(bike_id),
                        "endstation_id": int(endstation_id),
                        "endstation_name": endstation_name,
                        "startstation_id": int(startstation_id),
                        "startstation_name": startstation_name,
                        "pickup_time": datetime.strptime(start_date,
                                                         time_format),
                        "dropoff_time": datetime.strptime(end_date,
                                                          time_format)
                    }
                    try:
                        doc["startstation_gps"] = {
                            "type": "Point",
                            "coordinates": [float(startstation_gps["long"]),
                                            float(startstation_gps["lat"])]}
                    except Exception as e:
                        # print("ERROR: start id=", startstation_id, "null=",
                        #       startstation_gps == None)
                        bad_ids.add(startstation_id)
                        continue
                    try:
                        doc["endstation_gps"] = {
                            "type": "Point",
                            "coordinates": [float(endstation_gps["long"]),
                                            float(endstation_gps["lat"])]}
                    except:
                        # print("ERROR: end id=", endstation_id, "null=",
                        #       endstation_gps == None)
                        bad_ids.add(endstation_id)
                        continue

                    documents[collection].append(doc)

                    if len(documents["both"]) >= 4000:
                        db.both.insert(documents["both"])
                        documents["both"] = []
                    if len(documents["pickup"]) >= 4000:
                        db.pickup.insert(documents["pickup"])
                        documents["pickup"] = []
                    if len(documents["dropoff"]) >= 4000:
                        db.dropoff.insert(documents["dropoff"])
                        documents["dropoff"] = []
                    if len(documents["other"]) > 4000:
                        db.other.insert(documents["other"])
                        documents["other"] = []
                if len(documents["both"]) != 0:
                    db.both.insert(documents["both"])
                if len(documents["pickup"]) != 0:
                    db.pickup.insert(documents["pickup"])
                if len(documents["dropoff"]) != 0:
                    db.dropoff.insert(documents["dropoff"])
                if len(documents["other"]) != 0:
                    db.other.insert(documents["other"])


if __name__ == '__main__':
    if len(sys.argv) == 1:
        sys.exit("Error: need to pass at least one zip file to load")
    for s in sys.argv[1:]:
        if s[-4:] != ".zip":
            sys.exit("Error: files passed into loader need to be .zip")
    data_files = sys.argv[1:]
    for fname in data_files:
        print('\tParsing %s' % fname)
        parse_trip_data(fname)
    print 'Done. Bad IDs:', bad_ids, len(bad_ids)
