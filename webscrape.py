#!/usr/bin/python3

from bs4 import BeautifulSoup  
import configparser
import csv
import datetime
import re
import requests
import sqlite3
import sys
import time

from balloon import *
from telemetry import *

config = configparser.ConfigParser()
config.read('balloon.ini')
push_habhub = config['main']['push_habhub']
push_sondehub = config['main']['push_sondehub']
push_aprs = config['main']['push_aprs']
balloons = config['main']['balloons']
filter_only_spots_newer = config['main']['filter_only_spots_newer']

balloons = json.loads(config.get('main','balloons'))

db_counter = 0

print("Tracking these balloons:")
for b in balloons:
      print(b)
# print("Tracking these balloons:\n",type(balloons))

# sys.exit(0)

def getspots (nrspots):
#    print("Fetching...")
#    wiki = "http://wsprnet.org/olddb?mode=html&band=all&limit=" + str(nrspots) + "&findcall=&findreporter=&sort=spotnum"
    wiki = "https://wsprnet.org/olddb?mode=html&band=10&limit=" + str(nrspots) + "&findcall=&findreporter=&sort=spotnum"
    try:
        page = requests.get(wiki)
    except requests.exceptions.RequestException as e:
        print("ERROR",e)
        return []

#    print(page.status)
#    print(page.data)

    soup = BeautifulSoup(page.content, 'html.parser')
    
    data = []

    try:
        table = soup.find_all('table')[2]
        # print("TABLE:",table)

        rows = table.findAll('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele]) # Get rid of empty values

    except IndexError as e:
        print("ERROR",e)

    # Strip empty rows
    newspots = [ele for ele in data if ele] 

    # Strip redundant columns Watt & miles and translate/filter data
    for row in newspots:
        row[0] = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M')
        row[6] = int(row[6].replace('+',''))

        del row[10]
       # del row[11]
        del row[7]
        

    # Reverse the sorting order of time to get new spots firsts
    newspots.reverse()

    return newspots


# 
# Dump new spots to db. Note stripping of redundant fields
#
# 2018-05-28 05:50,OM1AI,7.040137,-15,0,JN88,+23,DA5UDI,JO30qj,724
def dumpnewdb(spotlist):
    con = None
    data = None
    
    try:
        con = sqlite3.connect('wsprdb.db')
        cur = con.cursor()
        cur.execute('create table if not exists newspots(timestamp varchar(20), tx_call varchar(10), freq real, snr integer, drift integer, tx_loc varchar(6), power integer, rx_call varchar(10), rx_loc varchar(6), distance integer)')
        for row in spotlist:
            cur.execute("INSERT INTO newspots VALUES(?,?,?,?,?,?,?,?,?,?)", (row))
            data = cur.fetchall()

        if not data:
            con.commit()
    except sqlite3.Error as e:
        print("Database error: %s" % e)
    except Exception as e:
        print("Exception in _query: %s" % e)
    finally:
        if con:
            con.close()
    return

# Filter out only calls from balloons and telemetrypackets
def balloonfilter(spots,balloons):
    filtered = []
    calls = []
    for b in balloons:
        calls.append(b[1])

    for row in spots:
        for c in calls:
            if row[1] == c:

                # Remove selfmade WSPR tranmissions
                if len(row[5]) == 4:
                    filtered.append(row)
                else:
                    row[5] = row[5][0:4]
                    filtered.append(row)

        if re.match('(^1|^0|^Q).[0-9].*', row[1]):
            
            # Coarse bogus filter - just save 30m and 20m
            if re.match('10\..*', row[2]) or re.match('28\..*', row[2]):
                #               print("Found", row)
                filtered.append(row)

#    for r in filtered:
#        print("filtered out",r)

    return filtered

# 2018-05-28 05:50,OM1AI,7.040137,-15,0,JN88,+23,DA5UDI,JO30qj,724
def deduplicate(spotlist):
    pre=len(spotlist)
    
    rc = 0
    rc_max = len(spotlist)-1
    if rc_max > 1:
        while rc < rc_max:
#            print("R:",rc, rc_max, len(spotlist))
#            print(spotlist[rc])
#            print(spotlist[rc+1])
            if (spotlist[rc][0] == spotlist[rc+1][0]) and (spotlist[rc][1] == spotlist[rc+1][1]):
#                print("Duplicate entry")
                del spotlist[rc]
                rc_max -= 1
            else:
                rc += 1


#    print("Deduplicate:",pre, len(spotlist))
    return spotlist


spots = []

# Read active balloons from db
# balloons = readballoonsdb()

# Spots to pull from wsprnet
nrspots_pull= 3000
spotcache = []

print("Preloading cache from wsprnet...")
#spotcache = getspots(10000)
spotcache = getspots(5000)
print("Fspots1",len(spotcache))
spotcache = balloonfilter(spotcache ,balloons)
print("Fspots2",len(spotcache))

spots = spotcache
cache_max = 10000
new_max = 0
only_balloon=False
sleeptime = 75

print("Entering pollingloop.")
while 1==1:
    tnow = datetime.datetime.now() 

    wwwspots = getspots(nrspots_pull)
    wwwspots = balloonfilter(wwwspots ,balloons)
    newspots = [] 
    # 
    # wwwspots.reverse()

#    for q in spotcache:
#        print("cache:",q)

    # Sort in case some spots arrived out of order

    spotcache.sort(reverse=False)   
    spotcache = timetrim(spotcache,120)


    src_cc = 0 

    # Loop trough cache and check for new spots
    for row in wwwspots:
        old = 0
        for srow in spotcache:
            # print("testing:",row, "\nagainst:", srow)
            src_cc += 1
            if row == srow:
                # print("Found",row)
                old = 1
                break

        if old == 0:
            print("New",row)
            
            # Insert in beginning for cache
            spotcache.insert(0, row)

 #           for w in spotcache:
 #               print("cache2:", w)

            # Add last for log
            newspots.append(row)

#     spotcache.sort(reverse=True)
#    print("first:",spotcache[0][0]," last: ",spotcache[-1:][0][0])
#    print("DATA:\n")
#    for row in newspots:
#        print("Newspots:",row)

#    dumpcsv(newspots)
    dumpnewdb(newspots)

    spots = spots + newspots
    spots.sort(reverse=False)   
    spots = deduplicate(spots) # needs sorted list
    # Filter out all spots newer that x minutes
    spots = timetrim(spots,int(filter_only_spots_newer))
    print("Filter all spots out newer than", int(filter_only_spots_newer), "minutes:")

    if len(spots) > 1:
        try:
            print("pre-tele:",len(spots))
            spots = process_telemetry(spots, balloons, habhub_callsign, push_habhub, push_sondehub, push_aprs)
            # print("pro-tele:",len(spots))
        except Exception as e:
            print("Process Telemetry Error: %s" % e)


    if new_max < len(newspots):
#  and len(newspots) != nrspots_pull:
        new_max = len(newspots)

    if len(newspots) == nrspots_pull:
        print("Hit max spots. Increasing set to fetch")
        nrspots_pull += 100

#    print("%s Spots: %6d New: %5d (max: %5d) Nrspots: %5d Looptime: %s Checks: %8d Hitrate: %5.2f%%" % 
#          (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), len(spotcache), len(newspots), new_max, nrspots_pull, str(datetime.datetime.now() - tnow).split(":")[2], src_cc, 100-(src_cc / (len(spotcache)*nrspots_pull))*100))

    print("%s Spots: %5d Cache: %6d New: %5d (max: %5d) Nrspots: %5d Looptime: %s Checks: %8d" % 
          (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), len(spots), len(spotcache), len(newspots), new_max, nrspots_pull, str(datetime.datetime.now() - tnow).split(":")[2], src_cc)) 

    spotcache = spotcache[:cache_max]

    # Delete unnecessary entries in the table "newspots" which are older than 1 day 
    try:
        # delete DB one time a day (one day has 1440 Minutes)
        if db_counter >= 1440:
            print("Checking DB (newspots) to delete entries older than 1 Day.....")
            con_oldest = sqlite3.connect('wsprdb.db')
            cur_oldest = con_oldest.cursor()
            cur_oldest.execute('DELETE FROM newspots WHERE timestamp < DATETIME("now", "-1 day")')
            deleted_entries = cur_oldest.rowcount
            con_oldest.commit()
            con_oldest.close()
            db_counter = 0
            if deleted_entries > 0:
                print(deleted_entries, "entries deleted from the database")
    except Exception as e:
        print("Error at checking DB and deleting entries older than....: %s" % e)    

    db_counter = db_counter + 1
#    print("DB COUNTER:", db_counter)

    sleeping = sleeptime - int(datetime.datetime.now().strftime('%S')) % sleeptime
#    print("Sleep:", sleeping)
    time.sleep(sleeping)







        
