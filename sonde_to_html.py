
# pip install python-dateutil

from __future__ import print_function, division, absolute_import
import fileinput
import sys
import ftplib
import datetime
import configparser
import json
from dateutil.relativedelta import relativedelta  # ✅ NEW

config = configparser.ConfigParser()
config.read('balloon.ini')

balloons = json.loads(config.get('main','balloons'))

push_ftp = config['main']['push_ftp']
ftp_server = config['main']['ftp_server']
ftp_username = config['main']['ftp_username']
ftp_password = config['main']['ftp_password']


# ✅ REPLACED FUNCTION
def getDuration(then, now, interval="default"):
    diff = relativedelta(now, then)

    return {
        'months': diff.years * 12 + diff.months,
        'days': diff.days,
        'hours': diff.hours,
        'minutes': diff.minutes,
    }[interval]


def push_balloon_to_html(balloon_name, telemetry):
    then = None
    
    # search the correct balloon release date from the ini
    for b_name in balloons:
        b_name_balloon = b_name[0]
        b_start = b_name[7]
        if b_name_balloon == balloon_name:
            then = datetime.datetime.strptime(b_start, "%Y%m%dT%H%M")

    def add_position(file):  # search and replace for the map position
        searchExp = "// #POSITION#"
        replaceExp = f"\t\tnew google.maps.LatLng({telemetry['lat']},{telemetry['lon']}),\n// #POSITION#"
        for line in fileinput.input(file, inplace=1):
            if searchExp in line:
                line = line.replace(searchExp, replaceExp)
            sys.stdout.write(line)

    add_position(balloon_name + '.html')  


    def update_popup(file):  # search and replace for the telemetry popup
        time_now = datetime.datetime.utcnow()
        time = time_now.strftime("%Y-%m-%d %H%M")

        searchExp = "'<p>Updated"
        replaceExp = (
            f"'<p>Updated {time}Z<br />"
            f"Locator = {telemetry['loc'].upper()}<br />"
            f"Duration = {getDuration(then, telemetry['time'], 'months')}mo "
            f"{getDuration(then, telemetry['time'], 'days')}d "
            f"{getDuration(then, telemetry['time'], 'hours')}h "
            f"{getDuration(then, telemetry['time'], 'minutes')}m<br />"
            f"Distance = '+ distance +'km<br />"
            f"Altitude = {telemetry['alt']}m<br />"
            f"Speed = {telemetry['speed']}kt {round(telemetry['speed']*1.852)}km/h<br />"
            f"Solar = {round(telemetry['batt'],2)}V, "
            f"Temp = {round(telemetry['temp'],1)}C, <br />"
            f"GPS = {int(telemetry['gps'])}, "
            f"Sats = {int(telemetry['sats'])}</p>'; \n"
        )

        for line in fileinput.input(file, inplace=1):
            if searchExp in line:
                line = replaceExp
            sys.stdout.write(line)

    update_popup(balloon_name + '.html')  


    if push_ftp == 'True':
        UPLOAD_FILENAME = balloon_name + '.html'
        session = ftplib.FTP(ftp_server, ftp_username, ftp_password)
        file = open(balloon_name + '.html', 'rb')
        session.storbinary('STOR %s' % UPLOAD_FILENAME, file)
        file.close()
        session.quit()
        print("HTML page uploaded with FTP")