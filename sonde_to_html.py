import fileinput
import sys
import ftplib
import datetime
import configparser
import ftplib
import json

config = configparser.ConfigParser()
config.read('balloon.ini')

balloons = json.loads(config.get('main','balloons'))

push_ftp = config['main']['push_ftp']
ftp_server = config['main']['ftp_server']
ftp_username = config['main']['ftp_username']
ftp_password = config['main']['ftp_password']

def getDuration(then, now, interval = "default"):

    # Returns a duration as specified by variable interval
    # Functions, except totalDuration, returns [quotient, remainder]
    
    duration = now - then # For build-in functions
    duration_in_s = duration.total_seconds() 

    def months():
        return duration_in_s // 2592000 # Seconds in a month=2592000.

    def days():
        return (duration_in_s // 86400 - 30 * months()) % 30# Seconds in a day = 86400

    def hours():
        return (duration_in_s // 3600 - 24 * days()) % 24# Seconds in an hour = 3600

    def minutes():
        return (duration_in_s // 60 - 60 * hours()) % 60# Seconds in a minute = 60

    return {
        'months': int(months()),
        'days': int(days()),
        'hours': int(hours()),
        'minutes': int(minutes()),
    }[interval]

def push_balloon_to_html(balloon_name, telemetry):
    then = None
    
    # search the correct ballon release date from the ini
    for b_name in balloons:
        b_name_balloon = b_name[0]
        b_start = b_name[7]
        #print(b_name_balloon, balloon_name)
        if b_name_balloon == balloon_name:
            #print(b_start)
            then = datetime.datetime.strptime(b_start, "%Y%m%dT%H%M")
            #print(then)

    def add_position(file): # search and replace for the map position
        searchExp = "// #POSITION#"
        replaceExp = f"\t\tnew google.maps.LatLng({telemetry['lat']},{telemetry['lon']}),\n// #POSITION#"
        for line in fileinput.input(file, inplace=1):
            if searchExp in line:
                line = line.replace(searchExp,replaceExp)
            sys.stdout.write(line)
    add_position(balloon_name + '.html');  


    def update_popup(file): # search and replace for the telemetry popup1
        time_now = datetime.datetime.utcnow()
#        time = telemetry['time'].strftime("%d-%b-%Y %H%M")
        time = time_now.strftime("%Y-%m-%d %H%M")
        time_now_delta = time_now.strftime("%Y%m%dT%H%M")
        #print(then)
        #print(telemetry['time'])

        searchExp = "'<p>Updated"
        replaceExp = f"'<p>Updated {time}Z<br />Locator = {telemetry['loc'].upper()}<br />Duration = {getDuration(then,telemetry['time'],'months')}mo {getDuration(then,telemetry['time'],'days')}d {getDuration(then,telemetry['time'],'hours')}h {getDuration(then,telemetry['time'],'minutes')}m<br />Distance = '+ distance +'km<br />Altitude = {telemetry['alt']}m<br />Speed = {telemetry['speed']}kt {round(telemetry['speed']*1.852)}km/h<br />Solar = {round(telemetry['batt'],2)}V, Temp = {round(telemetry['temp'],1)}C, <br />GPS = {int(telemetry['gps'])}, Sats = {int(telemetry['sats'])}</p>'; \n"
        for line in fileinput.input(file, inplace=1):
            if searchExp in line:
                line = line.replace(line,replaceExp)
            sys.stdout.write(line)
    update_popup(balloon_name + '.html');  
    
#    def update_txt(): # add telemetry to the txt file
#        telestr = "Telemetry NOB: %s,%s,%d,%d,%.1f,%.2f,%d,%d\n" % (telemetry['time'].strftime("%d-%b-%Y %H%M"), telemetry['loc'], telemetry['alt'], round(telemetry['speed']*1.852), telemetry['temp'], telemetry['batt'], telemetry['gps'], telemetry['sats'])
#        with open("Output.txt", "a") as text_file:
#            text_file.write(telestr)
#
#    update_txt();	    

    if push_ftp == 'True':
        UPLOAD_FILENAME = balloon_name + '.html'
        session = ftplib.FTP(ftp_server,ftp_username,ftp_password)
        file = open(balloon_name + '.html','rb')  # file to send
        session.storbinary('STOR %s' % UPLOAD_FILENAME, file)                      # send the file
#        session.storbinary('STOR NOB15.html', file)                                # send the file
#        file.close()                                                              # close file and FTP
#        file = open('Output.txt','rb')  # file to send
#        session.storbinary('STOR flights/ICT6_telemetry.txt', file)                        # send the file
        file.close()                                                              # close file and FTP
        session.quit()
        print("HTML page uploaded with FTP")