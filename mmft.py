import csv
import datetime
import pygeoip
from cgitb import handler

"""hack for unicode"""
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

"""geoloc databases"""
gi = pygeoip.GeoIP('/usr/share/GeoIP/GeoIPOrg.dat')
gic = pygeoip.GeoIP('/usr/share/GeoIP/GeoIP.dat')
gicc = pygeoip.GeoIP('/usr/share/GeoIP/GeoIPCity.dat')

"""globals"""
handler2 = open('mmdb.txt','r')
namedict = {}
chaneldict = {}

class logger:
    """a small logging class for outputting data"""
    """iso,username,ip,geoinfo['Organization'],geoinfo['City'],geoinfo['Country']"""
    def __init__(self,ofilename,header):
        self.filename = ofilename
        self.header = header
        self.load()

    def load(self):
        with open(self.filename, 'wb') as f:
            w = csv.DictWriter(f, self.header)
            w.writeheader()

    def write_line(self,line):
        with open(self.filename, 'a') as f:
            w = csv.DictWriter(f, self.header)
            w.writerow(line)

def enrich(IP):
    data = {}
    try:
        data['Organization'] = gi.org_by_addr(IP)
    except:
        data['Organization'] = "False"
    try:
        data['Country'] = gic.country_code_by_addr(IP)
    except:
        data['Country'] = "False"
    try:
        #print gicc.record_by_addr(data['Src_ip'])['city']
        data['City'] = gicc.record_by_addr(IP)['city']
    except:
        data['City'] = "False"
    return(data)

def get_channels():
    """
    --
    -- Data for Name: channels; Type: TABLE DATA; Schema: public; Owner: mmuser
    --
    """
    #a = logger('channels.txt',['Timestamp','Username','IP','Org','City','Country'])
    handler2.seek(0)
    count=0
    foundtable = False
    for lines in handler2:
        if "Data for Name: channels;" in  lines.strip():
            #print "Start Table",count
            starttable = count
            foundtable = True
        if foundtable:
            if lines.strip() == "\.":
                #print "End Table",count
                stoptable = count
                foundtable = False
        count+=1
    """COPY channels (id, createat, updateat, deleteat, teamid, type, displayname, name, header, purpose, lastpostat, totalmsgcount, extraupdateat, creatorid) FROM stdin;"""
    handler2.seek(0)
    for i, line in enumerate(handler2):
        if i > starttable+3 and i < stoptable:
            data = line.strip().split("\t")
            """if a direct chat"""
            if data[5] == 'D':
                name0, name1 =  data[7].split('__')
                try:
                    dname0 =  namedict[name0]
                except:
                    dname0 =  name0

                try:
                    dname1 =  namedict[name1]
                except:
                    dname1 =  name1
                name = dname0+"__"+dname1
            else:
                name = data[7]
            chaneldict[data[0]] = {'teamid':data[4],
                                   'type':data[5],
                                   'displayname':data[6],
                                   'name':name,
                                   'totalmsgcount':data[11],
                                   }
            """
            print data[0],{'teamid':data[4],
                                   'type':data[5],
                                   'displayname':data[6],
                                   'name':name,
                                   'totalmsgcount':data[11],
                                   }
            """
def get_posts():
    """
    --
    -- Data for Name: channels; Type: TABLE DATA; Schema: public; Owner: mmuser
    --
    """
    b = logger('posts.txt',['Timestamp','Username','Channel','Post'])
    handler2.seek(0)
    count=0
    foundtable = False
    for lines in handler2:
        if "-- Data for Name: posts;" in  lines.strip():
            #print "Start Table",count
            starttable = count
            foundtable = True
        if foundtable:
            if lines.strip() == "\.":
                #print "End Table",count
                stoptable = count
                foundtable = False
        count+=1
    """COPY channels (id, createat, updateat, deleteat, teamid, type, displayname, name, header, purpose, lastpostat, totalmsgcount, extraupdateat, creatorid) FROM stdin;"""
    handler2.seek(0)
    for i, line in enumerate(handler2):
        if i > starttable+3 and i < stoptable:
            data = line.strip().split("\t")
            #print data
            try:
                dname =  namedict[data[4]]
            except:
                dname =  data[4]
            #print dname
            
            try:
                dchan =  chaneldict[data[5]]
            except:
                dchan =  data[5]
            #print dchan['name']
            post =  {'Timestamp':epoch(data[1]),'Username':dname,'Channel':dchan['name'],'Post':data[9]}
            b.write_line(post)

def get_files():
    """
    --
    -- Data for Name: fileinfo; Type: TABLE DATA; Schema: public; Owner: mmuser
    COPY fileinfo (id, creatorid, postid, createat, updateat, deleteat, path, thumbnailpath, previewpath, name, extension, size, mimetype, width, height, haspreviewimage) FROM stdin;

    --
    """
    d = logger('files.txt',['Timestamp','Username','Filename','Size'])
    handler2.seek(0)
    count=0
    foundtable = False
    for lines in handler2:
        if "-- Data for Name: fileinfo;" in  lines.strip():
            #print "Start Table",count
            starttable = count
            foundtable = True
        if foundtable:
            if lines.strip() == "\.":
                #print "End Table",count
                stoptable = count
                foundtable = False
        count+=1
    """COPY channels (id, createat, updateat, deleteat, teamid, type, displayname, name, header, purpose, lastpostat, totalmsgcount, extraupdateat, creatorid) FROM stdin;"""
    handler2.seek(0)
    for i, line in enumerate(handler2):
        if i > starttable+3 and i < stoptable:
            data = line.strip().split("\t")
            #print data[11]
            try:
                dname =  namedict[data[1]]
            except:
                dname =  data[1]
            dtime = epoch(data[3])
            #print dname
            post = {"Timestamp":dtime,"Username":dname,"Filename":data[9],"Size":data[11]}
            d.write_line(post)

def epoch(seconds):
    ts = float(int(seconds)/1000)
    dt = datetime.datetime.utcfromtimestamp(ts)
    iso_format = dt.isoformat() + 'Z'
    return(iso_format)

def loadnames():
    for lines in handler2:
        if "mention_keys\":" in lines:
            data = lines.strip().split('\t')
            #print data[4]
            namedict[data[0]] = data[4]

def two():
    c = logger('logins.txt',['Timestamp','Username','IP','Org','City','Country'])
    handler2.seek(0)
    for lines in handler2:
        items = {}
        if "/api/v3/users/login" in lines:
            if "success" in lines:
                data = lines.strip().split('\t')
                user = data[2]
                ip = data[5]
                ts = data[1]
                try:
                    username =  namedict[user]
                except:
                    username = user
                iso = epoch(ts)
                geoinfo = enrich(ip)
                items['Timestamp'] = iso
                items['Username'] = username
                items['IP'] = ip
                items['Org'] = geoinfo['Organization']
                items['City'] = geoinfo['City']
                items['Country'] = geoinfo['Country']
                c.write_line(items)
                #['Timestamp','Username','IP','Org','City','Country']
                #print iso,username,ip,geoinfo['Organization'],geoinfo['City'],geoinfo['Country']

loadnames()
two()
get_channels()
get_posts()
get_files()
