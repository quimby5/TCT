from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
import os
import psycopg2

# We can specify [folder, recursive subdirectories, phototag]
folders=[['/path/to/photos','true','Tag1']]

fileTypes=('.jpg','.jpeg','.JPG','.JPEG')



def get_exif_data(image):
    """Returns a dictionary from the exif data of an PIL Image item. Also converts the GPS Tags"""
    exif_data = {}
    try:
        info = image._getexif()
    except:
        return None
    if info:
        for tag, value in info.items():
            decoded = TAGS.get(tag, tag)
            if decoded == "GPSInfo":
                gps_data = {}
                for t in value:
                    sub_decoded = GPSTAGS.get(t, t)
                    gps_data[sub_decoded] = value[t]

                exif_data[decoded] = gps_data
            else:
                exif_data[decoded] = value

    return exif_data



def _get_if_exist(data, key):
    if key in data:
        return data[key]
    return None



def _convert_to_degress(value):
    """Helper function to convert the GPS coordinates stored in the EXIF to degress in float format"""
    d0 = value[0][0]
    d1 = value[0][1]
    d = float(d0) / float(d1)

    m0 = value[1][0]
    m1 = value[1][1]
    m = float(m0) / float(m1)

    s0 = value[2][0]
    s1 = value[2][1]
    s = float(s0) / float(s1)

    return d + (m / 60.0) + (s / 3600.0)




def get_lat_lon(exif_data):
    """Returns the latitude and longitude, if available, from the provided exif_data (obtained through get_exif_data above)"""
    lat = None
    lon = None

    if "GPSInfo" in exif_data:    
        gps_info = exif_data["GPSInfo"]

        gps_latitude = _get_if_exist(gps_info, "GPSLatitude")
        gps_latitude_ref = _get_if_exist(gps_info, 'GPSLatitudeRef')
        gps_longitude = _get_if_exist(gps_info, 'GPSLongitude')
        gps_longitude_ref = _get_if_exist(gps_info, 'GPSLongitudeRef')

        if gps_latitude and gps_latitude_ref and gps_longitude and gps_longitude_ref:
            lat = _convert_to_degress(gps_latitude)
            if gps_latitude_ref != "N":
                lat = 0 - lat

            lon = _convert_to_degress(gps_longitude)
            if gps_longitude_ref != "E":
                lon = 0 - lon

    return lat, lon




conn = psycopg2.connect("host=hostname dbname=dbname user=username password=password")
curr = conn.cursor()

curr.execute("CREATE TABLE photos(gid serial, path text, filename text, datetime timestamp without time zone, tags text,the_geom geometry(Point,4326),CONSTRAINT photos_pkey PRIMARY KEY (gid )) WITH (OIDS=FALSE);")
curr.execute("ALTER TABLE photos OWNER TO postgres;")

for folder in folders:
    if folder[1] == "true": # start traversing sub-directories
        for (pathz, dirz, filez) in os.walk(folder[0]):
                for files in os.listdir(pathz):
                        if files.endswith(fileTypes):
                            im = Image.open(pathz+'/'+files)
                            exif_data = get_exif_data(im)
                            if exif_data:
                                xy = get_lat_lon(exif_data)
                            if xy != (None,None) and "DateTime" in exif_data:
                                curr.execute("""insert into photos (path, filename, datetime, tags, the_geom) values (%s, %s, %s, %s, st_setsrid(st_makepoint(%s,%s),4326))""", (pathz,files, exif_data["DateTime"].rpartition(' ')[0].replace(':','-') + ' ' + exif_data["DateTime"].rpartition(' ')[2],folder[2],xy[1],xy[0]))
                               
    else: #only do current directory without recursive subdirectories 
        for files in os.listdir(pathz):
            if files.endswith(fileTypes):
                im = Image.open(pathz+'/'+files)
                exif_data = get_exif_data(im)
                if exif_data:
                    xy = get_lat_lon(exif_data)
                if xy != (None,None) and "DateTime" in exif_data:
                    curr.execute("""insert into photos (path, filename, datetime, tags, the_geom) values (%s, %s, %s, %s, st_setsrid(st_makepoint(%s,%s),4326))""", (pathz,files, exif_data["DateTime"].rpartition(' ')[0].replace(':','-') + ' ' + exif_data["DateTime"].rpartition(' ')[2],folder[2],xy[1],xy[0]))


conn.commit()
curr.close()
conn.close()
