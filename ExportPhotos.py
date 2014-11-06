from PIL import Image, ImageDraw, ImageFont
from PIL.ExifTags import TAGS, GPSTAGS
import os
import psycopg2


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




newFolder = '/path/to/outputfolder'
newSize = (1600,1200)  #resize all images to this 
conn = psycopg2.connect("host=hostname dbname=dbname user=username password=password")
curr = conn.cursor()


# select rows from the photos table to process and output
curr.execute("select filepath, filename, date, uniqueid, name, st_x(p.the_geom)::numeric(10,6), st_y(p.the_geom)::numeric(10,6) from photos p left join trails t using (uniqueid) where s_type='TRAILHEAD';")

for pic in curr:
	im = Image.open(pic[0])
	exif_data = get_exif_data(im)
	
	rotation_value = _get_if_exist(exif_data,"Orientation")
	
	#rotate if necessary, and also resize
	if rotation_value == 1:
		im = im.resize(newSize, Image.ANTIALIAS)
	elif rotation_value == 3:
		im = im.resize(newSize, Image.ANTIALIAS)
		im = im.rotate(180)	
	elif rotation_value == 6:
		im = im.resize(newSize, Image.ANTIALIAS)
		im = im.rotate(-90)
	elif rotation_value == 8:
		im = im.resize(newSize, Image.ANTIALIAS)
		im = im.rotate(-270)	

	#Do labelling on the photo at the bottom
	draw = ImageDraw.Draw(im)
	
	#Draw the background rectangle
	draw.rectangle(((0,im.size[1]-20),(im.size[0],im.size[1])),fill=(0,0,0))
	
	#Write the labels. label1 is drawn bottom right, label2 bottom left.	
	font = ImageFont.truetype("LiberationSans-Bold.ttf",20)
	label1 = pic[1] + ' | ' + str(pic[2]) + ' | ' + str(pic[5]) + ' ' + str(pic[6])
	label2 = pic[3] + ' | ' + pic[4]
	draw.text((0,im.size[1]-21),label2, (255,255,255),font=font)
	draw.text((im.size[0]-draw.textsize(label1,font=font)[0],im.size[1]-21),label1, (255,255,255),font=font)
	
	#Save the resize, rotated, labelled photograph	
	im.save(newFolder+'/Trailhead_Signs/'+pic[1])
	

curr.close()
conn.close()