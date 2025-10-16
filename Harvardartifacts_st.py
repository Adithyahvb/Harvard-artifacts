import streamlit as st
from streamlit_option_menu import option_menu
import json
import requests
import pymysql
import pandas as pd



API_key="b6723c26-d8c2-4695-9717-a7a36c705a7a"
url= "https://api.harvardartmuseums.org/object"

mydb = pymysql.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "harvard_artifacts")

mycursor = mydb.cursor()
data3={}
metadata = []
media = []
colors = []
#classification_data will fetch the data from the API
def classification_data(obj):
    data3=[]
    for i in range(1,26):
        parameters={"apikey":API_key,"size":100,"page":i,"classification":obj}
        response2=requests.get(url,parameters)
        data2=response2.json()
        data3.extend(data2['records'])        
    return data3
    
#Collected data extracted and separated into metadata, media and colors

def data_extraction(x):
    
    for i in x:  # <-- loop over the 'records' key
        # Metadata
        metadata.append(dict(
            Id = i.get('id'),
            title = i.get('title'),
            culture = i.get('culture'),
            period = i.get('period'),
            century = i.get('century'),
            medium = i.get('medium'),
            dimensions = i.get('dimensions'),
            description = i.get('description'),
            department = i.get('department'),
            classification = i.get('classification'),
            accessionyear = i.get('accessionyear'),
            accessionmethod = i.get('accessionmethod')
        ))

        # Media
        media.append(dict(
            objectid = i.get('id'),
            imagecount = i.get('imagecount'),
            mediacount = i.get('mediacount'),
            colorcount = i.get('colorcount'),
            rank = i.get('rank'),
            datebegin = i.get('datebegin'),
            dateend = i.get('dateend')
        ))

        # Colors
        color_details = i.get('colors')
        if color_details:
            for j in color_details:
                colors.append(dict(
                    objectid = i.get('id'),
                    color = j.get('color'),
                    spectrum = j.get('spectrum'),
                    hue = j.get('hue'),
                    percent = j.get('percent'),
                    css3 = j.get('css3')
                ))
    return metadata,media,colors

#Code for table creation in mysql
#creation of artifact_metadata table
def table_creation():
    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS artifact_metadata (
        Id INT PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INT,
        accessionmethod TEXT
    )
    """)
    #Creation of artifact_media table
    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS artifact_media (
        objectid INT,
        imagecount INT,
        mediacount INT,
        colorcount INT,
        ranknum INT,
        datebegin INT,
        dateend INT,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata (Id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    )
    """)
    #Creation of artifact_colors table
    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS artifact_colors (
        objectid INT,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent REAL,
        css3 TEXT,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata (Id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    )
    """)
table_creation() #Invoking the this function for table creation

#This function data_insertion will insert values into table
def data_insertion(metadata,media,colors):
    metaquery = "insert into artifact_metadata (Id,title,culture,period,century,medium,dimensions,description,department,classification,accessionyear,accessionmethod) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mediaquery = "insert into artifact_media (objectid,imagecount,mediacount,colorcount,ranknum,datebegin,dateend) values(%s,%s,%s,%s,%s,%s,%s)"
    colorquery = "insert into artifact_colors (objectid,color,spectrum,hue,percent,css3) values(%s,%s,%s,%s,%s,%s)"
    for i in metadata:
        values1=i.values()
        mycursor.execute(metaquery,list(values1))
    for i in media:
        values2=i.values()
        mycursor.execute(mediaquery,list(values2))
    for i in colors:
        #print(i)
        values3=i.values()
        mycursor.execute(colorquery,list(values3))
    mydb.commit()

#This data_display will display the collected    
def data_display(metadata,media,colors):
    with col1:
        st.header("Meta_data")
        st.json(metadata)

    with col2:
        st.header("Media_data")
        st.json(media)

    with col3:
        st.header("Colors_data")
        st.json(colors)

#The selected query will be passed in query_execution method for execution
def query_execution(myquery):

    mycursor.execute(myquery)
    result=mycursor.fetchall()
    df = pd.DataFrame(result,columns = [i[0] for i in mycursor.description])
    st.dataframe(df)

#Code command for the particular query is selected
def queries_func(my_query):

    if my_query== "1. List all artifacts from the 11th century belonging to Byzantine culture.":

        query1 = "SELECT * FROM artifact_metadata WHERE century = '11th century' AND culture = 'Byzantine'"
        query_execution(query1)

    if my_query== "2. What are the unique cultures represented in the artifacts?":

        query2="select distinct(culture) from artifact_metadata;"
        query_execution(query2)
        
    if my_query== "3. List all artifacts from the Archaic Period.":
        
        query3="select * from artifact_metadata where period = 'Archaic Period';"
        query_execution(query3)

    if my_query== "4. List artifact titles ordered by accession year in descending order.":

        query4="select title from artifact_metadata order by accessionyear DESC;"
        query_execution(query4)

    if my_query== "5. How many artifacts are there per department?":

        query5="SELECT department, COUNT(*) AS artifacts_count_by_department FROM artifact_metadata GROUP BY department;"
        query_execution(query5)

    if my_query== "6. Which artifacts have more than 1 image?":

        query6="SELECT * FROM artifact_media where imagecount > 1;"
        query_execution(query6)

    if my_query== "7. What is the average rank of all artifacts?":

        query7="SELECT AVG(ranknum) FROM artifact_media ;"
        query_execution(query7)

    if my_query== "8. Which artifacts have a higher colorcount than mediacount?":

        query8="SELECT * FROM artifact_media where colorcount > mediacount ;"
        query_execution(query8)

    if my_query== "9. List all artifacts created between 1500 and 1600.":

        query9="SELECT * FROM artifact_media where 1600 > dateend >1500 ;"
        query_execution(query9)

    if my_query== "10. How many artifacts have no media files?":

        query10="SELECT * FROM artifact_media where mediacount=0;"
        query_execution(query10)

    if my_query== "11. What are all the distinct hues used in the dataset?":

        query11="select distinct(hue) from artifact_colors;"
        query_execution(query11)

    if my_query== "12. What are the top 5 most used colors by frequency?":

        query12="SELECT color, COUNT(*) AS frequency FROM artifact_colors GROUP BY color ORDER BY frequency DESC LIMIT 5;"
        query_execution(query12)

    if my_query== "13. What is the average coverage percentage for each hue?":

        query13="SELECT hue, AVG(percent) as average_percentage from artifact_colors GROUP BY hue;"
        query_execution(query13)

    if my_query== "14. List all colors used for a given artifact ID.":

        query14="SELECT color from artifact_colors where objectid = 256257;"
        query_execution(query14)

    if my_query== "15. What is the total number of color entries in the dataset?":

        query15="SELECT COUNT(color) as total_color_entries from artifact_colors;"
        query_execution(query15)

    if my_query== "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.":

        query16="SELECT artifact_metadata.title, artifact_metadata.culture, artifact_colors.hue from artifact_metadata left join artifact_colors on artifact_metadata.Id = artifact_colors.objectid WHERE culture = 'Byzantine';"
        query_execution(query16)

    if my_query== "17. List each artifact title with its associated hues.":

        query17="SELECT artifact_metadata.title, artifact_colors.hue from artifact_metadata inner join artifact_colors on artifact_metadata.Id = artifact_colors.objectid;"
        query_execution(query17)

    if my_query== "18. Get artifact titles, cultures, and media ranks where the period is not null.":
        
        query18="SELECT artifact_metadata.title, artifact_metadata.culture, artifact_metadata.period, artifact_media.ranknum from artifact_metadata inner join artifact_media on artifact_metadata.Id = artifact_media.objectid;"
        query_execution(query18)

    if my_query== "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'.":
        
        query19="SELECT artifact_metadata.title, artifact_media.ranknum, artifact_colors.color FROM artifact_metadata JOIN artifact_media ON artifact_metadata.Id = artifact_media.objectid JOIN artifact_colors ON artifact_metadata.Id = artifact_colors.objectid WHERE artifact_colors.color = 'Grey';"
        query_execution(query19)
    
    if my_query== "20. How many artifacts exist per classification, and what is the average media count for each?":

        query20="SELECT artifact_metadata.classification, COUNT(*) AS total_artifacts, AVG(artifact_media.mediacount) AS average_media_count FROM artifact_metadata INNER JOIN artifact_media ON artifact_metadata.id = artifact_media.objectid GROUP BY artifact_metadata.classification;"
        query_execution(query20)

    if my_query== "21. Categorizing the classification using if condition as old and new collections based on datebegin before 1900":

        query21="Select objectid, datebegin, IF(datebegin < 1800,'Old','New') AS collection_type from artifact_media;"
        query_execution(query21)

    if my_query== "22. categorizing the classification into multiple categories paintings,sculptures,drawings as arts, coins and photographs as collection":

        query22="Select classification, CASE WHEN classification = 'Paintings' THEN 'ARTS' WHEN classification = 'Coins' THEN 'COLLECTION' WHEN classification = 'Drawings' THEN 'ARTS' WHEN classification = 'Sculptures' THEN 'ARTS' ELSE 'COLLECTION' END AS type FROM artifact_metadata;"
        query_execution(query22)

    if my_query== "23. Rounded values for Percentage of the artifact's image area occupied by a color":

        query23="SELECT objectid, color, percent, ROUND(percent,2) AS rounded_percent from artifact_colors;"
        query_execution(query23)

    if my_query== "24. Ranks are assigned to artifact_metadata based on culture using window function":

        query24="select Id, title, culture, classification, rank() OVER (PARTITION BY CULTURE) AS rank_in_culture FROM artifact_metadata;"
        query_execution(query24)

    if my_query== "25. joining all the three tables artifact_metadata,artifact_media,artifact_colors":

        query25="select artifact_metadata.ID, artifact_metadata.title, artifact_metadata.classification, artifact_media.imagecount, artifact_colors.color, artifact_colors.spectrum, artifact_colors.percent FROM artifact_metadata LEFT JOIN artifact_media ON artifact_metadata.Id = artifact_media.objectid LEFT JOIN artifact_colors ON artifact_metadata.id = artifact_colors.objectid;"
        query_execution(query25)

#This method will go to the artifact_metadata table and returns what are the classifications are present
def classification_check():
    mycursor.execute("select distinct(classification) from artifact_metadata")
    result = mycursor.fetchall()
    list = [i[0] for i in result]
    return list

#Headings & Interface and its execution part
st.title("Harvard's Artifacts collection")
st.header('My collections are:')
st.header('Photographs, Drawings, Paintings, Sculptures, Coins')

#Option menu's definition 
choices=['Photographs','Drawings','Paintings','Sculptures','Coins']
classification=st.selectbox("Select your collection",choices,index=None)
menu = option_menu(None,["Select Your Choice","Migrate to SQL","SQL Queries"], orientation="horizontal")    

#"select your choice" option menu
if menu=="Select Your Choice":
    list=classification_check()    
    if st.button('Collect data'):
        if classification:
            if classification not in list:
                data3=classification_data(classification)
                metadata,media,colors=data_extraction(data3)        
                st.write("Data collected")
                col1, col2, col3 = st.columns(3)
                data_display(metadata,media,colors)
            else:
                st.write("Collection already exists, Try different one!")
        else:
            st.error("Please select some collection, then try!")

#"Migrate to SQL" option menu                      
if menu=="Migrate to SQL":
    list=classification_check()    
    st.subheader("Insert the collected data")
    if st.button('Insert'):
        if classification:                        
            if classification not in list:
                st.write(metadata)
                data_insertion(metadata,media,colors)
                st.write("Data inserted successfully!")
            else:
                st.error("Collection already exists!")
        else:
            st.error("Please collect data and then try!")
 
#"SQL Queries" option menu
if menu=="SQL Queries":
    #SQL queires list variable    
    ready_queries=["1. List all artifacts from the 11th century belonging to Byzantine culture.",
                   "2. What are the unique cultures represented in the artifacts?",
                   "3. List all artifacts from the Archaic Period.",
                   "4. List artifact titles ordered by accession year in descending order.",
                   "5. How many artifacts are there per department?",
                   "6. Which artifacts have more than 1 image?",
                   "7. What is the average rank of all artifacts?",
                   "8. Which artifacts have a higher colorcount than mediacount?",
                   "9. List all artifacts created between 1500 and 1600.",
                   "10. How many artifacts have no media files?",
                   "11. What are all the distinct hues used in the dataset?",
                   "12. What are the top 5 most used colors by frequency?",
                   "13. What is the average coverage percentage for each hue?",
                   "14. List all colors used for a given artifact ID.",
                   "15. What is the total number of color entries in the dataset?",
                   "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.",
                   "17. List each artifact title with its associated hues.",
                   "18. Get artifact titles, cultures, and media ranks where the period is not null.",
                   "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'.",
                   "20. How many artifacts exist per classification, and what is the average media count for each?",
                   "21. Categorizing the classification using if condition as old and new collections based on datebegin before 1900",
                   "22. categorizing the classification into multiple categories paintings,sculptures,drawings as arts, coins and photographs as collection",
                   "23. Rounded values for Percentage of the artifact's image area occupied by a color",
                   "24. Ranks are assigned to artifact_metadata based on culture using window function",
                   "25. joining all the three tables artifact_metadata,artifact_media,artifact_colors"]
    
    my_query=st.selectbox("Select your query",ready_queries)
    if st.button('Run query'):        
        queries_func(my_query) #Invoking the queries function
        
            
        
        
        

       
