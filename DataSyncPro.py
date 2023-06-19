import requests as r

import json as js

import pymongo as pm

import time as t

import pandas as pd

import psycopg2 as pg2

import streamlit as st

import re

# Sql Python Connectivity
praveen = pg2.connect(host='localhost', user='postgres', password='root', database='praveen_1')
cursor = praveen.cursor()


# Mongo Python connectivity
praveen_1 = pm.MongoClient('mongodb://praveen:praveenroot@ac-cd7ptzz-shard-00-00.lsdge0t.mongodb.net:27017,ac-cd7ptzz-shard-00-01.lsdge0t.mongodb.net:27017,ac-cd7ptzz-shard-00-02.lsdge0t.mongodb.net:27017/?ssl=true&replicaSet=atlas-ac7cyd-shard-0&authSource=admin&retryWrites=true&w=majority')
db = praveen_1['DataAnalysis']
collection = db['International_Space_Station']




class API2MongoSQL:


    # Title Of Project
    st.title('Welcome   :blue[Tech Geeks] Here  :blue[Praveen]')
    st.header("Let:green[']s See :green[Data]:green[Sync]:green[Pro] Project 👊 ")



    # Method 1 :  API to Mongodb Collection

    def api2mongo(self, gap, times):
        i = 0
        while i < times:
            t.sleep(gap)
            data = r.get('http://api.open-notify.org/iss-now.json')
            collection.insert_one(js.loads(data.text))
            i += 1

        # Total count of collection
        count = [i for i in collection.find()]

        return f"{i} Document inserted "



    # Method 2 : Fetch Mongo Documents

    def fetchMongodoc(self,limit):
        data = [i for i in collection.find()]
        if limit <= 0:
            return f"You Can't  Fetch Any Documents"
        elif limit>len(data):
            return "You Can't  Fetch Any Documents"
        else:
            st.info("Documents",icon='⬇️')
            for i in range(limit):
                st.code(data[i])

            return f"{limit} Documents  Has Retrieved"


    # Method 3 : Mongo Documents Into Sql Records

    def mongo2sql(self,Limit):

        # Coverting Documents into Dataframe
        mongodoc = [i for i in collection.find()]
        Dataframe = pd.DataFrame(mongodoc)

        # Cleaning Dataframe
        Dataframe[['latitude','longitude']] = Dataframe['iss_position'].apply(lambda x : pd.Series([x['latitude'],x['longitude']]))
        Dataframe.drop('iss_position',axis=1,inplace=True)

        Dataframe = Dataframe.reindex(columns =['_id','timestamp','message','latitude','longitude'])


        # limit assigned
        limit = Limit

        # Query TO Insert Values Into SQL
        query = 'insert into international_space_station(id,timestamp,message,latitude,longitude) values(%s,%s,%s,%s,%s);'

        # Getting DataFrame row values
        for n in Dataframe.loc[Dataframe.index].values:

            # Condition Applied For Records Insertion
            if limit > 0: # insert records

                    # Changing Datatype ObjectId Into String
                    value = [str(n[i]) if i == 0 else n[i] for i in range(len(n))]

                    # Duplicate Id verification
                    cursor.execute('select * from International_Space_Station')

                    x = cursor.fetchall()

                    Original_Object_id = [i[0] for i in x]

                    if value[0] not in Original_Object_id :
                        cursor.execute(query, value)
                        praveen.commit()
                        limit -= 1

        cursor.execute('select * from International_Space_Station')
        Current_records = cursor.fetchall()

        if Limit in range(1,len(Current_records)+1):
            return f"{Limit} Records Has Inserted"

        else:
            return f"You Can't Insert Records"


    # Method 4: Fetch Sql Table Records
    def fetchsqlrec(self):
        cursor.execute('select * from international_space_station;')
        records = cursor.fetchall()
        st.info(f"Total Records : {len(records)}")
        Limit=st.number_input('How Many Records You Wanna Fetch', step=1, value=0, format="%d")
        limit = Limit

        # Condition for Records Retrieval
        if st.button("GET"):
            if limit in range(1,len(records)+1):
                st.info("Records", icon='⬇️')
                st.code(f"Index      Id                      Timestamp    Message   Latitude  Longitude")
                for index,values in enumerate(records):
                    if limit >0:
                      st.code(f'  {index}     {values}')
                      limit-=1
                st.success(f"{Limit} Records  Has    Retrieved",icon="✅")
            else:
                st.error("You Can't Fetch records",icon="🚨")



    # Method 5 : Delete Document In Mongo Collection

    def delmongodoc(self):

        st.info(f"Total Documents :  {len([i for i in collection.find()])}")
        st.info("""Option\n\n1. Delete Entire Documents\n2. Delete Limited Documents
              """)
        option = st.number_input('Enter Either 1 or 2',step=1,value=0,format="%d")
        # if st.button("Take Option"):
        if option == 1:
                if st.button("WIPE OUT"):
                    x = [i for i in collection.find()]
                    if len(x) >0:
                        collection.delete_many({})
                        st.success(f"Entire  Documents Has Successfully Deleted",icon="✅")
                        st.snow()
                    else:
                        st.warning("No Documents Exists",icon="🚨")
        elif option == 2:

                x = [i for i in collection.find()]
                if len(x)>0:
                    question_3 = st.number_input('How Many Documents You Wanna Delete', step=1, value=0, format="%d")
                    if st.button("WIPE OUT"):
                        if question_3 > 0:
                                for i in range(question_3):
                                    collection.delete_one({})
                                x = [i for i in collection.find()]
                                st.success(f"{question_3} Documents Has Successfully Deleted ",icon="✅")
                                st.info(f'Available Documents :{len(x)}')
                                st.snow()




                        else:
                            st.error("You can't Delete Documents",icon="🚨")
                else:
                    st.warning("No Documents Exists",icon="🚨")





        else:
            st.warning('Invalid Option Entered', icon="🚨")




    # Method 6 : Delete Sql Records

    def delsqlrec(self):

        cursor.execute('select * from international_space_station; ')
        x = cursor.fetchall()

        st.info(f"\nTotal Records : {len(x)}\n")
        st.info("""Option\n\n1. Delete Entire Records\n2. Delete Limited Records
                      """)
        option=st.number_input('Enter Either 1 or 2',step=1,value=0,format="%d")
        if option in [1,2]:
            if len(x)>0:
                if option == 1:
                    if st.button("DROP"):
                        cursor.execute('delete from international_space_station;')
                        praveen.commit()
                        cursor.execute('select * from international_space_station; ')
                        x = cursor.fetchall()
                        st.success(f"Entire  Records Has Successfully Deleted", icon="✅")
                        st.snow()

                elif option == 2:
                    Limit = st.number_input('How Many Records You Wanna Delete',step=1,value=0,format="%d")
                    if st.button('DROP'):
                        if Limit <= 0:
                            st.error("You Can't Delete Records",icon='🚨')
                        elif Limit > 0:
                            query = f"delete from international_space_station  where id in (select id from international_space_station limit {Limit});"
                            cursor.execute(query)
                            praveen.commit()
                            cursor.execute('select * from international_space_station; ')
                            x = cursor.fetchall()
                            st.success( f"{Limit} Records  Has Successfully Deleted", icon="✅")
                            st.info(f"Available Records : {len(x)}")
                            st.snow()
            else:
                st.error("No Records Exists", icon="🚨")




        else :
             st.warning('Invalid Option Entered',icon="🚨")









# Object Creation

object = API2MongoSQL()

# Execution Starts .......



Input = st.selectbox('',
                       [ "Problem Requirement",'API data to MongoDB collection',
                         'Obtain MongoDB documents',
                         'Export MongoDB data as SQL records',
                         'Fetch Sql Table Records',
                         'Wipe out MongoDB Documents',
                         'Drop SQL records',
                          'Overview',
                         'Halt Execution'])



if Input == 'API data to MongoDB collection':
    gap= st.number_input('Enter Time Gap To fetch API Data ',step=1,value=0,format="%d")
    times = st.number_input('How Many Documents You wanna Insert',step=1,value=0,format="%d")
    x = object.api2mongo(gap,times)
    if times>0 :
        st.success(x, icon="😊")
        st.balloons()
        st.balloons()
        st.balloons()
    else:
        st.info('Please Insert Document', icon="🥺")


elif Input == 'Obtain MongoDB documents':
    data = len([i for i in collection.find()])
    st.info(f'Total Documents : {data}')
    value = st.number_input('How many Documents You Want ', step=1, value=0, format="%d")
    if st.button('GET'):
            y = object.fetchMongodoc(value)
            if y =="You Can't  Fetch Any Documents":
                st.error("You Can't  Fetch Any Documents", icon="🚨")
            else:
                st.success(y,icon="✅")


elif Input == 'Export MongoDB data as SQL records':
     Limit = st.number_input('How Many Records You Wanna Insert :',step=1,value=0,format="%d")
     if st.button("MIGRATE"):
         response = object.mongo2sql(Limit)
         if response == "You Can't Insert Records":
             st.error("You Can't Insert Records", icon="🚨")
         else:
             cursor.execute('select * from International_Space_Station')
             Current_records = cursor.fetchall()
             st.success(response, icon="✅")
             st.balloons()
             st.info(f"Available Records :  {len(Current_records)}")




elif Input == 'Fetch Sql Table Records':
    object.fetchsqlrec()

elif Input == 'Wipe out MongoDB Documents':
   object.delmongodoc()


elif Input == 'Drop SQL records':
    object.delsqlrec()


elif Input == 'Halt Execution':
    st.success("Thank You .... The Process Has Successfully Done",icon='🤝')
    st.info("Skills Covered ✅ ⬇️")
    st.info("""Python (Scripting)\n\nData Collection\n\nMongoDB\n\nSql\n\nAPI integration\n\nData Managment using MongoDB (Atlas) and PostgresSQl\n\nIDE: Pycharm Community Version""")
elif Input=="Overview":
    st.warning("Space Exploration Data Fusion: Unleashing the International Space Station Insights with MongoDB and SQL Integration")

    st.success("In this project , I have transformed the raw API data of ' International Space Station' (ISS)  into   MongoDB   documents , SQL records . With this project we are unlocking the valuable insights that would be helpful for Communication timestamp analysis , message content analysis and more with effective storage power of MongoDB's document-oriented storage and SQL's relational database management")

elif Input=="Problem Requirement":
    st.error("Will You Solve ❓ ⬇️ ")

    st.info("Get ISS API Data then Migrate it into MongoDB Documents , Sql Records  and access it")


