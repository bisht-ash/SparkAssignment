from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests
import json
from config import KEY
spark = SparkSession.builder.master("local[2]").appName("pySpark").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def getDataFromUrl():
    # API details
    url = "https://covid-19-india2.p.rapidapi.com/details.php"
    headers = {
        "X-RapidAPI-Key": KEY,
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    return json.loads(response.text)

def trimCheck(state):
    try:
        idx=state.index("*")
        return state[0:idx]
    except:
        return state
    

def cleanData(jsonResponse):
    data=[] # convering the response to list of lists
    for state in jsonResponse:
        l=[]
        dataValues=jsonResponse[state]
        if(isinstance(dataValues,dict)):
            for val in dataValues:
                l.append(dataValues[val])
            
        data.append(l)
    data.pop()      
    data.pop()
    for val in data:
        val[2]=int(val[2]) # converting the the following to integer
        val[3]=int(val[3])
        val[1]=trimCheck(val[1]) # removing the * from some states
        
    return data
    
def getDataFrame(data):    
    rdd=sc.parallelize(data,numSlices=2)
    rowRdd=rdd.map( lambda x: Row(x[0],x[1],x[2],x[3],x[4],x[5])) # converting to row rdd.
    schema= (StructType()
                .add(StructField("slno",IntegerType(),False))
                .add(StructField("state",StringType(),False))
                .add(StructField("confirm",IntegerType(),False))
                .add(StructField("cured",IntegerType(),False))
                .add(StructField("death",IntegerType(),True))
                .add(StructField("total",IntegerType(),True))
            )
    df=spark.createDataFrame(rowRdd,schema=schema)
    return df


jsonResponse=getDataFromUrl()
data=cleanData(jsonResponse)
df=getDataFrame(data)
    
    
