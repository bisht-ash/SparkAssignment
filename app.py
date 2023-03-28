from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pprint import pprint
from flask import Flask, jsonify
from dataframe import df,spark,getDataFromUrl
import json

app = Flask(__name__)
df.createOrReplaceTempView("data")
@app.route('/')
def home():
    return jsonify({'/most_affected_state': "Most affected state among all the states ( total death/total covid cases)",
                    '/least_affected_state': "Least affected state among all the states ( total death/total covid cases)",
                    '/highest_covid_cases': "State with highest covid cases.",
                    '/least_covid_cases': "State with least covid cases.",
                    '/total_cases': "Total cases.",
                    '/most_efficient_state':"State that handled the covid most efficiently( total recovery/ total covid cases).",
                    '/least_efficient_state': "State that handled the covid least efficiently( total recovery/ total covid cases).",
                    '/show_all_data' : "Show all data"})

@app.route('/show_all_data')
def show_all():
    return jsonify(getDataFromUrl())

@app.route('/most_affected_state')
def get_most_affected_state():
    ans=spark.sql("SELECT state, death/confirm AS ans FROM data").orderBy("ans",ascending=False).select("state").limit(1).collect()
    most_affected_state=ans[0][0]
    return jsonify({'most_affected_state': most_affected_state})


@app.route('/least_affected_state')
def get_least_affected_state():
    ans=spark.sql("SELECT state, death/confirm AS ans FROM data").orderBy("ans",ascending=True).select("state").limit(1).collect()
    least_affected_state=ans[0][0]
    return jsonify({'least_affected_state': least_affected_state})

@app.route('/highest_covid_cases')
def get_highest_covid_cases():
    ans=df.orderBy("confirm",ascending=False).select("state","confirm").limit(1).collect()
    get_highest_covid_cases=ans[0][0]
    cases=ans[0][1]
    return jsonify({'get_highest_covid_cases':get_highest_covid_cases,
                    'cases':cases})

@app.route('/least_covid_cases')
def get_least_covid_cases():
    ans=df.orderBy("confirm",ascending=True).select("state","confirm").limit(1).collect()
    get_least_covid_cases=ans[0][0]
    cases=ans[0][1]
    return jsonify({'get_least_covid_cases':get_least_covid_cases,
                    'cases':cases})
@app.route('/total_cases')
def get_total_cases():
    ans=spark.sql("SELECT SUM(confirm) as Total_Cases FROM data").collect()
    cases=ans[0][0]
    return jsonify({'Total Cases':cases})
    
@app.route('/most_efficient_state')
def get_most_efficient_state():
    ans=spark.sql("SELECT state, cured/confirm AS ans FROM data").orderBy("ans",ascending=False).select("state").limit(1).collect()
    most_efficient_state=ans[0][0]
    return jsonify({'most efficient_state':most_efficient_state})

@app.route('/least_efficient_state')
def get_least_efficient_state():
    ans=spark.sql("SELECT state, cured/confirm AS ans FROM data").orderBy("ans",ascending=True).select("state").limit(1).collect()
    least_efficient_state=ans[0][0]
    return jsonify({'most efficient_state':least_efficient_state})

if __name__ == '__main__':
    app.run(debug=True)