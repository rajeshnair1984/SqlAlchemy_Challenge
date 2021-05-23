from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

#connect to the hawai sqllite

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

connection = engine.connect()

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# list the table names.
Base.classes.keys()

# Save references to each table

Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
# date for analysis
start_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
year_ago = (dt.datetime.strptime(start_date[0], '%Y-%m-%d') - dt.timedelta(days=365)).date()

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# Home page return the information of the different routes
@app.route("/")
def home():
    # List all the route available
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
        )
####################################################
@app.route("/api/v1.0/precipitation")

def precipitation():

    session = Session(engine)

    # query to retrieve the last 12 months of precipitation data.

    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= year_ago ).all()

    session.close()

    prcp_list = []
    for result in results:
        row = {}
        row["date"] = result[0]
        row["prcp"] = result[1]
        prcp_totals.append(row)

    #Return the JSON representation of your dictionary.
    return jsonify(prcp_list)

###########################################################
@app.route("/api/v1.0/stations")

def station():

    session = Session(engine)
    # query to get the stations.
    results = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station)\
        .order_by(func.count(Measurement.station).desc())
    
    session.close()

    stations = []
    for result in results:
        row = {}
        row["station"] = result[0]
        stations.append(row)
    #Return a JSON representation of your dictionary.
    return jsonify(stations)

###########################################################
@app.route("/api/v1.0/tobs")

def tobs():

    session = Session(engine)

    #query for the dates and temperature observations from a year from the last data point

    active_station = session.query(Measurement.station,func.count(Measurement.station)).group_by(Measurement.station)\
    .order_by(func.count(Measurement.station).desc()).first()

    top_station = active_station[0]
    results = session.query(Measurement.date,Measurement.tobs).filter(Measurement.station==top_station).\
    filter(Measurement.date >= year_ago).order_by(Measurement.date.desc()).all()
    
    session.close()

    tobs_date = []
    for result in results:
        row = {}
        row["date"] = result[0]
        row["tobs"] = result[1]

        tobs_date.append(row)

    return jsonify(tobs_date)


@app.route("/api/v1.0/<start>")

def startdate(start):

    session = Session(engine)
    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).all()
    
    session.close()

    temp_stat = []
    for result in results:
        row = {}
        row["TMIN"] = result[0]
        row["TMAX"] = result[1]
        row["TAVG"] = result[2]

        temp_stat.append(row)

    return jsonify(temp_stat)


@app.route("/api/v1.0/<start>/<end>")

def startend(start,end):

    session = Session(engine)
    
    #When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.
    
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    
    session.close()
  
    temp_stat = []
    for result in results:
        row = {}
        row["TMIN"] = result[0]
        row["TMAX"] = result[1]
        row["TAVG"] = result[2]

        temp_stat.append(row)

    return jsonify(temp_stat)

if __name__ == '__main__':
    app.run(debug=True)


