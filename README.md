# pybarb

A python package for interacting with [BARB's web API](https://barb-api.co.uk/api-docs).

## About Barb

Barb is the industry’s standard for understanding what people watch.

Barb's hybrid approach integrates people-based panel data with census-level online viewing data. Barb's methodology enables them to deliver inclusive measurement of total identified viewing across all broadcast, VOD and video-sharing platforms, delivered onto and consumed via multiple platforms and devices.

As the past, present and future of total viewing measurement, Barb is uniquely placed to empower transformation of the UK TV and advertising ecosystem, through integrated audience data and actionable insights.

## About the Barb API

The Barb Application Programming Interface (API) enables Barb subscribers and underwriters to access Barb data directly for ingestion into internal tools and systems.

There are several endpoints available that cover advertising campaign analysis, programmes and content, general viewing analysis and descriptive analysis of Barb panel homes.

### For users interested in analysis of advertising campaigns:

- GET: /advertising_spots - Audience ratings data for advertising spots by day, station and panel. Delivered in JSON.
- GET: /spot_audience - Get full viewer level details of specified spot(s). Delivered asynchronously in csv or parquet for conversion to JSON.
- GET: /spot_schedule - Get all spots timings delivered by scheduled station(s). Delivered in JSON.


### For users interested in the analysis of programmes and content:

- GET: /programme_ratings - Get audience ratings data for broadcasted content by day, station and panel. Delivered in JSON.
- GET: /programme_audience - Get all programme audience profiles delivered by scheduled station(s). Delivered asynchronously in csv or parquet for conversion to JSON.
- GET: /programme_schedule - Get all programme timings scheduled by station(s). Delivered in JSON.


### For users interested in the general viewing analysis:

GET: /viewing - Get comprehensive data on viewing sessions by panel(s), station(s) and activities(s). Delivered asynchronously in csv or parquet for conversion to JSON. This is the most comprehensive level of data available via the API.
GET: /audiences_by_time - Get audiences for various time periods by day, station and panel. Delivered in JSON.

### For users interested in descriptive analysis of Barb panel homes and members:

- GET: /panel_members - Get descriptive data for all Barb panel members. Delivered in JSON.
- GET: /households - Get descriptive data for all Barb panel households. Delivered in JSON.

There are additional endpoints for users to look up Metadata for use with the main endpoints. Users are encouraged to familiarise themselves with these.

The data in the API are enriched with metadata from Barb and Clearcast on programmes and adverts.

Barb welcomes feedback from users if they experience problems or have any suggestions for improvements. Please email Jim Jarrett (jim.jarrett@barb.co.uk) in the first instance.

The Barb API is organized around REST. Our API has predictable resource-oriented URLs, accepts form-encoded request bodies, returns JSON-encoded responses, and uses standard HTTP response codes, authentication, and verbs.


## What's in this repository?

### pybarb

pybarb is a python package for interacting with the Barb API. It allows you to connect to an API endpoint, query it, and convert the results into a number of formats including pandas dataframes and csv, excel and json files. It also allows you to write the results to a database using SQLAlchemy. 

- [The package code](https://github.com/coppeliaMLA/pybarb/tree/main/pybarb)
- [The getting started guide](http://www.coppelia.io/pybarb/pybarb/getting_started.html)
- [The getting started guide - jupyter notebook version](https://github.com/coppeliaMLA/pybarb/blob/main/pybarb/getting_started.ipynb)

*Important:* As the licence states, code is provided 'as is'. It is the user's responsibility to check the code and the outputs.

### Jupyter notebooks

This repository also contains a number of jupyter notebooks that demonstrate how to use python and R to interact with the Barb API. 

- [Connecting to the Barb API using python](jupyter_notebooks/connecting_to_the_barb_api_using_python.ipynb)
- [Connecting to the Barb API using R](jupyter_notebooks/connecting_to_the_barb_api_using_R.ipynb)
- [Querying the Barb API using python](jupyter_notebooks/querying_the_barb_api_using_python.ipynb)
- [Querying the Barb API using R](jupyter_notebooks/querying_the_barb_api_using_R.ipynb)
- [Pulling data from the advertising spots endpoint using pybarb](jupyter_notebooks/pulling_data_from_the_advertising_spots_endpoint_using_pybarb.ipynb)
- [Pulling data from the programme ratings endpoint using pybarb](jupyter_notebooks/pulling_data_from_the_programme_ratings_endpoint_using_pybarb.ipynb)
- [Pulling data from the viewing endpoint using pybarb](jupyter_notebooks/pulling_data_from_the_viewing_endpoint_using_pybarb.ipynb)
- [Exporting to a database using pybarb](jupyter_notebooks/exporting_to_a_database_using_pybarb.ipynb)

### A few extra resources

- [Some geojson files for mapping tv regions](assets/tv_regions)

#### Installing pybarb

pybarb is available on pip and can be installed by running:

```
pip install pybarb
```

## If you are more of an R user...

Then check out Neil's [baRb](https://github.com/neilc-itv/baRb) repository!


