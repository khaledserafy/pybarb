# barb_api
A public repository containing code packages and notebook examples for interacting with [BARB's web api](https://barb-api.co.uk/api-docs).

## About Barb

TBC

## About the Barb API

The Barb Application Programming Interface (API) enables Barb subscribers and underwriters to access Barb data directly for ingestion into internal tools and systems.

There are three main endpoints available:

- GET: /audiences_by_time - Audience sizes for various time periods by day, station and panel

- GET: /programme_ratings - Audience ratings data for broadcasted content by day, station and panel

- GET: /advertising_spots - Get the audience ratings data for advertising spots by day, station and panel

There are additional endpoints for users to look up Stations, Panel numbers, Advertisers and Media buying agencies for use with the main endpoints.

The data in the API are enriched with metadata from Barb and Clearcast on programmes and adverts.

# What's in this repository?

## pybarb

pybarb is a python package for interacting with the Barb API. It allows you to connect to an API endpoint, query it, and convert the results into a number of formats including pandas dataframes and csv, excel and json files. It also allows you to write the results to a database using SQLAlchemy. 

- [The package code](python/pybarb)
- [The getting started guide](python/pybarb/getting_started.html)
- [The code documentation]()

## Installing pybarb

pybarb is available on pip and can be installed by running:

```
pip install pybarb
```


## barbR

barbR will be (soon) our R package for interacting with the Barb API. Some R notebooks are already available [here](R/R_notebooks)


## Other resources

This repository also contains

- Jupyter notebooks demonstrating how the Barb API can be accessed in python without using pybarb
- Some geofiles that may be useful for plotting maps using the data accessible with the API


