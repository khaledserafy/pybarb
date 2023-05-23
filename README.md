# pybarb

A python package for interacting with [BARB's web API](https://barb-api.co.uk/api-docs).

## About Barb

Barb is the industry’s standard for understanding what people watch.

Barb's hybrid approach integrates people-based panel data with census-level online viewing data. Barb's methodology enables them to deliver inclusive measurement of total identified viewing across all broadcast, VOD and video-sharing platforms, delivered onto and consumed via multiple platforms and devices.

As the past, present and future of total viewing measurement, Barb is uniquely placed to empower transformation of the UK TV and advertising ecosystem, through integrated audience data and actionable insights.

## About the Barb API

The Barb Application Programming Interface (API) enables Barb subscribers and underwriters to access Barb data directly for ingestion into internal tools and systems.

There are three main endpoints available:

- GET: /audiences_by_time - Audience sizes for various time periods by day, station and panel

- GET: /programme_ratings - Audience ratings data for broadcasted content by day, station and panel

- GET: /advertising_spots - Get the audience ratings data for advertising spots by day, station and panel

There are additional endpoints for users to look up Stations, Panel numbers, Advertisers and Media buying agencies for use with the main endpoints.

The data in the API are enriched with metadata from Barb and Clearcast on programmes and adverts.

## What's in this repository?

### pybarb

pybarb is a python package for interacting with the Barb API. It allows you to connect to an API endpoint, query it, and convert the results into a number of formats including pandas dataframes and csv, excel and json files. It also allows you to write the results to a database using SQLAlchemy. 

- [The package code](https://github.com/coppeliaMLA/barb_api/tree/pyBARB_dev/python/pybarb)
- [The getting started guide](http://www.coppelia.io/pybarb/pybarb/getting_started.html)

### Jupyter notebooks

Examples of how to use the Barb API for various usecases. *Note these were written before the development of pybarb. It's much easier now!*

- [A broadcaster example](jupyter_notebooks/a_broadcaster_example.ipynb)
- [A media agency example](jupyter_notebooks/a_media_agency_example.ipynb)
- [An example using audiences by time](jupyter_notebooks/an_audiences_by_time_example.ipynb)

### A few extra resources

- [Some geojson files for mapping tv regions](assets/tv_regions)

#### Installing pybarb

pybarb is available on pip and can be installed by running:

```
pip install pybarb
```

## If you are more of an R user...

Then check out Neil's [baRb](https://github.com/neilc-itv/baRb) repository!


