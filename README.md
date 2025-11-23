# User-Guide for AirHealth Dashboard

## Description

This project provides an end-to-end framework for forecasting air quality and associated health outcomes across U.S. states. It integrates multiple raw datasets (EPA (pollutant data), NOAA (meteorological data), mobility, vegetation, and flights), performs data cleaning and interpolation, and combines everything into a single monthly dataset by state. 

The package then compares candidate time-series models and selects a final autoregressive LSTM that learns from the past 12 months of air quality and top 5 key features to generate forecasts up to December 2030. 

The forecasted values are used to predict health outcomes for **ischemic heart disease (IHD)** and **chronic pulmonary disease (COPD)** up to December 2030.

In the visualization dashboard, the user can:
- Visulize a **choropleth map** that displays the distribution of a user-selected pollutant across states for a user-chosen month and year.
    - This allows users to visualize how pollution levels vary across the country at a specific point in time. 
- Explore historical trends
- Explore LSTM AR-based future predictions
- Run simple "what-if" simulations to see their impact on health outcomes
- Shows the health outcomes trend for each disease over the years for both historical and predicted data.

## Installation

For installation, clone the project's GitHub repository to a local machine. All data cleaning and modeling workflows are implemented in Jupyter notebooks, so ensure recent Python installation (e.g. >= 3.8.20), pip, and Jupyter Notebook. We recommend creating a virtual or conda environment and then install required dependencies with `pip install`. A `requirements.txt` file is provided for easy installation. This will set up everything needed to open and run the notebooks. 

The visualization dashboard is a D3 dashboard. Any local HTTP server can be used to serve the dashboard files. More information is provided in the execution section.

## Execution

For execution, activate the installed environment and launch Jupyter. Open the notebooks organized in order, and execute them top-to-bottom to reproduce the results or modify parameters as needed. 

To use the interactive dashoard, navigate to the `Visualization` directory and open it with a simple local HTTP server. Once the server is running, go to the corresponding URL to explore historical time-series, model-based forecasts, choropleth maps, and simulation views.
