# FlightsDelayPredictor

The basic problem of this exercise is to create a model capable of predicting the arrival delay
time of a commercial flight, given a set of parameters known at time of take-off. To do that,
publicly available data will be used from commercial USA domestic flights. The main result
of this work will be a Spark application, programmed to perform the following tasks:

● Load the input data, previously stored at a known location.

● Select, process and transform the input variables, to prepare them for training the model.

● Perform some basic analysis of each input variable.

● Create a machine learning model that predicts the arrival delay time.

● Validate the created model and provide some measure of its accuracy.


The dataset consists of a single table with 29 columns. Some of these columns must not be
used, and therefore need to be filtered at the beginning of the analysis. These are:

● ArrTime

● ActualElapsedTime

● AirTime

● TaxiIn

● Diverted

● CarrierDelay

● WeatherDelay

● NASDelay

● SecurityDelay

● LateAircraftDelay


These variables contain information that is unknown at the time the plane takes off and,
therefore, cannot be used in the prediction model.
