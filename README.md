# Final assigment for "Tools Supporting Data Analysis in Python" course - package name is 'project'.

## Description
Using data available on the webpage https://api.um.warszawa.pl/#  the package collects the data about bus positions over
a period of time. Then it performs an analysis of the collected data. Exemplary information that the package can provide:


● How many buses exceeded the speed of 50 km/h (the bus position is updated every minute, we can
approximate the real speed by assuming that the bus is moving during the minute in a straight line).
Locations with a significant percent of busses exceeding that speed limit plotted on a map.
For example:


<img src="https://github.com/KacDom/Final-Assignment-in-DiP/blob/main/speeders.png" width="300" height="200">


● Analysis of punctuality of buses during the observed period (compare actual arrival times at
the bus stops against the schedule).

## The package is installable with pip
```bash
git clone https://github.com/KacDom/Final-Assignment-in-DiP
cd Final-Assignment-in-DiP
python setup.py install
```
## See demo_usage.ipnyb for information how to use the package
