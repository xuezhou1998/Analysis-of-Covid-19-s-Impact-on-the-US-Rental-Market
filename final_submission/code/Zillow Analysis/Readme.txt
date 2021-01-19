# Introduction
This section of analytics focuses on analyzing Zillow Rental Research Data from January 2014 to October 2020, and aims to find out how covid-19 has impacted US rental prices, especially for the states being hit by covid-19 the hardest. The program runs in Apache Pig. 

# Requirements
To run and test this program, it is required to upload Data.csv along with ApartmentRental.pig to Dumbo cluster. To run pig, the command is pig -x local ApartmentRental.pig

# Files
1. ApartmentRental.pig -- main program running the analytics 
2. Data.csv -- raw data to be analyzed.
3. rawCountyCases.csv -- covid-19 cases by county in the US
4. rawStateCases.csv -- covid-19 cases by State in the US
5. rawUSCases.csv -- overall covid-19 cases in the US
6. Output folder -- outputs after running ApartmentRental.pig for monthly analysis (Jan. through Oct.) 
6.1 Example output: Jan -- the format is City, State, Rental Price decrease percentiles. 

# Program Running Steps
1. Upload ApartmentRental.pig to Dumbo 
2. Upload Data.csv to Dumbo
3. Enter command pig -x local ApartmentRental.pig 
4. Collect outputs for further analytics 

# Maintainers
1. Ethan Bai
2. Wanting Xi 
3. Xuezhou Wen 