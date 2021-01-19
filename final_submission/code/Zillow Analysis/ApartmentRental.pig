--load the csv file using commaa separator
--Now we will rename the columns to simplify future data manipulations
data = load 'Data.csv' using PigStorage (',')
as (zipCode:chararray,
    dummy:int,
   City:chararray,
   State:chararray,
   Jan_2014:int,
   Feb_2014:int,
   Mar_2014:int,
   Apr_2014:int,
   May_2014:int,
   Jun_2014:int,
   Jul_2014:int,
   Aug_2014:int,
   Sep_2014:int,
   Oct_2014:int,
   Nov_2014:int,
   Dec_2014:int,
   Jan_2015:int,
   Feb_2015:int,
   Mar_2015:int,
   Apr_2015:int,
   May_2015:int,
   Jun_2015:int,
   Jul_2015:int,
   Aug_2015:int,
   Sep_2015:int,
   Oct_2015:int,
   Nov_2015:int,
   Dec_2015:int,
   Jan_2016:int,
   Feb_2016:int,
   Mar_2016:int,
   Apr_2016:int,
   May_2016:int,
   Jun_2016:int,
   Jul_2016:int,
   Aug_2016:int,
   Sep_2016:int,
   Oct_2016:int,
   Nov_2016:int,
   Dec_2016:int,
   Jan_2017:int,
   Feb_2017:int,
   Mar_2017:int,
   Apr_2017:int,
   May_2017:int,
   Jun_2017:int,
   Jul_2017:int,
   Aug_2017:int,
   Sep_2017:int,
   Oct_2017:int,
   Nov_2017:int,
   Dec_2017:int,
   Jan_2018:int,
   Feb_2018:int,
   Mar_2018:int,
   Apr_2018:int,
   May_2018:int,
   Jun_2018:int,
   Jul_2018:int,
   Aug_2018:int,
   Sep_2018:int,
   Oct_2018:int,
   Nov_2018:int,
   Dec_2018:int,
   Jan_2019:int,
   Feb_2019:int,
   Mar_2019:int,
   Apr_2019:int,
   May_2019:int,
   Jun_2019:int,
   Jul_2019:int,
   Aug_2019:int,
   Sep_2019:int,
   Oct_2019:int,
   Nov_2019:int,
   Dec_2019:int,
   Jan_2020:int,
   Feb_2020:int,
   Mar_2020:int,
   Apr_2020:int,
   May_2020:int,
   Jun_2020:int,
   Jul_2020:int,
   Aug_2020:int,
   Sep_2020:int,
   Oct_2020:int);

--find out the months where prices dropped the most for states having the most covid-cases
--The states are: 'CA', 'TX', 'FL', 'IL', 'NY', 'GA', 'OH', 'MI', 'PA', 'WI'
--We trim the attribute just in case there is space before and after the attribute in data
top10State = filter data by TRIM(State) IN('CA', 'TX', 'FL', 'IL', 'NY', 'GA', 'OH', 'MI', 'PA', 'WI');

--deleting zip code as we will focus on measuring on city level.
top10State2020 = foreach top10State generate City, State,
Jan_2020,Feb_2020,Mar_2020,Apr_2020,May_2020,Jun_2020,Jul_2020,Aug_2020,Sep_2020,Oct_2020;

cityGroup = group top10State2020 by City;
cityAverage = foreach cityGroup generate flatten(top10State2020.City) as City,flatten(top10State2020.State) as State,AVG(top10State2020.Jan_2020) as m1,AVG(top10State2020.Feb_2020) as m2,AVG(top10State2020.Mar_2020) as m3,AVG(top10State2020.Apr_2020)as m4,AVG(top10State2020.May_2020)as m5,AVG(top10State2020.Jun_2020)as m6,AVG(top10State2020.Jul_2020)as m7,AVG(top10State2020.Aug_2020)as m8,AVG(top10State2020.Sep_2020)as m9,AVG(top10State2020.Oct_2020)as m10;
cityAverageB2020 = distinct cityAverage;

--Now we have the average rental prices within respective cities, we will use the data and calculate
--which cities/months experience the largest price drops compared with their average
--we will try to compare the price changes of 2020 by month with 2019
top10State2019 = foreach top10State generate City, State,
Jan_2019,Feb_2019,Mar_2019,Apr_2019,May_2019,Jun_2019,Jul_2019,Aug_2019,Sep_2019,Oct_2019;
cityGroup2019 = group top10State2019 by City;
cityAverage2019 = foreach cityGroup2019 generate flatten(top10State2019.City) as City,flatten(top10State2019.State) as State,AVG(top10State2019.Jan_2019) as m1,AVG(top10State2019.Feb_2019) as m2,AVG(top10State2019.Mar_2019) as m3,AVG(top10State2019.Apr_2019)as m4,AVG(top10State2019.May_2019)as m5,AVG(top10State2019.Jun_2019)as m6,AVG(top10State2019.Jul_2019)as m7,AVG(top10State2019.Aug_2019)as m8,AVG(top10State2019.Sep_2019)as m9,AVG(top10State2019.Oct_2019)as m10;
cityAverageB2019 = distinct cityAverage2019;

--price change percentage per month
cogroupData = cogroup cityAverageB2020 by City, cityAverageB2019 by City;
--compare the difference of these two bags
flattenCogroup = foreach cogroupData generate flatten(cityAverageB2020.City) as City,flatten(cityAverageB2020.State) as State,
flatten(cityAverageB2020.m1) as a1,
flatten(cityAverageB2019.m1) as b1,
flatten(cityAverageB2020.m2) as a2,
flatten(cityAverageB2019.m2) as b2,
flatten(cityAverageB2020.m3) as a3,
flatten(cityAverageB2019.m3) as b3,
flatten(cityAverageB2020.m4) as a4,
flatten(cityAverageB2019.m4) as b4,
flatten(cityAverageB2020.m5) as a5,
flatten(cityAverageB2019.m5) as b5,
flatten(cityAverageB2020.m6) as a6,
flatten(cityAverageB2019.m6) as b6,
flatten(cityAverageB2020.m7) as a7,
flatten(cityAverageB2019.m7) as b7,
flatten(cityAverageB2020.m8) as a8,
flatten(cityAverageB2019.m8) as b8,
flatten(cityAverageB2020.m9) as a9,
flatten(cityAverageB2019.m9) as b9,
flatten(cityAverageB2020.m10) as a10,
flatten(cityAverageB2019.m10) as b10;

--below will calculate the price change percentage compared with year of 2019.
diff = foreach flattenCogroup generate City, State, (a1-b1)/a1 as d1,
(a2-b2)/a2 as d2,
(a3-b3)/a3 as d3,
(a4-b4)/a4 as d4,
(a5-b5)/a5 as d5,
(a6-b6)/a6 as d6,
(a7-b7)/a7 as d7,
(a8-b8)/a8 as d8,
(a9-b9)/a9 as d9,
(a10-b10)/a10 as d10;

--Now we want to figure out the cities that had the biggest drop by month
--For example, January.
--diffGroup = group diff ALL;
--janChange = foreach diffGroup generate diff.City, MIN (diff.d1);
change01 = order (foreach diff generate City, State,d1) by d1 asc;
change01Filtered = filter change01 by d1<0;
store change01Filtered into 'month1Change.txt';
change02 = order (foreach diff generate City, State,d2) by d2 asc;
change02Filtered = filter change02 by d2<0;
store change02Filtered into 'month2Change.txt';
change03 = order (foreach diff generate City, State,d3) by d3 asc;
change03Filtered = filter change03 by d3<0;
store change03Filtered into 'month3Change.txt';
change04 = order (foreach diff generate City, State,d4) by d4 asc;
change04Filtered = filter change04 by d4<0;
store change04Filtered into 'month4Change.txt';
change05 = order (foreach diff generate City, State,d5) by d5 asc;
change05Filtered = filter change05 by d5<0;
store change05Filtered into 'month5Change.txt';
change06 = order (foreach diff generate City, State,d6) by d6 asc;
change06Filtered = filter change06 by d6<0;
store change06Filtered into 'month6Change.txt';
change07 = order (foreach diff generate City, State,d7) by d7 asc;
change07Filtered = filter change07 by d7<0;
store change07Filtered into 'month7Change.txt';
change08 = order (foreach diff generate City, State,d8) by d8 asc;
change08Filtered = filter change08 by d8<0;
store change08Filtered into 'month8Change.txt';
change09 = order (foreach diff generate City, State,d9) by d9 asc;
change09Filtered = filter change09 by d9<0;
store change09Filtered into 'month9Change.txt';
change010 = order (foreach diff generate City, State,d10) by d10 asc;
change010Filtered = filter change010 by d10<0;
store change010Filtered into 'month10Change.txt';

allOutput = union change01Filtered, change02Filtered, change03Filtered,
change04Filtered, change05Filtered, change06Filtered, change07Filtered,
change08Filtered, change09Filtered, change010Filtered;

store allOutput into 'allOutput.txt';


--top10 = order (limit cityChangePercentage 20) by City;
dump allOutput;
