# 2018-Rozwiazanie_Staz-w-zespole-Danych_Poznan
Project was created for internship program at Allegro company. 

Program counts the orders and calulates the average order value (in EUR) for every country. The orders where value wasn't given are not taken into account.
### Implementation
Project was implemented using Scala and Apache Spark on Ubuntu 16.04. It wasn't tested on any other OS.

Firstly, data from *countries_codes.csv* (codes) and from *TED_CN_\** files are read. Next, columns, that hava data we are intrested in, are selected and temporal view is created, to make using SQL commands possible. **ISO_COUNTRY_CODE** column has countries codes and **VALUE_EURO_FIN_2** has final values for the order. Since in some rows there is no country code or it's invalid (ex. instead of country code, there's a zipcode) and in others, there is no given value, the data is cleaned using view that was created from codes. Finally, data is grouped by country and results are printed on the console.

### Prerequisites
- To use the program you need to have [sbt](https://www.scala-sbt.org/1.0/docs/Setup.html) (v1.1.4) and [scala](https://www.scala-lang.org/download/) (v2.11.12) installed.
- This program uses data from [European Union](https://data.europa.eu/euodp/en/data/dataset/ted-csv) listed as *TED - Contract notices $YEAR*. You can download the chosen files and put them in *src/main/resources/* directory or declare how many files you want to use while running the program.
- **WARNING:** the sizes of those files are quite big, be sure if you are going to download many of them
### Running the program
All commands need to be executed in the same directory as *build.sbt* file.

To run the program you need to write in your console:
```
$ sbt run
```
If you want the program to download the files for you, you need to specify how many of them do you want. They'll be downloaded beginning from the year 2006 up. If you want, for example, to get data from two years (2006 and 2007) you need to use the following command:
```
$ sbt "run 2"
```
As a default, the program runs locally using one thread but you can change it using SPARK_MASTER command for example:
```
$ SPARK_MASTER='local[2]' sbt "run 3"
```
for more avalible configurations see [Spark documentation](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)

### Tests
Simple tests are also implemented, to run them use:
```
$ sbt test
```
