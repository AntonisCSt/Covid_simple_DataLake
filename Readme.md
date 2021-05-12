
# Data Engineering Capstone Project: COVID-19 World Vaccination Progress
#### Daily and Total Vaccination for COVID-19 in the World


### 1 Sources:
* Vaccinations : https://www.kaggle.com/gpreda/covid-world-vaccination-progress

* Demographics : https://www.oecd-ilibrary.org/social-issues-migration-health/health-spending/indicator/english_8643de7e-en

* Covid-19 cases: https://data.world/covid-19-data-resource-hub/covid-19-case-counts/workspace/file?filename=COVID-19+Activity.csv


## 2 Scope the Project and Data decription

#### 2.1 Scope 
What is the goal of this mini-project?
* The goal of this project is to gather data regarding the covid-19 cases, vaccination processes and demographics. We would like to create a DataLake that extracts the data from online sources and loads them into a database that can be used for analytics or any other data app.

What data do we use?

We use data from:
* OECD library regarding the demographics of the countries
* Dataworld for the covid-19 cases in all countries
* Vaccination processes from github sources

What is the end solution look like?

* It will be a etl.py script that will extract the covid data from the online sources and will create partition data in the data/outputs file directory

What tools did we use?

* Due to the big amount of data and the need of computationaly expensive queries, we used spark and aws.

#### 2.2 purpose of the data model
What is the purpose of the data model we create?(what are the questions it will answer?)

* The data model will be able to give information regarding the vaccination progress, covid cases and gdpr info for every country. By connecting each table of the model with the country we can get cross reference of the aformentioned information.

#### 3 Instructions

* Rather simple :) just run the etl.py program.
* In case you want to use the datalake we suggest you to add this repo and connect your app with the partitioned data.

Here are the overview steps of the project:

<img src="Project_overview.png">


Here is the equivalent schema database representation of our partitioned data:

<img src="Schema_representation_of_data_lake.png">

More info: 

This model was chosen in order to separate key factors for the covid vaccinations, case progress and ecomonical factors. All those factors have their own table entity.
The schema above is a equivalent representation if this project was a data werehouse. 
Since we have a datalake we partition those tables in order to be used by any user or app.
#### 4.3 Data dictionary 

**/data** -> contains partitioned data outputs (and possibly in the future inputs)

**/Covid Vaccines Capstone Project.ipynb** -> contains the main code but in a notebook form

**/etl.py** -> contains the main code in .py form. It extracts data from different sources and creates datalake

**/Health spendings per country GDPR percentage.csv** -> contains data regarding the GDPR of each country


#### 5 Project Write Up

This sums our work. Here some things to keep in mind:
* Due to the high volume of the data (covid cases >1 milion instances) we chose to utilize sparks distributed power. The datalake will be able to provide data for queries and other BI tools.

* The data is updated automatically because it comes directly from the sources. Regarding the GDPR data, it can be updated every year. 

* If the data was increased by 100x it would be wize to utilize aws EMR capabilities and run in a powerfull computer.
* The pipeline should be running everyday at ~ 06:00 am since the data is updated before that by the internet sources
* Airflow can be used as well to perform this feat.
* There is no restrction for the amount of user who can use this data.