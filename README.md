# Data-Engineering-Pipeline-in-the-Cloud

Given the desire for sustainable eco-friendly based mobility, simplicity and
accessibility to mobility, Gans has decided to provide a solution and access the
market via an e-scooter sharing system, where users of the service rent the escooters.


The choice of what cities in Europe to provide this service remains an open and
important decision. Also, decisions about how accessible the e-scooters are to
users due to the distribution of the e-scooters by usage are a thing of concern.


This project seeks to answer some of these questions by providing data which
supports the choice of city and the operational time redistribution for
accessibility of the service.

The project wishes to provide foundations to answer these question using the
basic techniques of extraction, transformation and loading/storage as depicted
in the diagram below


![image](https://user-images.githubusercontent.com/103940202/197272648-ac74ef97-00f0-4a3e-8bdd-ae9203b7cb94.png)



# Goals and Objectives of the Project

The goal and objective of the project are;
1. Obtain demographic data about cities in Europe to help the company decide into what cities to expand.
2. Obtain weather and flight information about these cities to judge human traffic frequency, and manage asymmetries which develop due to these activities.
3. Automate the collection of these data.


# Methodology

The method utilized to achieve these objectives and goals are;
1. Data collection; involves scrapping the web for data which might be necessary, and extracting these data (html format) via python library BeautifulSoup. Data is also be collected using APIs via the requests Library in python.
2. Data Storage; the data obtained from the web scrapping and APIs are stored in a local database on MySQL called gans_data_analytics, and as csv files.
3. Data collection automation; this involves using the Amazon Web Services relational database service (RDS), and the Amazon Lambda to setup instances and implement scripts.


The project involved the following important tasks:
1. Webscrapping using requests and beautiful soup. 
2. Data cleaning. 
3. API requests using rapidapis. 
4. Accessing Database in MySQL workbench with python via SQLAlchemy.
5. Connection of local MySQL database to an AWS Cloud database.
6. Implementation of cloud based computing or data extraction on amazon web services (AWS) using the lambda function handler.
7. Transfer of Cloud extracted data to a local MySQL Database.

A technical report is available also for details on implementation. The python codes are also made available in this repository. The project concluded successfully.
