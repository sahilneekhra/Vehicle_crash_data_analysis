
# Vehicle Accidents Analysis




## Overview
This project analyzes vehicle accidents data using PySpark, focusing on various analytics tasks based on provided datasets. It leverages Spark's distributed processing capabilities to handle large-scale data efficiently.
## Project Structure
```
├── data
│   ├── input                                                                       
│   │   ├── Charges_use.csv
│   │   ├── Damages_use.csv
│   │   ├── Endorse_use.csv
│   │   ├── Primary_Person_use.csv
│   │   ├── Restrict_use.csv
│   │   └── Units_use.csv
│   └── output
│       ├── Analysis1.csv                                                           # Output of Analysis 1
│       ├── Analysis2.csv                                                           # Output of Analysis 2
│       ├── Analysis3
│       │   ├── ._SUCCESS.crc
│       │   ├── .part-00000-669ca00a-52a7-4c09-a80a-23147dfab209-c000.csv.crc
│       │   ├── _SUCCESS
│       │   └── part-00000-ee1c8122-0c4a-438b-9e0d-faefaf53dc7e-c000.csv            # Output of Analysis 3
│       ├── Analysis4.csv                                                           # Output of Analysis 4
│       ├── Analysis5.csv                                                           # Output of Analysis 5
│       ├── Analysis6
│       │   ├── ._SUCCESS.crc
│       │   ├── .part-00000-b281ee20-aeac-460a-a632-040ce7e1df9a-c000.csv.crc
│       │   ├── _SUCCESS
│       │   └── part-00000-f2fc7bc8-373e-45aa-a379-a364f695195e-c000.csv            # Output of Analysis 6
│       ├── Analysis7
│       │   ├── ._SUCCESS.crc
│       │   ├── .part-00000-b024d95f-4e02-425e-ab52-cfc2d47eb8f2-c000.csv.crc
│       │   ├── _SUCCESS
│       │   └── part-00000-9e24c9c5-1f3f-473a-bee5-306782dcaff5-c000.csv            # Output of Analysis 7
│       ├── Analysis8
│       │   ├── ._SUCCESS.crc
│       │   ├── .part-00000-251e4d5d-b176-4d20-b6f8-6c045dd1dd89-c000.csv.crc
│       │   ├── _SUCCESS
│       │   └── part-00000-276a37da-529f-4efb-b1bd-0a7d6fc867b4-c000.csv            # Output of Analysis 8
│       ├── Analysis9.csv                                                           # Output of Analysis 9
│       └── Analysis10
│           ├── ._SUCCESS.crc
│           ├── .part-00000-8d3ec76b-97de-46a2-bb67-e838b43e143b-c000.csv.crc
│           ├── _SUCCESS
│           └── part-00000-cf04592a-8be9-4b3f-a697-8fdb1ff8bdb2-c000.csv            # Output of Analysis 10
├── docs
│   ├── BCG_Case_Study_CarCrash_Updated_Questions.docx                              # Problem Statement
│   └── Data Dictionary.xlsx                                                        
├── requirements.txt                                                                # Python dependencies
├── resources
│   └── config.py
├── src                                                                             # Source code directory
│    ├── __init__.py
│    ├── main
│    │   ├── data_processor.py
│    │   └── transformation
│    │       └── analysis.py                                                        # Analytics tasks implementations
│    └── utility
│        ├── logger_config.py                                                       # logger configuration
│        └── spark_session.py                                                       # Initialize Spark Session
├── main.py                                                                         # Main script for running PySpark jobs 
├── Case_Study_Application.sh                                                       # Script to run projecct using spark-submit in linux 
├── Case_Study_Application.bat                                                      # Script to run projecct using spark-submit in Windows
├── README.md
```
## Setup Instructions
### Prerequisites
Python (3.12) and Pip installed\
PySpark 3.5.1

### Installation
1. Clone the repository:
```
git clone https://github.com/sahilneekhra/Case_Study_BCG.git
cd Case_Study_BCG
```
2. Install dependencies:
```
pip install -r requirements.txt
```

### Running the Project
1. Ensure Spark is installed, and environment variables like HADOOP_HOME are configured.
2. Change input/Output path if required
3. If using Case_Study_Application.sh to run, configure spark-submit accordingly
4. To run, Run the main script 
```
python main.py
```
or if running spark-submit in windows
```
Case_Study_Application.bat
```
or if running spark-submit in linux
```
bash Case_Study_Application.sh
```

## Analytics Tasks
Application should perform below analysis and store the results for each analysis.
1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Final Results
Output of all Analysis is inside data/Output/ directory


## Authors

- [@sahilneekhra](https://github.com/sahilneekhra)

