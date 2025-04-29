# Project Title
Healthcare Provider and Treatment Cost Analytics

# Project Goal
The main goal of this project is to build an ETL (Extract, Transform, Load) pipeline that processes healthcare provider and treatment data into a structured star schema database, enabling efficient analytical queries and rich data visualizations.

The project also implements Slowly Changing Dimension (SCD) Type II logic to track changes in doctor affiliations to hospitals over time, and supports incremental data loads, ensuring that only new files are processed after the initial ETL.

Through various analytical queries and visualizations, this project enables:

--Analysis of treatment durations and costs across time periods

--Evaluation of treatment effectiveness

--Understanding provider hospital changes over time

--Discovering patterns in treatment days (weekday/weekend)

--Monitoring healthcare cost trends by month, quarter, and state

# Project Insights
Key Queries Implemented
--Calculate the average treatment duration per treatment type

--Determine total cost of treatments by outcome quarter and year

--Find the most common day of the week for treatments

--Calculate the effectiveness score for treatments by disease

--Get the total number of treatments and average treatment cost for each provider

--Calculate monthly treatment costs for 2024 and 2025

--Identify doctors who changed affiliated hospitals within the last 6 months

# Visualizations Created
--Monthly Treatment Cost Trend (2024 & 2025)

--Effectiveness Score Distribution

--Total Cost of Treatments by State

--Number of Treatments on Weekend vs Weekday

--Quarterly Treatment Volume by Year

--Effectiveness Score by Treatment Type and Outcome Day

# How to Use the Project

**1. Clone the Repository**

Import the project from GitHub:
git clone https://github.com/avinash8898/Healtcare_Provider_And_Cost_Tracking_ETL.git

**2. Install Dependencies**

Install the required Python libraries:

pip install seaborn
pip install mplcursors
!pip install plotly
Or install all at once using:
pip install -r requirements.txt

**3. Run the ETL Script**

Command:
python scripts/run_etl.py

Purpose:
This script extracts data from raw CSV files, transforms the data, and loads it into an SQLite database.
It ensures that only new files (incremental loading) are processed after the first run.

**4. Create Database Schema**

Command:
python scripts/Create_Schema.py

Purpose:
This script creates the star schema database structure (Fact and Dimension tables).
It also triggers Provider_SCD.py internally to apply SCD Type II logic on the Provider dimension:
Tracks doctors' affiliated hospital changes over time by maintaining historical records.

**5. Create Indexes**

Command:
python scripts/Create_Indexes.py

Purpose:
Drops existing indexes if they exist, then creates new indexes on important fields (e.g., Provider_ID, Valid_From, Treatment Dates, Cost, etc.).
This improves query performance especially when working with large datasets.

**6. Access Analytical Notebooks**

Navigate to the /notebooks directory and open the following notebooks:
query_db.ipynb
Data_Visualization.ipynb


These notebooks allow you to:
Perform analytical queries
Visualize treatment trends
Analyze doctor affiliation history
Explore treatment cost and outcome patterns

**7. Incremental Data Load Support**

This project is incremental:
After the initial ETL run, if new raw files are added to /Healthcare_ETL_Project/raw_data/, running run_etl.py again will only process the new files.
Already processed files are tracked in processed_files.txt.

**8. Docker Support**

You can run the entire environment using Docker for easy setup and reproducibility.
Dockerfile and docker-compose.yml are provided.

Spin up the environment with:
docker-compose up
This will create a containerized environment with all dependencies installed and allow you to access Jupyter notebooks via browser.

# Feature Description

-'id': Unique Identifier.
-'start_date': The timestamp without time zone value when the treatment was initiated.
-'completion_date': The timestamp without time zone value when the treatment was completed
-'outcome_status': Status can be successful, partially-successful or others.
-'outcome_date': The timestamp without time zone value when the outcome after the completion_date was declared.
-'duration_in_days': Difference between start_date and completion_date
-'cost': The cost is considered to be in INR.
-'type': The kind of treatment provided which could be therapeutic, surgical, etc.
-'full_name': First Name and Last Name of practitioner / specialist.
-'speciality_id': The unique identifier of the specialty they have studies, in order to treat patients accordingly.
-'speciality_name': Name of the specialisation.
-'affiliated_hospital': The name of the Hospital they're working in.
-'country': United States
-'state': California, Massachusetts, Chicago, etc.
-'city': The 5 cities per states are chosen at random.
-'full_name': First Name and Last Name of the Patient.
-'gender': Male or Female.
-'age': Numeric Value ranging from 18 to 80.
-'speciality_id': Refers to the speciality of the provider's speciality_id that can treat this disease as they have specialized in it.
-'name': Name of the disease
-'type': Specifies type for the disease like acute, infectious, non-infectious, etc.
-'severity': The severity could be moderate, severe, etc.
-'transmission_mode': How the disease is generally transmitted.
-'mortality_rate': A decimal value denoting the likelihood to live.
