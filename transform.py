import pandas as pd
import numpy as np
import os

##### ONLINE LEARNING #####
## Read all districts info
online_learning = pd.read_csv("rawData/onlinelearning/districts_info.csv")
online_learning.head()

# Select only district id's and state names
onlineData = online_learning[["district_id", "state"]]
# Replace NaN's with "unknow"
onlineData.loc[:, ["state"]] = onlineData["state"].replace(np.nan, "Unknown")

## Create new dataframe where all information will be placed
allOnline = pd.DataFrame(columns=["state", "date", "pct_access", "engagement_index"])

# For each engagement file, add their info to the df
for file in os.listdir("rawData/onlinelearning/engagement_data"):
    id = int(file.split('.')[0]) # get id from file
    # get state name from id
    state = onlineData[onlineData["district_id"] == id]["state"].values[0]

    # read file
    a = pd.read_csv(f"rawData/onlinelearning/engagement_data/{file}", index_col=None)
    a = a.drop("lp_id", axis=1) # remove non usage columns
    a = a.dropna() # drop all na
    aggData = a.groupby(["time"]).mean() # aggregate by date
    aggData = aggData.reset_index() # remove time from index
    # reset column name
    cols = aggData.columns.tolist()
    cols[0] = "date"
    aggData.columns = cols
    aggData["state"] = state # add state to df
    allOnline = allOnline.append(aggData) # add values to df

# Change 'District Of Columbia' to match with other df
allOnline["state"].replace({"District Of Columbia": "District of Columbia"}, inplace=True)
allOnline = allOnline.drop_duplicates(subset=['state', "date"])

#### COVID-19 cases ####
## Create new dataframe where all information will be placed
allCases = pd.DataFrame(columns=["state", "date", "confirmed", "deaths", "recovered", "active"])

# for each file load daily statistics and add to main df
for file in os.listdir("rawData/csse_covid_19_daily_reports_us"):
    # avoid README file
    if file.split('.')[1] != 'csv':
        continue
    date = pd.to_datetime((file.split('.')[0])) # convert filename to date
    data = pd.read_csv(f"rawData/csse_covid_19_daily_reports_us/{file}")
    # Select desired columns
    data = data[["Province_State", "Confirmed", "Deaths", "Recovered", "Active"]]
    data["date"] = date.strftime("%Y-%m-%d") # add date as column
    data.columns = ["state", "confirmed", "deaths", "recovered", "active", "date"]
    allCases = allCases.append(data) # add rows to df

allCases = allCases.drop_duplicates(subset=['state', "date"])


# display states not in both df
un = allCases["state"].unique()
for state in allOnline["state"].unique():
    if state not in un:
        print(state)


#### Merge online with cases ####
#allData = allOnline.merge(allCases, left_on=["state", "date"], right_on=["state", "date"])
allData = allOnline.merge(allCases, how="inner", on=["state", "date"])

vac = pd.read_csv("rawData/time_series_covid19_vaccine_doses_admin_US.csv")
# delete undesired columns
vac = vac.drop(['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Country_Region', 'Lat', 'Long_', 'Combined_Key'], axis=1)
vac_states = vac["Province_State"].unique()
vac_cols = vac.columns.tolist()

vals = pd.DataFrame(columns=["state", "doses", "date"]) # all rows to be added
for index, row in allData.iterrows():
    if row["state"] in vac_states and row["date"] in vac_cols:
        thisRow = vac[vac["Province_State"] == row["state"]]
        doses = thisRow[row["date"]]
        try:
            doses = int(doses)
        except:
            doses = 0
        vals = vals.append({"state": row["state"], "date": row["date"], "doses": doses}, ignore_index=True)
    elif row["state"] in vac_states:
        vals = vals.append({"state": row["state"], "date": row["date"], "doses": 0}, ignore_index=True)


vals = vals.drop_duplicates(subset=['state', "date"])


#### Merge past merge with vaccine data ####
data = allData.merge(vals, how="left", on=["state", "date"])
data.to_csv("Online_Education_And_Covid.csv", index=False)
