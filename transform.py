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
allData = pd.DataFrame(columns=["state", "date", "pct_access", "engagement_index"])

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
    allData = allData.append(aggData) # add values to df
