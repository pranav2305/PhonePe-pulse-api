import pandas as pd
import requests

BASE_URL = "https://raw.githubusercontent.com/PhonePe/pulse/master/data"
AGG_TRANS = BASE_URL + "/aggregated/transaction/country/india/state"
AGG_USERS = BASE_URL + "/aggregated/user/country/india/state"
START_YR = 2018
END_YR = 2021
STATES = [
    "andaman-&-nicobar-islands",
    "andhra-pradesh",
    "arunachal-pradesh",
    "assam",
    "bihar",
    "chandigarh",
    "chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu",
    "delhi",
    "goa",
    "gujarat",
    "haryana",
    "himachal-pradesh",
    "jammu-&-kashmir",
    "jharkhand",
    "karnataka",
    "kerala",
    "ladakh",
    "lakshadweep",
    "madhya-pradesh",
    "maharashtra",
    "manipur",
    "meghalaya",
    "mizoram",
    "nagaland",
    "odisha",
    "puducherry",
    "punjab",
    "rajasthan",
    "sikkim",
    "tamil-nadu",
    "telangana",
    "tripura",
    "uttar-pradesh",
    "uttarakhand",
    "west-bengal",
]
TRANS_COLS = ["State", "Year", "Quarter", "Name", "Type", "Count", "Amount"]
USER_COLS = ["State", "Year", "Quarter", "Brand", "Count", "Percentage"]
trans_df = pd.DataFrame(columns=TRANS_COLS)
users_df = pd.DataFrame(columns=USER_COLS)


def get_data(url):
    try:
        r = requests.get(url)
        if r.ok:
            return r.json()
        else:
            return None
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


for state in STATES:
    for year in range(START_YR, END_YR+1):
        for quarter in range(1, 5):
            trans = get_data(AGG_TRANS + "/" + state + "/" +
                             str(year) + "/" + str(quarter) + ".json")
            if not trans:
                continue
            for data in trans["data"]["transactionData"]:
                for payment in data["paymentInstruments"]:
                    trans_df.loc[len(trans_df.index)] = [
                        state, year, quarter, data["name"], payment["type"], payment["count"], payment["amount"]]

            users = get_data(AGG_USERS + "/" + state + "/" +
                             str(year) + "/" + str(quarter) + ".json")
            for data in users["data"]["usersByDevice"]:
                users_df.loc[len(users_df.index)] = [
                    state, year, quarter, data["brand"], data["count"], data["percentage"]]

trans_df.to_csv("transactions-data.csv")
users_df.to_csv("users-data.csv")
