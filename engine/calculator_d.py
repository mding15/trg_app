import pandas as pd
import numpy as np

# from trg import config
# import trg.utils.hdf_utils as hdf


# Files used in the test
'''
TEST_POSITION_FILE       = config.TEST_DIR / 'data' /'test_positions.csv'
TEST_RISKMAP_FILE        = config.TEST_DIR / 'data' /'RiskMap.csv'
TEST_DISTRIBUTION_FILE   = config.TEST_DIR / 'data' /'dist.h5'
'''



input_df=pd.read_excel("./Engine.xlsx", sheet_name="Input")
Riskmap_df=pd.read_excel("./Engine.xlsx", sheet_name="RiskMap")
distribution_df=pd.read_excel("./Engine.xlsx", sheet_name="Distribution")


# Step 1: get calculator data
# TODO

# Step 2: calculate risk
# Load the data
data = {
    "PositionID": [1, 2, 3, 4, 4, 5, 5, 6, 6],
    "SecurityID": ['T00001', 'T00002', 'T00003', 'T00004', 'T00004', 'T00005', 'T00005', 'T00006', 'T00006'],
    "ProductType": ['STOCK', 'STOCK', 'STOCK', 'BOND', 'BOND', 'BOND', 'BOND', 'BOND', 'BOND'],
    "RiskType": ['DailyReturn', 'DailyReturn', 'DailyReturn', 'Spread', 'RiskFreeRate', 'Spread', 'RiskFreeRate', 'Spread', 'RiskFreeRate'],
    "RiskFactor": ['R00001', 'R00002', 'R00003', 'R00004', 'R00006', 'R00004', 'R00007', 'R00005', 'R00007'],
    "Sensitivity": [490077, 412587, 48074, 3.0, 3.0, 213.9, 213.9, 271.9, 271.9],
    "P/L vector": [1, 2, 3, 4, 6, 4, 7, 5, 7]
}
df = pd.DataFrame(data)

# Load the distribution of risk factor data
# distribution_df = pd.read_excel('distribution.xlsx')  # Load your Excel file with distribution data

# Initialize a list to store the risk for the position with PositionID equal to 1 (T00001)
risk_results = []

# Get the distribution values for the position with PositionID equal to 1 (T00001)
distribution_values = distribution_df['R00001']  # Assuming the column names start from R00001
# print (distribution_values)

# Calculate risk for each distribution value
for distribution_value in distribution_values:
    sensitivity = df.loc[(df['PositionID'] == 1) & (df['RiskFactor'] == 'R00001'), 'Sensitivity'].values[0]
    risk = sensitivity * distribution_value * df.loc[df['PositionID'] == 1, 'P/L vector'].values[0]
    risk_results.append(risk)

print(risk_results)




# Step 3: Calculate VaR
# Question: what VaR shall be positive?
var = abs(np.percentile(risk_results, 5))
print(var)
