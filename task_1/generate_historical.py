import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
branches = ['Branch1','Branch2','Branch3','Branch4','Branch5']
timestamps = pd.date_range(start='2024-01-01', end='2025-09-15', freq='H').to_pydatetime().tolist()
sample_ts = np.random.choice(timestamps, 10000)

data = {
    'timestamp': sample_ts,
    'branch_id': np.random.choice(branches, 10000),
    'revenue': np.round(np.random.uniform(50, 500, 10000), 2),
    'service_time': np.round(np.random.uniform(5, 20, 10000), 2),
    'rating': np.round(np.random.uniform(3, 5, 10000), 2)
}
df = pd.DataFrame(data)
df.to_csv('historical_orders.csv', index=False)
print("Saved historical_orders.csv (10000 rows)")