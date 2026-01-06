import pandas as pd
from datetime import date
import random

output_path = "/opt/airflow/data/patient_census.csv"

records = []
for i in range(1, 5000):
    admit = date(2024, 1, random.randint(1, 20))
    discharge = (
        date(2024, 1, random.randint(admit.day, 28))
        if random.choice([True, False])
        else None
    )

    records.append({
        "patient_id": f"P{i:03}",
        "hospice_id": "H001",
        "admit_date": admit,
        "discharge_date": discharge,
    })

df = pd.DataFrame(records)
df.to_csv(output_path, index=False)
