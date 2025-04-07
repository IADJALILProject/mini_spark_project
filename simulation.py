import csv
import random
from faker import Faker
from tqdm import tqdm

fake = Faker()
output_file = "users_csv.csv"
num_rows = 10_00000  # 10 millions

with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['email','age','country','last_login','monthly_spend','subscription_type' ])

    for i in tqdm(range(num_rows), desc="Generating data"):
        email = fake.email()
        age = random.randint(18, 70)
        country = fake.country()
        last_login = fake.date_time_this_decade()
        monthly_spend = round(random.uniform(10, 500), 2)
        subscription_type = random.choice(['free', 'basic', 'premium'])    
        writer.writerow([i, email, age, country,monthly_spend,subscription_type])
        if i % 1000000 == 0:
            print(f"Generated {i} rows")
            print(f"Generated {i} rows")
