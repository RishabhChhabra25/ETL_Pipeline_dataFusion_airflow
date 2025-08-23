from faker import Faker
import random
import string
import pandas as pd
from google.cloud import storage
num_employees = 100
fake = Faker()

def generate_fake_employee_data(num_employees):
    print(f"--- Generating data for {num_employees} employees... ---") # Debug print statement
    
    employee_data = []
    
    for i in range(num_employees):
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        employee_data.append({
            'employee_id': fake.unique.uuid4(),
            'first_name': first_name,
            'last_name': last_name,
            'email': f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}",
            'password': fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True),
            'job_title': fake.job(),
            'department': fake.random_element(elements=('Sales', 'Marketing', 'Engineering', 'Human Resources', 'Finance', 'IT')),
            'salary': random.randint(30000, 150000),
            'hire_date': fake.date_between(start_date='-5y', end_date='today'),
            'phone_number': fake.phone_number()
        })
        
    return pd.DataFrame(employee_data)


df_employees = generate_fake_employee_data(num_employees)
print(df_employees)

csv_file_name  = 'fake_employee_data.csv'
df_employees.to_csv(csv_file_name, index = False)
print(f"successfully created : {csv_file_name}")

def upload_to_gcp(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client(project= 'etl-project1-468723')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    
    print(f"file {source_file_name} uploaded to {destination_blob_name}")

bucket_name = 'etl-project1-bucket'
destination_blob_name = 'fake_employee_data.csv'

upload_to_gcp(bucket_name,csv_file_name, destination_blob_name)


