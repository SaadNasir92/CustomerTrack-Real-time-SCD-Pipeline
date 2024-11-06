from faker import Faker
import csv
from datetime import datetime as dt
import time

RECORD_COUNT = 10000
fake = Faker()


def update_time_stamp():
    current_time = dt.now().strftime("%Y%m%d%H%M%S")
    return current_time


def create_csv_file(time_stamp):
    with open(
        f"nifi/nifi_shared_data/customer_{time_stamp}.csv", "w", newline=""
    ) as csvfile:
        fieldnames = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "street",
            "city",
            "state",
            "country",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(RECORD_COUNT):
            writer.writerow(
                {
                    "customer_id": i,
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "email": fake.email(),
                    "street": fake.street_address(),
                    "city": fake.city(),
                    "state": fake.state(),
                    "country": fake.country(),
                }
            )


if __name__ == "__main__":
    file_counter = 0
    try:
        while True:
            current_time = update_time_stamp()
            file_counter += 1

            print(f"Generating fake data, Batch #: {file_counter} at {current_time}.")

            create_csv_file(current_time)

            time.sleep(150)

    except KeyboardInterrupt:
        print("Generation stopped manually.")

    finally:
        print(f"{file_counter} batches generated, final file time: {current_time}.")
