import csv
import random
import uuid
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

OUTPUT_FILE = "dataset_ecommerce.csv"
NUM_ROWS = 2_000_000
CHUNK_SIZE = 100_000   # escritura por bloques

countries = ["Chile", "CL", "chile", "Argentina", "Peru", None]
products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor", "Mouse"]
categories = ["Electronics", "Accessories", None, "Tech"]
payment_methods = ["credit_card", "debit_card", "paypal", "crypto", "invalid"]
devices = ["mobile", "desktop", "tablet", "unknown"]


def random_date():
    start = datetime.now() - timedelta(days=365 * 2)
    return start + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 365 * 2))


def maybe_null(value, prob=0.1):
    return value if random.random() > prob else None


def generate_row():
    # Introducir errores intencionales
    price = round(random.uniform(-50, 2000), 2)  # negativos
    quantity = random.randint(-2, 5)  # negativos

    email = fake.email()
    if random.random() < 0.05:
        email = "invalid_email"

    phone = fake.phone_number()
    if random.random() < 0.1:
        phone = str(random.randint(1000, 9999))  # mal formato

    return [
        str(uuid.uuid4()) if random.random(
        ) > 0.01 else None,  # duplicados/nulos
        maybe_null(str(uuid.uuid4()), 0.05),
        maybe_null(fake.name(), 0.05),
        email,
        phone,
        maybe_null(fake.address().replace("\n", " "), 0.1),
        random.choice(countries),
        maybe_null(random_date().isoformat(), 0.1),
        random_date().isoformat(),
        random.choice(products + ["", None]),
        random.choice(categories),
        price,
        quantity,
        random.choice(payment_methods),
        True if random.random() < 0.02 else False,  # fraude bajo
        random.choice(devices),
        fake.ipv4() if random.random() > 0.05 else "999.999.999.999",
        fake.user_agent(),
        maybe_null(fake.lexify(text="????-DISCOUNT"), 0.7)
    ]


def write_dataset():
    headers = [
        "event_id", "user_id", "name", "email", "phone", "address",
        "country", "signup_date", "event_timestamp", "product",
        "category", "price", "quantity", "payment_method",
        "is_fraud", "device", "ip_address", "user_agent", "discount_code"
    ]

    write_header = not os.path.exists(OUTPUT_FILE)

    with open(OUTPUT_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if write_header:
            writer.writerow(headers)

        for _ in range(NUM_ROWS // CHUNK_SIZE):
            rows = [generate_row() for _ in range(CHUNK_SIZE)]
            writer.writerows(rows)
            print(f"Chunk escrito: {CHUNK_SIZE} filas")


def write_parquet():
    all_rows = []

    for _ in range(100_000):
        all_rows.append(generate_row())

    df = pd.DataFrame(all_rows, columns=[
        "event_id", "user_id", "name", "email", "phone", "address",
        "country", "signup_date", "event_timestamp", "product",
        "category", "price", "quantity", "payment_method",
        "is_fraud", "device", "ip_address", "user_agent", "discount_code"
    ])

    table = pa.Table.from_pandas(df)
    pq.write_table(table, "dataset.parquet")


def write_partitioned_parquet(rows):
    df = pd.DataFrame(rows, columns=[
        "event_id", "user_id", "name", "email", "phone", "address",
        "country", "signup_date", "event_timestamp", "product",
        "category", "price", "quantity", "payment_method",
        "is_fraud", "device", "ip_address", "user_agent", "discount_code"
    ])

    # Convertir timestamp
    df["event_timestamp"] = pd.to_datetime(
        df["event_timestamp"], errors="coerce")

    # Crear columnas de partición
    df["year"] = df["event_timestamp"].dt.year
    df["month"] = df["event_timestamp"].dt.month
    df["day"] = df["event_timestamp"].dt.day

    # Escribir particionado
    # Registro malo: Escribiendo partición: year=2024.0, month=4.0, day=3.0 con 74 filas
    for (y, m, d), group in df.groupby(["year", "month", "day"]):
        print(
            f"Escribiendo partición: year={y}, month={m}, day={d} con {len(group)} filas")
        path = f"data/year={y}/month={m:02d}/day={d:02d}"
        # os.makedirs(path, exist_ok=True)
#
        # file_path = f"{path}/data.parquet"
        # group.to_parquet(file_path, index=False)


def write_partitioned():
    for _ in range(NUM_ROWS // CHUNK_SIZE):
        rows = [generate_row() for _ in range(CHUNK_SIZE)]
        write_partitioned_parquet(rows)


if __name__ == "__main__":
    write_partitioned()
