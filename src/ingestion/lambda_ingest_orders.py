import json
import boto3
from datetime import datetime
import random
from io import BytesIO

s3 = boto3.client("s3")

BUCKET = "your-demo-bucket-name"          # TODO: change this
BASE_PREFIX = "aws-lakehouse/"
BRONZE_PREFIX = "bronze/orders/"


def generate_csv():
    """Generate a small CSV string with sample orders."""
    headers = "order_id,order_date,customer_id,country,product,quantity,unit_price\n"
    rows = []
    countries = ["US", "CA", "MX"]
    products = ["Widget A", "Widget B", "Widget C"]

    for order_id in range(1, 11):
        order_date = datetime.utcnow().date().isoformat()
        customer_id = random.randint(100, 200)
        country = random.choice(countries)
        product = random.choice(products)
        quantity = random.randint(1, 10)
        unit_price = random.choice([9.99, 19.99, 29.99])
        rows.append(f"{order_id},{order_date},{customer_id},{country},{product},{quantity},{unit_price}")

    csv_content = headers + "\n".join(rows)
    return csv_content


def lambda_handler(event, context):
    csv_content = generate_csv()
    key = (
        BASE_PREFIX
        + BRONZE_PREFIX
        + f"orders_lambda_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=csv_content.encode("utf-8")
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "CSV written to S3",
            "bucket": BUCKET,
            "key": key
        })
    }
