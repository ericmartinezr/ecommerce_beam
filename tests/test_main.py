from tests.mock_data import DATA
from main import Normalization, Masking
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import sys
import os

# Add parent directory to sys.path to import main
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MainTest(unittest.TestCase):

    def test_pipeline_workflow(self):
        # We expect the fraudulent record to be filtered out,
        # and the valid record to be normalized and masked.
        expected_data = [
            {
                "name": "Alice Smith",
                "email": "***@***.***",
                "phone": "**********",
                "address": "**********",
                "country": "CL",
                "signup_date": "2024-01-01",
                "event_timestamp": "2025-01-01T12:00:00.000000",
                "product": "Laptop",
                "category": "Electronics",
                "price": 599.99,
                "quantity": 1,
                "payment_method": "credit_card",
                "is_fraud": False,
                "device": "desktop",
                "ip_address": "***.***.***.***"
            }
        ]

        with TestPipeline() as p:
            data = p | "Read mock data" >> beam.Create(DATA)

            # Replicate the pipeline workflow
            filter_fraud = data | "Filter fraudulent transactions" >> beam.Filter(
                lambda x: not x['is_fraud'])

            filter_negative_price = filter_fraud | "Filter negative prices" >> beam.Filter(
                lambda x: x['price'] > 0)

            filter_negative_quantity = filter_negative_price | "Filter negative quantities" >> beam.Filter(
                lambda x: x['quantity'] > 0)

            filter_invalid_emails = filter_negative_quantity | "Filter invalid emails" >> beam.Filter(
                lambda x: x['email'] != 'invalid_email')

            normalization = filter_invalid_emails | "Normalization" >> beam.ParDo(
                Normalization()).with_outputs('errors', main='normalized')

            masking = normalization.normalized | "Masking" >> beam.ParDo(
                Masking())

            assert_that(masking, equal_to(expected_data))


if __name__ == '__main__':
    unittest.main()
