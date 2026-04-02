import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)

# TODO: Normalizacion
# TODO: Eliminar emails erroneos
# TODO: Enmascaramiento
# TODO: Analisis (averiguar libreria para hacerlo local)


def run():

    options = PipelineOptions(runner='DirectRunner')

    columns = ["name", "email", "phone", "address",
               "country", "signup_date", "event_timestamp", "product",
               "category", "price", "quantity", "payment_method",
               "is_fraud", "device", "ip_address"]

    with beam.Pipeline(options=options) as p:

        # Lee los datos desde el archivo de entrada
        # Usa solo los campos necesarios para el proceso
        data = p | "Read data" >> beam.io.ReadFromParquet(
            'dataset.parquet', columns=columns, as_rows=True)

        # Elimina las transacciones fraudulentas
        filter_fraud = data | "Filter fraudulent transactions" >> beam.Filter(
            lambda x: x['is_fraud'])

        # Elimina las transacciones con precios negativos
        filter_negative_price = filter_fraud | "Filter negative prices" >> beam.Filter(
            lambda x: x['price'] < 0)

        # Elimina las transacciones con cantidades negativas
        filter_negative_quantity = filter_negative_price | "Filter negative quantities" >> beam.Filter(
            lambda x: x['quantity'] < 0)

    logging.info("Running the application...")
    pass


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}", exc_info=True)
