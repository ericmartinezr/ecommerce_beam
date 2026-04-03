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


class Normalization(beam.DoFn):
    def process(self, element):
        # Chile, chile, CL > CL
        countries = {
            'chile': 'CL',
            'Chile': 'CL',
            'Argentina': 'AR',
            'argentina': 'AR',
            'Peru': 'PE',
            'peru': 'PE',
        }

        # Normaliza los paises
        if element['country'] in countries:
            element['country'] = countries[element['country']]

        # Normaliza la fecha de registro
        if element['signup_date'] and 'T' in element['signup_date']:
            element['signup_date'] = element['signup_date'].split('T')[0]

        yield element


class Masking(beam.DoFn):
    def process(self, element):
        # Enmascara el email
        if element['email']:
            element['email'] = f'***@***.***'

        # Enmascara el telefono
        if element['phone']:
            element['phone'] = '**********'

        # Enmascara la direccion
        if element['address']:
            element['address'] = '**********'

        # Enmascara el ip_address
        if element['ip_address']:
            element['ip_address'] = '***.***.***.***'

        yield element


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
            'dataset.parquet', columns=columns)

        # Elimina las transacciones fraudulentas
        filter_fraud = data | "Filter fraudulent transactions" >> beam.Filter(
            lambda x: x['is_fraud'])

        # Elimina las transacciones con precios negativos y cantidades negativas
        filter_negative_price = filter_fraud | "Filter negative prices" >> beam.Filter(
            lambda x: x['price'] > 0)

        # Elimina las transacciones con cantidades negativas
        filter_negative_quantity = filter_negative_price | "Filter negative quantities" >> beam.Filter(
            lambda x: x['quantity'] > 0)

        # Elimina correos invalidos
        filter_invalid_emails = filter_negative_quantity | "Filter invalid emails" >> beam.Filter(
            lambda x: x['email'] != 'invalid_email')

        # Normalizacion
        normalization = filter_invalid_emails | "Normalization" >> beam.ParDo(
            Normalization())

        # Ensmascaramiento
        masking = normalization | "Masking" >> beam.ParDo(Masking())

        masking | beam.Map(print)

    logging.info("Running the application...")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}", exc_info=True)
