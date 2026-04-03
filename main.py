import apache_beam as beam
import pyarrow as pa
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue
from apache_beam.runners.render import RenderRunner

# TODO: Analisis (averiguar libreria para hacerlo local)


class Normalization(beam.DoFn):
    def process(self, element):

        try:
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
        except Exception as e:
            print(f"Error processing element: {e}")
            yield pvalue.TaggedOutput('errors', element)


class Masking(beam.DoFn):
    def process(self, element):

        # Enmascara el email
        if element['email']:
            element['email'] = '***@***.***'

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

    options = PipelineOptions(runner=RenderRunner())

    columns = ["name", "email", "phone", "address",
               "country", "signup_date", "event_timestamp", "product",
               "category", "price", "quantity", "payment_method",
               "is_fraud", "device", "ip_address"]

    schema = pa.schema([
        ("name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("address", pa.string()),
        ("country", pa.string()),
        ("signup_date", pa.string()),
        ("event_timestamp", pa.timestamp("us")),
        ("product", pa.string()),
        ("category", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.int64()),
        ("payment_method", pa.string()),
        ("is_fraud", pa.bool_()),
        ("device", pa.string()),
        ("ip_address", pa.string()),
    ])

    with beam.Pipeline(options=options) as p:

        # Lee los datos desde el archivo de entrada
        # Usa solo los campos necesarios para el proceso
        data = p | "Read data" >> beam.io.ReadFromParquet(
            'data/**/*.parquet', columns=columns)

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
            Normalization()).with_outputs('errors', main='normalized')

        # Ensmascaramiento
        masking = normalization.normalized | "Masking" >> beam.ParDo(Masking())

        #
        masking | "Write normalized data" >> beam.io.WriteToParquet(
            'output/normalized_data.parquet', schema=schema)

        normalization.errors | "Write errors" >> beam.io.WriteToText(
            'output/errors.txt')


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"An error occurred: {e}")
