import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)


def run():

    options = PipelineOptions(runner='DirectRunner')

    with beam.Pipeline(options=options) as p:
        pass

    logging.info("Running the application...")
    pass


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}", exc_info=True)
