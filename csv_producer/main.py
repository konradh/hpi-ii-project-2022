import argparse
import csv
import dotenv
import importlib
import logging
import os
import sys

from producer import Producer

dotenv.load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

def produce_from_csv(filename, schema_cls, topic):
    producer = Producer(schema_cls, topic)

    with open(filename) as file:
        reader = csv.DictReader(file)
        key_field = reader.fieldnames[0]

        for row in reader:
            event = schema_cls()
            for key, value in row.items():
                setattr(event, key.replace('.', '_'), value)
            producer.produce(key=row[key_field], value=event)
        producer.poll()


def import_object(description):
    module, cls = description.rsplit(':', 1)
    path, module = module.rsplit('/', 1)

    print(path, module, cls)

    sys.path.append(path)
    module = importlib.import_module(module)
    sys.path.remove(path)

    return getattr(module, cls)


def parse_args():
    parser = argparse.ArgumentParser(description='Import csv file into kafka')
    parser.add_argument('filename', help='csv file to import')
    parser.add_argument('schema', help='protobuf schema to use (should correspond to csv header): path/to/schema_pb2:Schema')
    parser.add_argument('topic', help='kafka topic to produce to')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    produce_from_csv(args.filename, import_object(args.schema), args.topic)
