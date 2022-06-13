import argparse
import logging

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


def csv_to_parquet(src: str, dst: str, logger=None, batch_size=1e6) -> None:
    """
    Reads a CSV file and writes it to a Parquet file

    :param src: The path to the CSV file to convert
    :param dst: The destination parquet file
    :param logger: The logger object to use
    :param batch_size: The number of rows to read from the CSV file before converting to a parquet file
    """
    try:
        writer = None
        with csv.open_csv(src, read_options=csv.ReadOptions(block_size=batch_size)) as reader:
            for batch in reader:
                if writer is None:
                    writer = pq.ParquetWriter(dst, batch.schema)
                table = pa.Table.from_batches(batches=[batch])
                writer.write_table(table)
    except Exception as e:
        if logger:
            logger.exception(e, exc_info=False)
    finally:
        if writer:
            writer.close()


def parquet_to_csv(src: str, dst: str, logger=None) -> None:
    """
    Reads a Parquet file and writes a CSV file
    
    :param src: The path to the Parquet file
    :param dst: The path to the destination CSV file
    :param logger: The logger object to use
    """
    try:
        table = pq.read_table(src)
        csv.write_csv(table, dst)
    except Exception as e:
        if logger:
            logger.exception(e, exc_info=False)


def get_schema(src: str, logger=None) -> pa.lib.Schema:
    """
    Reads the schema from a parquet file

    :param src: The source file path
    :param logger: The logger object to use
    :return: A schema object
    """
    schema = pa.lib.Schema
    try:
        schema = pq.read_schema(src)
    except Exception as e:
        if logger:
            logger.exception(e, exc_info=False)
    finally:
        return schema


def get_args() -> dict:
    """
    Get the command line arguments and return them as a dictionary
    :return: A dictionary of the arguments
    """
    parser = argparse.ArgumentParser(description='Console utility for converting an input csv-format file to parquet '
                                                 'or back, as well as for obtaining parquet file schema')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--csv2parquet', action='store', nargs=2, metavar=('src-filename', 'dst-filename'),
                       help='convert csv file to parquet')
    group.add_argument('--parquet2csv', action='store', nargs=2, metavar=('src-filename', 'dst-filename'),
                       help='convert parquet file to csv')

    parser.add_argument('--get-schema', action='store', metavar='filename', help='get schema of parquet file')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='turn on a debug mode')

    return vars(parser.parse_args())


def get_logger() -> logging.Logger:
    """
    Get a logger object and set a basic config for him
    :return: A logger object
    """
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(levelname)s - %(asctime)s:\n%(message)s', level=logging.ERROR)
    return logger


def main() -> None:
    args = get_args()
    logger = get_logger() if args['debug'] else None

    if args['csv2parquet']:
        src, dst = args['csv2parquet']
        csv_to_parquet(src, dst, logger)
    elif args['parquet2csv']:
        src, dst = args['parquet2csv']
        parquet_to_csv(src, dst, logger)

    if args['get_schema']:
        src = args['get_schema']
        schema = get_schema(src, logger)
        if schema != pa.lib.Schema:
            print(schema)


if __name__ == '__main__':
    main()
