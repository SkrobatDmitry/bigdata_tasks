# Converter
---
Console utility for converting an input csv file to parquet or back, as well as for obtaining parquet file schema.

## Installation
---
Your system must have:

- [Python](https://www.python.org/downloads/)
- [Required frameworks](https://bitbucket.org/coherentprojects/coherent-training-dmitry-skrobat/src/master/converter/requirements.txt)

To install the required frameworks use:
```bash
pip install -r requirements.txt
```

## Parameters
---
- `-h, --help` - show help message and exit.
- `--csv2parquet src-filename dst-filename` - convert csv file to parquet.
- `--parquet2csv src-filename dst-filename` - convert parquet file to csv.
- `--schema-parquet filename` - get schema of parquet file.
- `d, --debug` - turn on a debug mode.

## Usage
---
```text
converter.py [-h] [--csv2parquet src-filename dst-filename | --parquet2csv src-filename dst-filename] [--get-schema filename] [-d]
```

### Show help message
---
Use `-h` or `--help` to get the help message. For example:
```bash
python3 converter.py --help
```

The utility produces the following output:
```text
usage: converter.py converter.py [-h] [--csv2parquet src-filename dst-filename | --parquet2csv src-filename dst-filename] [--get-schema filename] [-d]

Console utility for converting an input csv-format file to parquet or back, as well as for obtaining parquet file schema

optional arguments:
  -h, --help                                show this help message and exit
  --csv2parquet src-filename dst-filename   convert csv file to parquet
  --parquet2csv src-filename dst-filename   convert parquet file to csv
  --get-schema filename                     get schema of parquet file
  -d, --debug                               turn on a debug mode
```

### Convert CSV to Parquet
---
Specify `--csv2parquet` argument to convert a csv-format file src-filename to parquet and save it in a dst-filename. For example:
```bash
python3 converter.py --csv2parquet dataset/pokemon.csv dataset/pokemon.parquet
```

### Convert Parquet to CSV
---
Use `--parquet2csv` to convert a parquet-format file src-filename to csv and save it in a dst-filename. For example:
```bash
python3 converter.py --parquet2csv dataset/sales.parquet dataset/sales.csv
```

### Get Parquet schema
---
Specify `--schema-parquet` argument to get the filename parquet file schema. For example:
```bash
python3 converter.py --schema-parquet dataset/movies.parquet
```

The utility produces the following output:
```text
Film: string
Genre: string
Lead Studio: string
Audience score %: int64
Profitability: double
Rotten Tomatoes %: int64
Worldwide Gross: string
Year: int64
```

### Debug mode
---
Use `-d` or `--debug` argument to turn on a debug mode. For example:
```bash
python3 converter.py --schema-parquet dataset/sales.parqt -d
```

The utility produces the following output:
```text
ERROR - 2022-02-10 02:24:34:
[Errno 2] Failed to open local file 'dataset/movies.parqt'. Detail: [errno 2] No such file or directory
```