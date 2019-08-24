# ASN.1 Data Source for Apache Spark 2.x
A library for parsing and querying ASN.1 encoded data (Ber/Der) with Apache Spark, for Spark SQL and DataFrames.
## Requirements

This library requires Spark 2.0+

## Features
This package allows reading ASN.1 encoded files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `schemaFileType`: the type of the file that contain the schema (currently supports asn and json files).
* `schemaFilePath`: the path of the file that contain the schema definition (currently supports scala and java).
* `customDecoderLanguage`: the language in which the custom decoder is written.
* `customDecoder`: the fully qualified name of the user custom decoder.