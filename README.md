# Fluentbit WriteAPI Output Plugin

This README includes all the necessary information to use the WriteAPI output plugin, which allows you to stream records into Google Cloud BigQuery. This implementation only supports the following formats:
- JSON files

Currently, the plugin is able to stream any JSON formatted data. This data must be present in a given input path and formatted to match the corresponding BigQuery table schema. Refer to the Accepted Data Type for each BigQuery Field section for more information.

## Creating a BigQuery Dataset and Table
Fluentbit does not create the dataset and table for your data, so you must create these ahead of time. 
- [Creating and using datasets](https://cloud.google.com/bigquery/docs/datasets)
- [Creating and using tables](https://cloud.google.com/bigquery/docs/tables)
For this output plugin, the fields in the table and JSON file must match the target table exactly.

## Installing Fluentbit
First, install the necessary build tools:
```
sudo apt-get update
sudo apt-get install git
sudo apt-get install make
sudo apt-get install gcc
sudo apt-get install cmake
sudo apt-get install build-essential
sudo apt-get install flex
sudo apt-get install bison
sudo apt install libyaml-dev
sudo apt install libssl-dev
sudo apt install pkg-config
```

Clone the git repository with `git clone  https://github.com/fluent/fluent-bit.git` and then build the repo:
```
cd build
cmake ../
make
sudo make install
```

## Setting Up the Plugin
Clone this repository into its own directory. The binary file should be up to date, but you can make it again by running `go build -buildmode=c-shared -o out_writeapi.so`. This command generates a header file (`out_writeapi.h`) and the binary file (`out_writeapi.so`). If this command gives you an `undefined` error, you may also be able to get both the header and binary file by running `go build -buildmode=c-shared -o out_writeapi.so out_writeapi.go retry.go`.

## Configuration File and Parameters
The WriteAPI Output Plugin enables a customer to send data to Google BigQuery without writing any code. Most of the work is done through the `config` file (named something like `examplefile.config`). The FluentBit `config` file should contain the following sections at the very least: `SERVICE`, `INPUT`, `OUTPUT`. The following is an example of a `SERVICE` section:
```
[SERVICE]
    Flush           1
    Daemon          off
    Log_Level       error
    Parsers_File    path/to/jsonparser.conf
    plugins_file    path/to/plugins.conf
```
The `Parsers_File` field points to the parsing of your input. An example parsers file is included as jsonparser.conf. The `plugins_file` field is the path to the binary plugin file you wish to use. An example plugins file is included as plugins.conf. The paths here are the absolute or relative paths to the files.

Here is an example of a `INPUT` section:
```
[INPUT]
    Name    tail
    Path    path/to/logfile.log
    Parser  json
    Tag     logfile1
```
This establishes an input (with a specified path) which uses the `tail` input plugin and `json` parser specified in the `SERVICES` section. The tag is an important field, as it is used to route data and match with relevant outputs. The path parameter can be an absolute or relative path to the file. 

Here is an example of an `OUTPUT` section:
```
[OUTPUT]
    Name                               writeapi
    Match                              logfile*
    ProjectId                          sample_projectID
    DatasetId                          sample_datasetID
    TableId                            sample_tableID
    Format                             json_lines
    Max_Chunk_Size                     1048576
    Max_Queue_Requests                 100
    Max_Queue_Bytes                    52428800
    Exactly_Once                       False
    Num_Synchronous_Retries            4
    DateTime_String_Type               True
```
This establishes an output using the `writeapi` plugin which matches to any input with a tag of `logfile*`. The tag-match uses regex, so the input (with tag logfile1) from above would be routed to this output. The next three fields describe the destination table in BigQuery. The `format` parameter is optional and relates to how the file should be parsed. The next six fields are also optional and configure stream settings based on how you want to ingest data into BigQuery. The first five fields are mandatory for the plugin to run. The last seven are optional, and have default values corresponding to those shown above.

The `Max_Chunk_Size` field takes in the number of bytes that the plugin will attempt to chunk data into before appending into the BigQuery Table. Fluent-Bit supports around a maximum of 2 MB of data within a single flush and we exercise a hard maximum of 9 MB (as BigQuery cannot handle appending data larger than this size). The `Max_Queue_Requests` and `Max_Queue_Bytes` fields describe the maximum number of requests/bytes of outstanding asynchronous responses that can be queued. When the first limit is reached, data appending will be blocked until enough responses are ready and the number of outstanding requests/bytes decreases. 

The `Exactly_Once` field takes in a boolean that describes whether exactly-once semantics will be utilized. By default, this field has value false and exactly-once is not used. With exactly-once delivery, response checking will be synchronous (as opposed to asynchronous response checking with at-least once/default semantics).

The `Num_Synchronous_Retries` field takes in the maximum number of synchronous retries the plugin will attempt when streaming data with exactly once semantics. This field does not change the number of asynchronous retries attempted with at-least once/default semantics. The default number of synchronous retries with exactly-once delivery is 4.

The `DateTime_String_Type` field takes in a boolean that describes whether the plugin will support DateTime string input data. When set to true, string literals will be accepted and when set to false, civil-time encoded int64 data will be accepted. Note that when this field is set, the input type applies for nested DataTime, as well (like a Record type with schema including DataTime). However, the plugin only supports int64 data for the Range<DataTime> BigQuery field regardless of the value set in the config file. More details about supported input data types for each BigQuery data type is below.

Once the configuration file is set and the source is properly configured, the command `fluent-bit -c nameOfFile.conf` will start sending data to BigQuery.

## Accepted Data Type for each BigQuery Field
|BigQuery Data Type|Supported Input Data Type|
|:----------------:|:-----------------------:|
|BOOL              |bool                     |
|BYTES             |bytes                    |
|DATE              |int32                    |
|DATETIME          |string or int64          |
|FLOAT             |float64                  |
|GEOGRAPHY         |string                   |
|INTEGER           |int64                    |
|JSON              |string                   |
|NUMERIC           |string                   |
|BIGNUMERIC        |string                   |
|STRING            |string                   |
|TIME              |string                   |
|TIMESTAMP         |int64                    |
|RANGE             |struct                   |
|RECORD            |struct                   |

Examples of sending each data type are included in integration_test.go \
Encoding details can be found here: [BigQuery Write API Protobuf Data Type Conversions](https://cloud.google.com/bigquery/docs/write-api#data_type_conversions)

## Plugin Error Handling and Resilience
The plugin is designed to log and handle both client-side and server-side errors, ensuring continuous and resilient data processing.

 - Client-Side Errors: Errors that occur during data preparation, such as transforming data formats, are logged. The affected row is skipped, but processing continues for subsequent data.
 - Server-Side Errors: These errors, such as those occurring during communication with BigQuery, are also logged. If any row in a request fails, the entire request fails (adhering to the default behavior of BigQuery), but the plugin continues to process future requests.

Regardless of the type of error, the plugin is built to maintain the flow of incoming data, ensuring that operations continue smoothly and that any issues are documented for troubleshooting.

## Backpressure and Buffering
This plugin utilizes dynamic stream scaling up when the rate of data being sent from Fluent Bit is too great for a single managed stream. Currently, dynamic scaling is only supported for the default stream type. However, to manage backpressure from the input/source to Fluent Bit itself, Fluent Bit implements its own buffering system where processed data is temporarily stored before being sent out. Fluent Bit primarily uses memory for buffering but can also utilize filesystem-based buffering for enhanced data safety.

- Memory Buffering: Fluent Bit stores data chunks in memory by default. This method is fast but can lead to high memory usage under heavy load or network delays. To manage this, you can set a `Mem_Buf_Limit` field in the input section of the configuration field, which will restrict the memory used by an input plugin, pausing data ingestion when the limit is reached. For example, when using the tail input plugin (utilized to read log/text files), memory buffering is often sufficient due to its ability to track log offsets which minimizes data loss during pauses.
- Filesystem Buffering: For greater data safety, filesystem buffering can be used. This requires finding a relevant input plugin that configures the `storage.type` to filesystem in its settings. This method stores data chunks both in memory and on disk, which gives control over memory usage.

More information on Fluent Bit buffering can be found here: [Fluent Bit: Official Manual - Buffering & Storage](https://docs.fluentbit.io/manual/administration/buffering-and-storage)

## Additional Information
For more information about the config file, look to [Fluentbit Official Guide to a Config File](https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file)
For more information about WriteAPI, look to [Google Cloud WriteAPI Documentation](https://cloud.google.com/bigquery/docs/write-api)
For more information about how to use Fluent Bit, look to [Fluent Bit Official Manual](https://docs.fluentbit.io/manual)