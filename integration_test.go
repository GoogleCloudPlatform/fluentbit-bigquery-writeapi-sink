// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

const (
	logFileName    = "logfile.log"
	logFilePath    = "./" + logFileName
	configFileName = "fluent-bit.conf"
	configFilePath = "./" + configFileName
	numRows        = 10
)

// Integration test validates the end-to-end fluentbit and bigquery pipeline with all bigquery fields
// Data is inputted based on documentation (constraints for each field included there)
func TestPipeline(t *testing.T) {
	ctx := context.Background()

	// Define schema of relevant BigQuery Table
	tableSchema := bigquery.Schema{
		{Name: "StringField", Type: bigquery.StringFieldType},
		{Name: "BytesField", Type: bigquery.BytesFieldType},
		{Name: "IntegerField", Type: bigquery.IntegerFieldType},
		{Name: "FloatField", Type: bigquery.FloatFieldType},
		{Name: "NumericField", Type: bigquery.NumericFieldType},
		{Name: "BigNumericField", Type: bigquery.BigNumericFieldType},
		{Name: "BooleanField", Type: bigquery.BooleanFieldType},
		{Name: "TimestampField", Type: bigquery.TimestampFieldType},
		{Name: "DateField", Type: bigquery.DateFieldType},
		{Name: "TimeField", Type: bigquery.TimeFieldType},
		{Name: "DateTimeField", Type: bigquery.DateTimeFieldType},
		{Name: "GeographyField", Type: bigquery.GeographyFieldType},
		{Name: "RecordField", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "SubField1", Type: bigquery.StringFieldType},
			{Name: "SubField2", Type: bigquery.NumericFieldType},
		}},
		// When DateTime is the range element type, must send civil-time encoded int64 datetime data (as documented)
		{Name: "RangeField", Type: bigquery.RangeFieldType, RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateTimeFieldType}},
		{Name: "JSONField", Type: bigquery.JSONFieldType},
	}

	// Set up bigquery client and create dataset & table within existing project
	// Create config file with relevant parameters & create log file
	client, dataset, fullTableID, file := setupBQTableAndFiles(ctx, t, tableSchema, "false")
	defer client.Close()
	defer file.Close()

	// Start Fluent Bit with the config file
	FBcmd := exec.Command("fluent-bit", "-c", configFileName)
	if err := FBcmd.Start(); err != nil {
		t.Fatalf("Failed to start Fluent Bit: %v", err)
	}

	// Wait for fluent-bit connection to generate data; add delays before ending fluent-bit process
	time.Sleep(2 * time.Second)
	if err := generateAllDataTypes(numRows); err != nil {
		t.Fatalf("Failed to generate data: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Stop Fluent Bit
	if err := FBcmd.Process.Kill(); err != nil {
		t.Fatalf("Failed to stop Fluent Bit: %v", err)
	}

	// Verify data in BigQuery by querying
	queryMsg := "SELECT * FROM `" + fullTableID + "`"
	BQquery := client.Query(queryMsg)
	BQdata, err := BQquery.Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query data information BigQuery: %v", err)
	}

	rowCount := 0
	for {
		// Check that the data sent is correct
		var BQvalues []bigquery.Value
		err := BQdata.Next(&BQvalues)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read query results: %v", err)
		}
		// Verify size of the data
		assert.Equal(t, len(BQvalues), reflect.TypeOf(log_entry_alltypes{}).NumField())

		// Verify the value of the data
		assert.Equal(t, "hello world", BQvalues[0])
		assert.Equal(t, []byte("hello bytes"), BQvalues[1])
		assert.Equal(t, int64(123), BQvalues[2])
		assert.Equal(t, float64(123.45), BQvalues[3])
		assert.Equal(t, big.NewRat(12345, 100), (BQvalues[4].(*big.Rat)))
		assert.Equal(t, big.NewRat(123456789123456789, 1000000000), BQvalues[5].(*big.Rat))
		assert.Equal(t, true, BQvalues[6])
		assert.Equal(t, time.Date(2024, time.July, 26, 6, 8, 46, 0, time.UTC), BQvalues[7])
		assert.Equal(t, civil.Date{Year: 2024, Month: 7, Day: 25}, BQvalues[8].(civil.Date))
		assert.Equal(t, civil.Time{Hour: 12, Minute: 34, Second: 56}, BQvalues[9].(civil.Time))
		assert.Equal(t, civil.DateTime{Date: civil.Date{Year: 2024, Month: 7, Day: 26}, Time: civil.Time{Hour: 12, Minute: 30, Second: 0, Nanosecond: 450000000}}, BQvalues[10].(civil.DateTime))
		assert.Equal(t, "POINT(1 2)", BQvalues[11].(string))
		assert.Equal(t, "sub field value", BQvalues[12].([]bigquery.Value)[0])
		assert.Equal(t, big.NewRat(456, 10), (BQvalues[12].([]bigquery.Value)[1]).(*big.Rat))
		assert.Equal(t, &bigquery.RangeValue{Start: civil.DateTime{Date: civil.Date{Year: 1987, Month: 1, Day: 23}, Time: civil.Time{Hour: 12, Minute: 34, Second: 56, Nanosecond: 789012000}}, End: civil.DateTime{Date: civil.Date{Year: 1987, Month: 1, Day: 23}, Time: civil.Time{Hour: 12, Minute: 34, Second: 56, Nanosecond: 789013000}}}, BQvalues[13])
		assert.Equal(t, "{\"age\":28,\"name\":\"Jane Doe\"}", BQvalues[14].(string))
		rowCount++
	}

	// Verify the number of rows
	assert.Equal(t, numRows, rowCount)

	// Clean up - delete the BigQuery dataset and its contents(includes generated table) as well as config & log files
	cleanUpBQDatasetAndFiles(ctx, t, dataset)
}

// Integration test validates the exactly-once functionality
func TestExactlyOnce(t *testing.T) {
	ctx := context.Background()

	// Define schema of relevant BigQuery table
	tableSchema := bigquery.Schema{
		{Name: "Message", Type: bigquery.StringFieldType},
	}

	// Set up bigquery client and create dataset & table within existing project
	// Create config file with relevant parameters & create log file
	client, dataset, fullTableID, file := setupBQTableAndFiles(ctx, t, tableSchema, "true")
	defer client.Close()
	defer file.Close()

	// Start Fluent Bit with the config file
	FBcmd := exec.Command("fluent-bit", "-c", configFileName)
	if err := FBcmd.Start(); err != nil {
		t.Fatalf("Failed to start Fluent Bit: %v", err)
	}

	// Wait for fluent-bit connection to generate data; add delays before ending fluent-bit process
	time.Sleep(2 * time.Second)
	if err := generateData(numRows, (2 * time.Second), false); err != nil {
		t.Fatalf("Failed to generate data: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Stop Fluent Bit
	if err := FBcmd.Process.Kill(); err != nil {
		t.Fatalf("Failed to stop Fluent Bit: %v", err)
	}

	// Verify data in BigQuery by querying
	queryMsg := "SELECT Message FROM `" + fullTableID + "`"
	BQquery := client.Query(queryMsg)
	BQdata, err := BQquery.Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query data information BigQuery: %v", err)
	}

	rowCount := 0
	for {
		// Check that the data sent is correct
		var BQvalues []bigquery.Value
		err := BQdata.Next(&BQvalues)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read query results: %v", err)
		}

		assert.Equal(t, "hello world", BQvalues[0])
		rowCount++
	}

	// Verify the number of rows
	assert.Equal(t, numRows, rowCount)

	// Clean up - delete the BigQuery dataset and its contents(includes generated table) as well as config & log files
	cleanUpBQDatasetAndFiles(ctx, t, dataset)
}

// This test validates that if a single row that cannot be transformed to binary is sent (with default semantics), the rest of the batch will not be dropped
func TestErrorHandlingDefault(t *testing.T) {
	const numGoodRows = 8

	ctx := context.Background()

	// Define schema of relevant BigQuery table
	tableSchema := bigquery.Schema{
		{Name: "Message", Type: bigquery.StringFieldType},
	}

	// Set up bigquery client and create dataset & table within existing project
	// Create config file with relevant parameters & create log file
	client, dataset, fullTableID, file := setupBQTableAndFiles(ctx, t, tableSchema, "false")
	defer client.Close()
	defer file.Close()

	// Start Fluent Bit with the config file
	FBcmd := exec.Command("fluent-bit", "-c", configFileName)
	if err := FBcmd.Start(); err != nil {
		t.Fatalf("Failed to start Fluent Bit: %v", err)
	}

	// Wait for fluent-bit connection to generate data; add delays before ending fluent-bit process
	time.Sleep(2 * time.Second)
	if err := generateData(numRows, 500*time.Millisecond, true); err != nil {
		t.Fatalf("Failed to generate data: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Stop Fluent Bit
	if err := FBcmd.Process.Kill(); err != nil {
		t.Fatalf("Failed to stop Fluent Bit: %v", err)
	}

	// Verify data in BigQuery by querying
	queryMsg := "SELECT Message FROM `" + fullTableID + "`"
	BQquery := client.Query(queryMsg)
	BQdata, err := BQquery.Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query data information BigQuery: %v", err)
	}

	rowCount := 0
	for {
		// Check that the data sent is correct
		var BQvalues []bigquery.Value
		err := BQdata.Next(&BQvalues)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read query results: %v", err)
		}

		assert.Equal(t, "hello world", BQvalues[0])
		rowCount++
	}

	// Verify the number of rows
	assert.Equal(t, numGoodRows, rowCount)

	// Clean up - delete the BigQuery dataset and its contents(includes generated table) as well as config & log files
	cleanUpBQDatasetAndFiles(ctx, t, dataset)
}

// This test validates that if a single row that cannot be transformed to binary is sent (with exactly-once semantics), the rest of the batch will not be dropped
func TestErrorHandlingExactlyOnce(t *testing.T) {
	const numGoodRows = 8

	ctx := context.Background()

	// Define schema of relevant BigQuery table
	tableSchema := bigquery.Schema{
		{Name: "Message", Type: bigquery.StringFieldType},
	}

	// Set up bigquery client and create dataset & table within existing project
	// Create config file with relevant parameters & create log file
	client, dataset, fullTableID, file := setupBQTableAndFiles(ctx, t, tableSchema, "true")
	defer client.Close()
	defer file.Close()

	// Start Fluent Bit with the config file
	FBcmd := exec.Command("fluent-bit", "-c", configFileName)
	if err := FBcmd.Start(); err != nil {
		t.Fatalf("Failed to start Fluent Bit: %v", err)
	}

	// Wait for fluent-bit connection to generate data; add delays before ending fluent-bit process
	time.Sleep(2 * time.Second)
	if err := generateData(numRows, (500 * time.Millisecond), true); err != nil {
		t.Fatalf("Failed to generate data: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Stop Fluent Bit
	if err := FBcmd.Process.Kill(); err != nil {
		t.Fatalf("Failed to stop Fluent Bit: %v", err)
	}

	// Verify data in BigQuery by querying
	queryMsg := "SELECT Message FROM `" + fullTableID + "`"
	BQquery := client.Query(queryMsg)
	BQdata, err := BQquery.Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query data information BigQuery: %v", err)
	}

	rowCount := 0
	for {
		// Check that the data sent is correct
		var BQvalues []bigquery.Value
		err := BQdata.Next(&BQvalues)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read query results: %v", err)
		}

		assert.Equal(t, "hello world", BQvalues[0])
		rowCount++
	}

	// Verify the number of rows
	assert.Equal(t, numGoodRows, rowCount)

	// Clean up - delete the BigQuery dataset and its contents(includes generated table) as well as config & log files
	cleanUpBQDatasetAndFiles(ctx, t, dataset)
}

// This function closes to the BigQuery client and sends a fatal error
func closeClientFatal(client *bigquery.Client, t *testing.T, message string, args ...any) {
	client.Close()
	t.Fatalf(message, args...)
}

// This function sets up the BigQuery client, and creates a dataset and table in an existing project
// The configuration file and log file are also created
func setupBQTableAndFiles(ctx context.Context, t *testing.T, tableSchema bigquery.Schema, exactlyOnceVal string) (*bigquery.Client, *bigquery.Dataset, string, *os.File) {
	// Get projectID from environment
	projectID := os.Getenv("ProjectID")
	if projectID == "" {
		t.Fatal("Environment variable 'ProjectID' is required to run this test, but not set currently")
	}

	// Set up BigQuery client
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Create a random dataset and table name
	datasetHash := randString()
	datasetID := "testdataset_" + datasetHash

	tableHash := randString()
	tableID := "testtable_" + tableHash

	// Create BigQuery dataset and table in an existing project
	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		closeClientFatal(client, t, "Failed to create BigQuery dataset %v", err)
	}
	table := dataset.Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: tableSchema}); err != nil {
		closeClientFatal(client, t, "Failed to create BigQuery table %v", err)
	}

	// Create config file with random table name and relevant exactlyOnce parameter
	if err := createConfigFile(projectID, datasetID, tableID, exactlyOnceVal); err != nil {
		closeClientFatal(client, t, "Failed to create config file %v", err)
	}

	// Create log file
	file, err := os.Create(logFilePath)
	if err != nil {
		closeClientFatal(client, t, "Failed to create log file %v", err)
	}

	// Return client, dataset, and full tableID
	return client, dataset, (projectID + "." + datasetID + "." + tableID), file

}

// This function deletes the BigQuery dataset, config file, and log file
func cleanUpBQDatasetAndFiles(ctx context.Context, t *testing.T, dataset *bigquery.Dataset) {
	// Clean up - delete the BigQuery dataset and its contents(includes generated table)
	if err := dataset.DeleteWithContents(ctx); err != nil {
		t.Fatalf("Failed to delete BigQuery dataset and table: %v", err)
	}

	// Clean up - delete the log file
	if err := os.Remove(logFilePath); err != nil {
		t.Fatalf("Failed to delete log file: %v", err)
	}

	// Clean up - delete the config file
	if err := os.Remove(configFilePath); err != nil {
		t.Fatalf("Failed to delete config log file: %v", err)
	}
}

// Generate a random string with length 10 (to act as a hash for the table name)
func randString() string {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	charset := "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM0123456789"
	str := make([]byte, 10)
	for i := range str {
		str[i] = charset[rng.Intn(len(charset))]
	}
	return string(str)
}

// Template for the config file - dynamically update TableId field
const configTemplate = `
[SERVICE]
    Daemon          off
    Log_Level       error
    Parsers_File    ./jsonparser.conf
    plugins_file    ./plugins.conf

[INPUT]
    Name                               tail
    Path                               {{.CurrLogfilePath}}
    Parser                             json
    Tag                                hello_world

[OUTPUT]
    Name                               writeapi
    Match                              hello_world
    ProjectId                          {{.CurrProjectName}}
    DatasetId                          {{.CurrDatasetName}}
    TableId                            {{.CurrTableName}}
    Exactly_Once                       {{.CurrExactlyOnce}}
`

// Struct for dynamically updating config file
type Config struct {
	CurrLogfilePath string
	CurrProjectName string
	CurrDatasetName string
	CurrTableName   string
	CurrExactlyOnce string
}

// Function creates configuration file with the input as the TableId field
func createConfigFile(currProjectID string, currDatasetID string, currTableID string, currExactlyOnceVal string) error {
	// Struct with the TableId
	config := Config{
		CurrLogfilePath: logFilePath,
		CurrProjectName: currProjectID,
		CurrDatasetName: currDatasetID,
		CurrTableName:   currTableID,
		CurrExactlyOnce: currExactlyOnceVal,
	}

	// Create a new template with the format of configTemplate
	tmpl, err := template.New("currConfig").Parse(configTemplate)
	if err != nil {
		return err
	}

	// Create the new file
	file, err := os.Create(configFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Return file with the given template and TableId
	return tmpl.Execute(file, config)
}

// Log entry template (corresponding to BQ table schema)

type log_entry struct {
	Message string `json:"Message"`
}

// Data generation function
func generateData(numRows int, sleepTime time.Duration, sendBadRow bool) error {
	// Open file
	file, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		return err
	}

	// Send json marshalled data
	for i := 1; i <= numRows; i++ {
		curr := log_entry{
			Message: "hello world",
		}
		entry, err := json.Marshal(curr)
		// Comparing counter to mod 5 in order to send 2 bad rows of data
		if sendBadRow && ((i % 5) == 0) {
			entry, err = json.Marshal("Bad data entry")
		}
		if err != nil {
			return err
		}
		// Write data to source logfile with logger.Println call
		logger.Println(string(entry))

		time.Sleep(sleepTime)
	}

	return nil
}

// Log entry template with all bigquery fields
type log_entry_alltypes struct {
	StringField     string  `json:"StringField"`
	BytesField      []byte  `json:"BytesField"`
	IntegerField    int64   `json:"IntegerField"`
	FloatField      float64 `json:"FloatField"`
	NumericField    string  `json:"NumericField"`
	BigNumericField string  `json:"BigNumericField"`
	BooleanField    bool    `json:"BooleanField"`
	TimestampField  int64   `json:"TimestampField"`
	DateField       int32   `json:"DateField"`
	TimeField       string  `json:"TimeField"`
	DateTimeField   string  `json:"DateTimeField"`
	GeographyField  string  `json:"GeographyField"`
	RecordField     struct {
		SubField1 string `json:"SubField1"`
		SubField2 string `json:"SubField2"`
	} `json:"RecordField"`
	RangeField struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"RangeField"`
	JSONField string `json:"JSONField"`
}

// Data generation function including all suported BQ fields
func generateAllDataTypes(numRows int) error {
	// Open file
	file, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	logger := log.New(file, "", 0)

	// Send json marshalled data
	for i := 0; i < numRows; i++ {
		jsonData := map[string]interface{}{
			"name": "Jane Doe",
			"age":  28,
		}
		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			return err
		}

		curr := log_entry_alltypes{
			StringField:     "hello world",
			BytesField:      []byte("hello bytes"),
			IntegerField:    123,
			FloatField:      123.45,
			NumericField:    "123.45",
			BigNumericField: "123456789.123456789",
			BooleanField:    true,
			// Milliseconds since unix epoch
			TimestampField: 1721974126000000,
			// Days since unix epoch
			DateField:      19929,
			TimeField:      "12:34:56",
			DateTimeField:  "2024-07-26 12:30:00.45",
			GeographyField: "POINT(1 2)",
			RecordField: struct {
				SubField1 string `json:"SubField1"`
				SubField2 string `json:"SubField2"`
			}{
				SubField1: "sub field value",
				SubField2: "45.6",
			},
			// Hardcoded civil-time encoded value
			RangeField: struct {
				Start int64 `json:"start"`
				End   int64 `json:"end"`
			}{
				Start: 139830307704277524,
				End:   139830307704277525,
			},
			JSONField: string(jsonBytes),
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			return err
		}
		// Write data to source logfile with logger.Println call
		logger.Println(string(entry))

		time.Sleep(time.Second)
	}

	return nil
}
