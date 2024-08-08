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
	"C"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Struct for each stream - can be multiple per output
type streamConfig struct {
	managedstream MWManagedStream
	appendResults *[]*managedwriter.AppendResult
	offsetCounter int64
}

// Struct for each instance - one per output
type outputConfig struct {
	messageDescriptor     protoreflect.MessageDescriptor
	streamType            managedwriter.StreamType
	currProjectID         string
	tableRef              string
	schemaDesc            *descriptorpb.DescriptorProto
	enableRetry           bool
	maxQueueBytes         int
	maxQueueRequests      int
	managedStreamSlice    *[]*streamConfig
	client                ManagedWriterClient
	maxChunkSize          int
	mutex                 sync.Mutex
	exactlyOnce           bool
	requestCountThreshold int
	numRetries            int
}

var (
	ms_ctx    = context.Background()
	configMap = make(map[int]*outputConfig)
	configID  = 0
)

const (
	chunkSizeLimit             = 9 * 1024 * 1024
	queueRequestDefault        = 1000
	queueByteDefault           = 100 * 1024 * 1024
	exactlyOnceDefault         = false
	queueRequestScalingPercent = 0.8
	numRetriesDefault          = 4
	maxNumStreamsPerInstance   = 10
	minQueueRequests           = 10
	dateTimeDefault            = true
)

// This function mangles the top-level and complex (struct) BigQuery schema to convert NUMERIC, BIGNUMERIC, DATETIME, TIME, and JSON fields to STRING.
func mangleInputSchema(input *storagepb.TableSchema, dataTimeString bool) *storagepb.TableSchema {
	if input == nil {
		return nil
	}
	// Create a clone of the table schema
	newMsg := proto.Clone(input).(*storagepb.TableSchema)
	newMsg.Fields = make([]*storagepb.TableFieldSchema, len(input.GetFields()))
	for k, f := range input.GetFields() {
		// Create a clone of the field
		newF := proto.Clone(f).(*storagepb.TableFieldSchema)
		switch newF.GetType() {
		// Overwrite the field to be string
		case storagepb.TableFieldSchema_NUMERIC,
			storagepb.TableFieldSchema_BIGNUMERIC,
			storagepb.TableFieldSchema_TIME,
			storagepb.TableFieldSchema_JSON:
			newF.Type = storagepb.TableFieldSchema_STRING

		}
		// If datatimestring is true, then set the field type to string
		if newF.GetType() == storagepb.TableFieldSchema_DATETIME && dataTimeString {
			newF.Type = storagepb.TableFieldSchema_STRING
		}
		// If the field is a struct type it will have a non-zero number of fields
		if len(newF.GetFields()) > 0 {
			// Call mangeInputSchema on the fields in the struct
			newF.Fields = mangleInputSchema(&storagepb.TableSchema{Fields: newF.Fields}, dataTimeString).Fields
		}
		newMsg.Fields[k] = newF
	}
	return newMsg
}

// This function handles getting data on the schema of the table data is being written to.
// The getDescriptors function returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto
func getDescriptors(curr_ctx context.Context, mw_client ManagedWriterClient, project string, dataset string, table string, dataTimeString bool) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	// Create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	// Create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	// Call getwritestream to get data on the table
	table_data, err := mw_client.GetWriteStream(curr_ctx, &req)
	if err != nil {
		return nil, nil, err
	}
	// Get the schema from table data
	init_table_schema := table_data.GetTableSchema()
	table_schema := mangleInputSchema(init_table_schema, dataTimeString)
	// Storage schema -> proto descriptor
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
	if err != nil {
		return nil, nil, err
	}
	// Proto descriptor -> message descriptor
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, errors.New("Message descriptor could not be created from table's proto descriptor")
	}

	// Message descriptor -> descriptor proto
	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, err
	}

	return messageDescriptor, dp, nil
}

// This function handles the data transformation from JSON to binary for a single json row.
// The outputs of this function are the corresponding binary data as well as any error that occur.
func jsonToBinary(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
	// JSON map -> JSON byte
	row, err := json.Marshal(jsonRow)
	if err != nil {
		return nil, err
	}
	// Create empty message
	message := dynamicpb.NewMessage(message_descriptor)

	// First, json byte -> proto message
	err = protojson.Unmarshal(row, message)
	if err != nil {
		return nil, err
	}

	// Then, proto message -> bytes.
	b, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// From https://github.com/majst01/fluent-bit-go-redis-output.git
// Function is used to transform fluent-bit record to a JSON map
func parseMap(mapInterface map[interface{}]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			// Prevent encoding to base64
			m[k.(string)] = string(t)
		case map[interface{}]interface{}:
			m[k.(string)] = parseMap(t)
		default:
			m[k.(string)] = v
		}
	}
	return m
}

var isReady = func(result *managedwriter.AppendResult) bool {
	select {
	case <-result.Ready():
		return true
	default:
		return false
	}
}

var pluginGetResult = func(result *managedwriter.AppendResult, ctx context.Context) (int64, error) {
	return result.GetResult(ctx)
}

// This function is used for asynchronous WriteAPI response checking
// It takes in the relevant queue of responses as well as boolean that indicates whether we should block the AppendRows function
// And wait for the next ready response from WriteAPI
// This function returns an int which is the length of the queue after being checked or -1 if an error occured
func checkResponses(curr_ctx context.Context, streamSlice *[]*streamConfig, waitForResponse bool, exactlyOnceConf bool, id int, streamIndex int) int {
	currQueuePointer := (*streamSlice)[streamIndex].appendResults
	for len(*currQueuePointer) > 0 {
		if exactlyOnceConf {
			log.Printf("Asynchronous response queue has non-zero size when exactly-once is configured")
			break
		}
		queueHead := (*currQueuePointer)[0]
		if waitForResponse || isReady(queueHead) {
			_, err := pluginGetResult(queueHead, curr_ctx)
			*currQueuePointer = (*currQueuePointer)[1:]
			if err != nil {
				log.Printf("Encountered error:%s while verifying the server response to a data append for output instance with id: %d", err, id)
			}
		} else {
			break
		}

	}
	return len(*currQueuePointer)
}

// This function checks the responses for all streams in the slice for each instance
func checkAllStreamResponses(ctx context.Context, streamSlice **[]*streamConfig, waitForResponse bool, currMutex *sync.Mutex, exactlyOnceConf bool, id int) {
	(*currMutex).Lock()
	defer (*currMutex).Unlock()
	sliceLen := len(**streamSlice)
	for i := 0; i < sliceLen; i++ {
		checkResponses(ctx, *streamSlice, waitForResponse, exactlyOnceConf, id, i)
	}
}

// This function gets the value of various configuration fields and returns an error if the field could not be parsed
func getConfigField[T int | bool](plugin unsafe.Pointer, key string, defaultval T) (T, error) {
	currstr := output.FLBPluginConfigKey(plugin, key)
	finval := defaultval
	if currstr != "" {
		switch any(defaultval).(type) {
		case int:
			intval, err := strconv.Atoi(currstr)
			if err != nil {
				return defaultval, err
			} else {
				finval = any(intval).(T)
			}
		case bool:
			boolval, err := strconv.ParseBool(currstr)
			if err != nil {
				return defaultval, err
			} else {
				finval = any(boolval).(T)
			}
		}
	}
	return finval, nil
}

// This function creates a new managed stream based on the config struct fields
func buildStream(ctx context.Context, config **outputConfig, streamIndex int) error {
	currManagedStream, err := getWriter((*config).client, ctx, (*config).currProjectID,
		managedwriter.WithType((*config).streamType),
		managedwriter.WithDestinationTable((*config).tableRef),
		// Use the descriptor proto when creating the new managed stream
		managedwriter.WithSchemaDescriptor((*config).schemaDesc),
		managedwriter.EnableWriteRetries((*config).enableRetry),
		managedwriter.WithMaxInflightBytes((*config).maxQueueBytes),
		managedwriter.WithMaxInflightRequests((*config).maxQueueRequests),
		managedwriter.WithTraceID("FluentBit"),
	)

	streamSlice := *(*config).managedStreamSlice

	if err == nil {
		(streamSlice)[streamIndex].managedstream = currManagedStream
	}
	return err
}

// This function returns whether or not the response indicates an invalid stream
func rebuildPredicate(err error) bool {
	if apiErr, ok := apierror.FromError(err); ok {
		storageErr := &storagepb.StorageError{}
		if e := apiErr.Details().ExtractProtoMessage(storageErr); e != nil {
			return storageErr.GetCode() == storagepb.StorageError_INVALID_STREAM_TYPE
		}
	}
	return false
}

// This function sends and checks the responses for data through a committed stream with exactly once functionality
func sendRequestExactlyOnce(ctx context.Context, data [][]byte, config **outputConfig, streamIndex int) error {
	(*config).mutex.Lock()
	defer (*config).mutex.Unlock()

	currStream := (*(*config).managedStreamSlice)[streamIndex]

	appendResult, err := currStream.managedstream.AppendRows(ctx, data, managedwriter.WithOffset(currStream.offsetCounter))
	if err != nil {
		return err
	}
	// Synchronously check the response immediately after appending data with exactly once semantics
	_, err = pluginGetResult(appendResult, ctx)
	if err != nil {
		return err
	}
	return nil
}

// This function enables synchronous retries and rebuilding a valid stream based on the server response
func sendRequestRetries(ctx context.Context, data [][]byte, config **outputConfig, streamIndex int) error {
	retryer := newStatelessRetryer((*config).numRetries)
	attempt := 0
	currStream := (*(*config).managedStreamSlice)[streamIndex]
	for {
		err := sendRequestExactlyOnce(ctx, data, config, streamIndex)
		if err == nil {
			break
		}
		// Unsuccesful data append
		if rebuildPredicate(err) {
			currStream.managedstream.Finalize(ctx)
			currStream.managedstream.Close()
			// Rebuild stream
			err := buildStream(ctx, config, streamIndex)
			if err != nil {
				return err
			}
			// Retry sending data without incrementing number of attempts or waiting between attempts
		} else {
			backoffPeriod, shouldRetry := retryer.Retry(err, attempt)
			if !shouldRetry {
				return err
			}
			// Retry sending data after incrementing attempt count and waiting for designated amount of time
			attempt++
			time.Sleep(backoffPeriod)
		}

	}
	return nil
}

// This function sends data and appends the responses to a queue to be checked asynchronously through a default stream with at least once functionality
func sendRequestDefault(ctx context.Context, data [][]byte, config **outputConfig, streamIndex int) error {
	(*config).mutex.Lock()
	defer (*config).mutex.Unlock()
	currStream := (*(*config).managedStreamSlice)[streamIndex]

	appendResult, err := currStream.managedstream.AppendRows(ctx, data)
	if err != nil {
		return err
	}

	*currStream.appendResults = append(*currStream.appendResults, appendResult)
	return nil
}

// This function cases on the exactly/at-least once functionality and sends the data accordingly
func sendRequest(ctx context.Context, data [][]byte, config **outputConfig, streamIndex int) error {
	if len(data) > 0 {
		if (*config).exactlyOnce {
			return sendRequestRetries(ctx, data, config, streamIndex)
		} else {
			return sendRequestDefault(ctx, data, config, streamIndex)
		}
	}
	return nil
}

// This is a test-only method that provides the instance count for configMap
func getInstanceCount() int {
	return len(configMap)
}

// Finds the stream index when dynamically scaling
func getLeastLoadedStream(streamSlice *[]*streamConfig) int {
	min := len(*(*streamSlice)[0].appendResults)
	minStreamIndex := 0
	for streamIndex, stream := range *streamSlice {
		if len(*stream.appendResults) < min {
			min = len(*stream.appendResults)
			minStreamIndex = streamIndex
		}
	}
	return minStreamIndex
}

// This is a test-only method which takes in a config id and returns the current offset value of the struct corresponding to the id
func getOffset(id int) int64 {
	config := configMap[id]
	streamSlice := *config.managedStreamSlice
	return streamSlice[0].offsetCounter
}

// Method to determine threshold
var setThreshold = func(maxQueueSize int) int {
	requestCountThreshold := int(math.Floor(queueRequestScalingPercent * float64(maxQueueSize)))
	if requestCountThreshold < minQueueRequests {
		requestCountThreshold = minQueueRequests
	}
	return requestCountThreshold
}

// This function check whether there is room for scaling and the scales the number of stream dynamically depending on if it
// Detects back pressure from the queue
func createNewStreamDynamicScaling(config **outputConfig) {
	(*config).mutex.Lock()
	defer (*config).mutex.Unlock()
	if len(*(*config).managedStreamSlice) < maxNumStreamsPerInstance {
		// Gets stream with least values in queue
		mostEfficient := getLeastLoadedStream((*config).managedStreamSlice)
		mostEfficientQueueLength := len(*(*(*config).managedStreamSlice)[mostEfficient].appendResults)

		var newResQueue []*managedwriter.AppendResult
		var newStream = streamConfig{
			offsetCounter: 0,
			appendResults: &newResQueue,
		}

		if mostEfficientQueueLength > (*config).requestCountThreshold {
			*(*config).managedStreamSlice = append(*(*config).managedStreamSlice, &newStream)
			newStreamIndex := len(*(*config).managedStreamSlice) - 1
			err := buildStream(ms_ctx, config, newStreamIndex)
			if err != nil {
				log.Printf("Creating an additional managed stream with destination table: %s failed in FLBPluginInit: %s", (*config).tableRef, err)
				// If failure, failed stream is removed from slice
				*(*config).managedStreamSlice = (*(*config).managedStreamSlice)[:newStreamIndex]
			}

		}
	}
}

// This interface acts as a wrapper for the *managedwriter.Client type which the realManagedWriterClient struct implements
// With its actual methods.
type ManagedWriterClient interface {
	NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	Close() error
	BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error)
	CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
}

type realManagedWriterClient struct {
	currClient *managedwriter.Client
}

func (r *realManagedWriterClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
	return r.currClient.NewManagedStream(ctx, opts...)

}

func (r *realManagedWriterClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.GetWriteStream(ctx, req, opts...)
}

func (r *realManagedWriterClient) Close() error {
	return r.currClient.Close()
}

func (r *realManagedWriterClient) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	return r.currClient.BatchCommitWriteStreams(ctx, req, opts...)
}

func (r *realManagedWriterClient) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.CreateWriteStream(ctx, req, opts...)
}

// This function acts as a wrapper for the managedwriter.NewClient function in order to inject a mock interface, one can
// Override the getClient method to return a different struct type (that still implements the managedwriterclient interface)
var getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
	client, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &realManagedWriterClient{currClient: client}, nil
}

type MWManagedStream interface {
	AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error)
	Close() error
	Finalize(ctx context.Context, opts ...gax.CallOption) (int64, error)
	FlushRows(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error)
	StreamName() string
}

// To inject a mock interface, we can override getWriter and getContext
var getWriter = func(client ManagedWriterClient, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
	return client.NewManagedStream(ctx, opts...)
}

// This function acts as a wrapper for the GetContext function so that we may override it to
// Mock it whenever needed
var getFLBPluginContext = func(ctx unsafe.Pointer) int {
	return output.FLBPluginGetContext(ctx).(int)
}

// Finalizes and Closes all streams in slice for a given instance
func finalizeCloseAllStreams(config **outputConfig, id int) bool {
	(*config).mutex.Lock()
	defer (*config).mutex.Unlock()
	errFlag := false
	streamSlice := (*config).managedStreamSlice
	for i := 0; i < len(*(*config).managedStreamSlice); i++ {
		if (*streamSlice)[i].managedstream != nil {
			if (*config).exactlyOnce {
				if _, err := (*streamSlice)[i].managedstream.Finalize(ms_ctx); err != nil {
					log.Printf("Finalizing managed stream for output instance with id %d and stream index %d failed in FLBPluginExit: %s", id, i, err)
					errFlag = true
				}
			}
			if err := (*streamSlice)[i].managedstream.Close(); err != nil {
				log.Printf("Closing managed stream for output instance with id %d and stream index %d failed in FLBPluginExitCtx: %s", id, i, err)
				errFlag = true
			}
		}
	}
	return errFlag
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "writeapi", "Sends data to BigQuery through WriteAPI")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Set projectID, datasetID, and tableID from config file params
	projectID := output.FLBPluginConfigKey(plugin, "ProjectID")
	datasetID := output.FLBPluginConfigKey(plugin, "DatasetID")
	tableID := output.FLBPluginConfigKey(plugin, "TableID")

	// Set exactly-once bool from config file param
	exactlyOnceVal, err := getConfigField(plugin, "Exactly_Once", exactlyOnceDefault)
	if err != nil {
		log.Printf("Invalid Exactly_Once parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}

	// Optional num synchronous retries parameter
	// This value is only used when the exactly-once field is configured to true (as it describes synchronous retries)
	numRetriesVal, err := getConfigField(plugin, "Num_Synchronous_Retries", numRetriesDefault)
	if err != nil {
		log.Printf("Invalid Num_Synchronous_Retries parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}

	// Optional maxchunksize param
	maxChunkSize_init, err := getConfigField(plugin, "Max_Chunk_Size", chunkSizeLimit)
	if err != nil {
		log.Printf("Invalid Max_Chunk_Size parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}
	if maxChunkSize_init > chunkSizeLimit {
		log.Printf("Max_Chunk_Size was set to: %d, but a single call to AppendRows cannot exceed 9 MB. Defaulting to 9 MB", maxChunkSize_init)
		maxChunkSize_init = chunkSizeLimit
	}

	// Optional max queue size params
	maxQueueSize, err := getConfigField(plugin, "Max_Queue_Requests", queueRequestDefault)
	if err != nil {
		log.Printf("Invalid Max_Queue_Requests parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}
	// Multiply floats, floor it, then convert it to integer for ease of use in Flush
	requestCountThreshold := setThreshold(maxQueueSize)
	maxQueueByteSize, err := getConfigField(plugin, "Max_Queue_Bytes", queueByteDefault)
	if err != nil {
		log.Printf("Invalid Max_Queue_Bytes parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}

	dateTimeStringType, err := getConfigField(plugin, "DateTime_String_Type", dateTimeDefault)
	if err != nil {
		log.Printf("Invalid DateTime_Input_Type parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}

	// Create new client
	client, err := getClient(ms_ctx, projectID)
	if err != nil {
		log.Printf("Creating a new managed BigQuery Storage write client scoped to: %s failed in FLBPluginInit: %s", projectID, err)
		return output.FLB_ERROR
	}

	// Create stream name
	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	// Call getDescriptors to get the message descriptor, and descriptor proto
	md, descriptor, err := getDescriptors(ms_ctx, client, projectID, datasetID, tableID, dateTimeStringType)
	if err != nil {
		log.Printf("Getting message descriptor and descriptor proto for table: %s failed in FLBPluginInit: %s", tableReference, err)
		return output.FLB_ERROR
	}

	// Set the stream type based on exactly once parameter
	var currStreamType managedwriter.StreamType
	var enableRetries bool
	if exactlyOnceVal {
		currStreamType = managedwriter.CommittedStream
	} else {
		currStreamType = managedwriter.DefaultStream
		enableRetries = true
	}

	var res_temp []*managedwriter.AppendResult
	streamSlice := []*streamConfig{}

	// Creates struct for stream and appends to slice
	initStream := streamConfig{
		offsetCounter: 0,
		appendResults: &res_temp,
	}
	streamSlice = append(streamSlice, &initStream)

	// Instantiates output instance
	config := outputConfig{
		messageDescriptor:     md,
		streamType:            currStreamType,
		tableRef:              tableReference,
		currProjectID:         projectID,
		schemaDesc:            descriptor,
		enableRetry:           enableRetries,
		maxQueueBytes:         maxQueueByteSize,
		maxQueueRequests:      maxQueueSize,
		client:                client,
		maxChunkSize:          maxChunkSize_init,
		exactlyOnce:           exactlyOnceVal,
		numRetries:            numRetriesVal,
		requestCountThreshold: requestCountThreshold,
		managedStreamSlice:    &streamSlice,
	}

	// Create stream using NewManagedStream
	configPointer := &config
	err = buildStream(ms_ctx, &configPointer, 0)
	if err != nil {
		log.Printf("Creating a new managed stream with destination table: %s failed in FLBPluginInit: %s", tableReference, err)
		return output.FLB_ERROR
	}

	configMap[configID] = &config

	// Creating FLB context for each output, enables multiinstancing
	config.mutex.Lock()
	output.FLBPluginSetContext(plugin, configID)
	configID = configID + 1
	config.mutex.Unlock()

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Print("[multiinstance] Flush called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := getFLBPluginContext(ctx)
	// Locate stream in map
	// Look up through reference
	config, ok := configMap[id]
	if !ok {
		log.Printf("Finding configuration for output instance with id: %d failed in FLBPluginFlushCtx", id)
		return output.FLB_ERROR
	}

	// Calls checkResponses for all streams in slice
	checkAllStreamResponses(ms_ctx, &config.managedStreamSlice, false, &config.mutex, config.exactlyOnce, id)
	// Checks for need to dynamically scale
	createNewStreamDynamicScaling(&config)

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	var binaryData [][]byte
	var currsize int
	// Keeps track of the number of rows previously sent
	var rowCounter int64

	// Find stream with least number of awaiting queue responses
	config.mutex.Lock()
	leastLoadedStreamIndex := getLeastLoadedStream(config.managedStreamSlice)
	config.mutex.Unlock()

	// Iterate Records
	for {
		// Extract Record
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		rowJSONMap := parseMap(record)

		// Serialize data
		// Transform each row of data into binary using the jsonToBinary function and the message descriptor from the getDescriptors function
		buf, err := jsonToBinary(config.messageDescriptor, rowJSONMap)
		if err != nil {
			log.Printf("Transforming row with value:%s from JSON to binary data for output instance with id: %d failed in FLBPluginFlushCtx: %s", rowJSONMap, id, err)
		} else {
			// Successful data transformation
			if (currsize + len(buf)) >= config.maxChunkSize {
				// Appending Rows
				err := sendRequest(ms_ctx, binaryData, &config, leastLoadedStreamIndex)
				if err != nil {
					log.Printf("Appending data for output instance with id: %d failed in FLBPluginFlushCtx: %s", id, err)
				} else if config.exactlyOnce {
					config.mutex.Lock()
					(*config.managedStreamSlice)[leastLoadedStreamIndex].offsetCounter += rowCounter
					config.mutex.Unlock()
				}

				rowCounter = 0

				binaryData = nil
				currsize = 0

			}
			binaryData = append(binaryData, buf)
			// Include the protobuf overhead to the currsize variable
			currsize += (len(buf) + 2)
			rowCounter++
		}
	}
	// Appending Rows
	err := sendRequest(ms_ctx, binaryData, &config, leastLoadedStreamIndex)
	if err != nil {
		log.Printf("Appending data for output instance with id: %d failed in FLBPluginFlushCtx: %s", id, err)
	} else if config.exactlyOnce {
		config.mutex.Lock()
		(*config.managedStreamSlice)[leastLoadedStreamIndex].offsetCounter += rowCounter
		config.mutex.Unlock()
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[multiinstance] Exit called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	// Get context
	id := output.FLBPluginGetContext(ctx).(int)

	// Locate stream in map
	config, ok := configMap[id]
	if !ok {
		log.Printf("Finding configuration for output instance with id: %d failed in FLBPluginExitCtx", id)
		return output.FLB_ERROR
	}

	// Calls checkResponses, finalizes, and closes each stream
	// If there is an error it preserves it and then continues to close and finalize all other streams
	checkAllStreamResponses(ms_ctx, &config.managedStreamSlice, false, &config.mutex, config.exactlyOnce, id)
	errFlag := finalizeCloseAllStreams(&config, id)

	if config.client != nil {
		if err := config.client.Close(); err != nil {
			log.Printf("Closing managed writer client for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
			errFlag = true
		}
	}

	if errFlag {
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[multiinstance] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}