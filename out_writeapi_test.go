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
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// This is a mock struct describing the states of the plugin after being register
type MockFLBPlugin struct {
	name string
	desc string
}

// This function mocks output.FLBPluginRegister by setting the fields of the MockFLBPlugin struct to the input parameter
// And returning a 0 (to imply success)
func (m *MockFLBPlugin) mockOutputRegister(def unsafe.Pointer, currname string, currdesc string) int {
	m.name = currname
	m.desc = currdesc

	return 0
}

// This function tests FLBPluginRegister
func TestFLBPluginRegister(t *testing.T) {
	currplugin := &MockFLBPlugin{}

	patch := monkey.Patch(output.FLBPluginRegister, currplugin.mockOutputRegister)

	defer patch.Unpatch()

	result := FLBPluginRegister(nil)

	assert.Equal(t, 0, result)
	assert.Equal(t, "writeapi", currplugin.name)

}

// This is a struct keeping track of whether the correct options are sent in NewManagedStream
type OptionChecks struct {
	configProjectID        bool
	configDatasetID        bool
	configTableID          bool
	configMaxChunkSize     bool
	configMaxQueueSize     bool
	configMaxQueueRequests bool
	configExactlyOnce      bool
	calledGetClient        int
	calledNewManagedStream int
	calledGetWriteStream   int
	calledSetContext       int
	numInputs              bool
	mapSizeIncremented     bool
}

type MockManagedWriterClient struct {
	client                      *managedwriter.Client
	NewManagedStreamFunc        func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStreamFunc          func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	CloseFunc                   func() error
	BatchCommitWriteStreamsFunc func(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error)
	CreateWriteStreamFunc       func(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
}

func (m *MockManagedWriterClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
	return m.NewManagedStreamFunc(ctx, opts...)
}

func (m *MockManagedWriterClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return m.GetWriteStreamFunc(ctx, req, opts...)
}

func (m *MockManagedWriterClient) Close() error {
	return m.client.Close()
}

func (m *MockManagedWriterClient) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	return m.client.BatchCommitWriteStreams(ctx, req, opts...)
}

func (m *MockManagedWriterClient) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return m.client.CreateWriteStream(ctx, req, opts...)
}

// TestFLBPluginInit tests the FLBPluginInit function
func TestFLBPluginInit(t *testing.T) {
	var currChecks OptionChecks
	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			currChecks.calledNewManagedStream++
			if len(opts) == 7 {
				currChecks.numInputs = true
			}
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			currChecks.calledGetWriteStream++
			return &storagepb.WriteStream{
				Name: "mockstream",
				TableSchema: &storagepb.TableSchema{
					Fields: []*storagepb.TableFieldSchema{
						{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
						{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					},
				},
			}, nil
		},
	}

	originalFunc := getClient
	getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
		currChecks.calledGetClient++
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		switch key {
		case "ProjectID":
			currChecks.configProjectID = true
			return "DummyProjectId"
		case "DatasetID":
			currChecks.configDatasetID = true
			return "DummyDatasetId"
		case "TableID":
			currChecks.configTableID = true
			return "DummyTableId"
		case "Max_Chunk_Size":
			currChecks.configMaxChunkSize = true
			return "0"
		case "Max_Queue_Requests":
			currChecks.configMaxQueueRequests = true
			return "0"
		case "Max_Queue_Bytes":
			currChecks.configMaxQueueSize = true
			return "0"
		case "Exactly_Once":
			currChecks.configExactlyOnce = true
			return "False"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patch2 := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		currChecks.calledSetContext++
	})
	defer patch2.Unpatch()

	plugin := unsafe.Pointer(nil)
	initsize := getInstanceCount()
	result := FLBPluginInit(plugin)
	finsize := getInstanceCount()
	if (finsize - 1) == initsize {
		currChecks.mapSizeIncremented = true
	}
	assert.Equal(t, output.FLB_OK, result)
	assert.True(t, currChecks.configProjectID)
	assert.True(t, currChecks.configDatasetID)
	assert.True(t, currChecks.configTableID)
	assert.True(t, currChecks.configMaxChunkSize)
	assert.True(t, currChecks.configMaxQueueRequests)
	assert.True(t, currChecks.configMaxQueueSize)
	assert.True(t, currChecks.configExactlyOnce)
	assert.Equal(t, 1, currChecks.calledGetClient)
	assert.Equal(t, 1, currChecks.calledGetWriteStream)
	assert.Equal(t, 1, currChecks.calledNewManagedStream)
	assert.Equal(t, 1, currChecks.calledSetContext)
	assert.True(t, currChecks.numInputs)
	assert.True(t, currChecks.mapSizeIncremented)

}

// This test checks that all relevant functions are called in init when exactly once is set to true
func TestFLBPluginInitExactlyOnce(t *testing.T) {
	var currChecks OptionChecks
	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			currChecks.calledNewManagedStream++
			if len(opts) == 7 {
				currChecks.numInputs = true
			}
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			currChecks.calledGetWriteStream++
			return &storagepb.WriteStream{
				Name: "mockstream",
				TableSchema: &storagepb.TableSchema{
					Fields: []*storagepb.TableFieldSchema{
						{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
						{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					},
				},
			}, nil
		},
	}

	originalFunc := getClient
	getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
		currChecks.calledGetClient++
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		switch key {
		case "ProjectID":
			currChecks.configProjectID = true
			return "DummyProjectId"
		case "DatasetID":
			currChecks.configDatasetID = true
			return "DummyDatasetId"
		case "TableID":
			currChecks.configTableID = true
			return "DummyTableId"
		case "Max_Chunk_Size":
			currChecks.configMaxChunkSize = true
			return "0"
		case "Max_Queue_Requests":
			currChecks.configMaxQueueRequests = true
			return "0"
		case "Max_Queue_Bytes":
			currChecks.configMaxQueueSize = true
			return "0"
		case "Exactly_Once":
			currChecks.configExactlyOnce = true
			return "True"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patch2 := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		currChecks.calledSetContext++
	})
	defer patch2.Unpatch()

	plugin := unsafe.Pointer(nil)
	initsize := getInstanceCount()
	result := FLBPluginInit(plugin)
	finsize := getInstanceCount()
	if (finsize - 1) == initsize {
		currChecks.mapSizeIncremented = true
	}
	assert.Equal(t, output.FLB_OK, result)
	assert.True(t, currChecks.configProjectID)
	assert.True(t, currChecks.configDatasetID)
	assert.True(t, currChecks.configTableID)
	assert.True(t, currChecks.configMaxChunkSize)
	assert.True(t, currChecks.configMaxQueueRequests)
	assert.True(t, currChecks.configMaxQueueSize)
	assert.True(t, currChecks.configExactlyOnce)
	assert.Equal(t, 1, currChecks.calledGetClient)
	assert.Equal(t, 1, currChecks.calledGetWriteStream)
	assert.Equal(t, 1, currChecks.calledNewManagedStream)
	assert.Equal(t, 1, currChecks.calledSetContext)
	assert.True(t, currChecks.numInputs)
	assert.True(t, currChecks.mapSizeIncremented)

}

func TestFLBPluginFlushCtxExactlyOnce(t *testing.T) {
	checks := new(StreamChecks)
	var setID int

	testTableSchema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}

	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			return &storagepb.WriteStream{
				Name:        "mockstream",
				TableSchema: testTableSchema,
			}, nil
		},
	}

	originalFunc := getClient
	getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	testGetDescrip := func() (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
		table_schema := testTableSchema
		descriptor, _ := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
		messageDescriptor, _ := descriptor.(protoreflect.MessageDescriptor)
		dp, _ := adapt.NormalizeDescriptor(messageDescriptor)

		return messageDescriptor, dp
	}

	md, _ := testGetDescrip()
	mockMS := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			checks.appendRows++

			var combinedData []byte
			for _, tempData := range data {
				combinedData = append(combinedData, tempData...)
			}

			message := dynamicpb.NewMessage(md)
			err := proto.Unmarshal(combinedData, message)
			if err != nil {
				return nil, fmt.Errorf("Failed to unmarshal")
			}

			textField := message.Get(md.Fields().ByJSONName("Text"))
			timeField := message.Get(md.Fields().ByJSONName("Time"))

			assert.Equal(t, "FOO", textField.String())
			assert.Equal(t, "000", timeField.String())

			return nil, nil
		},
		FinalizeFunc: func(ctx context.Context, opts ...gax.CallOption) (int64, error) {
			return 0, nil
		},
		FlushRowsFunc: func(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error) {
			return 0, nil
		},
		StreamNameFunc: func() string {
			return ""
		},
	}

	origFunc := getWriter
	getWriter = func(client ManagedWriterClient, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
		return mockMS, nil
	}
	defer func() { getWriter = origFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		switch key {
		case "Exactly_Once":
			return "True"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patchSetContext := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		setID = ctx.(int)
	})
	defer patchSetContext.Unpatch()

	initRes := FLBPluginInit(nil)

	orgFunc := getFLBPluginContext
	getFLBPluginContext = func(ctx unsafe.Pointer) int {
		checks.calledGetContext++
		return setID
	}
	defer func() { getFLBPluginContext = orgFunc }()

	origReadyFunc := isReady
	isReady = func(queueHead *managedwriter.AppendResult) bool {
		// Response is always ready for test
		return true
	}
	defer func() { isReady = origReadyFunc }()

	origResultFunc := pluginGetResult
	pluginGetResult = func(queueHead *managedwriter.AppendResult, ctx context.Context) (int64, error) {
		checks.getResultsCount++
		return -1, nil
	}
	defer func() { pluginGetResult = origResultFunc }()

	patchDecoder := monkey.Patch(output.NewDecoder, func(data unsafe.Pointer, length int) *output.FLBDecoder {
		checks.createDecoder++
		return nil
	})
	defer patchDecoder.Unpatch()

	var rowSent int = 0
	var rowCount int = 5
	patchRecord := monkey.Patch(output.GetRecord, func(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{}) {
		checks.gotRecord++
		dummyRecord := make(map[interface{}]interface{})
		if rowSent < rowCount {
			rowSent++
			// Represents "FOO" in bytes as the data for the Text field
			dummyRecord["Text"] = []byte{70, 79, 79}
			// Represents "000" in bytes as the data for the Time field
			dummyRecord["Time"] = []byte{48, 48, 48}
			return 0, nil, dummyRecord
		}
		// Reset to prepare for next call to Flush
		rowSent = 0
		return 1, nil, nil
	})
	defer patchRecord.Unpatch()

	// Converts id (int) to type unsafe.Pointer to be used as the ctx
	uintptrValue := uintptr(setID)
	pointerValue := unsafe.Pointer(uintptrValue)

	// Calls FlushCtx with this ID
	result := FLBPluginFlushCtx(pointerValue, nil, 0, nil)
	// Expect the offset to equal the number of successful rows previously sent (with no server-side errors)
	assert.Equal(t, int64(rowCount), getOffset(setID))
	result = FLBPluginFlushCtx(pointerValue, nil, 0, nil)
	assert.Equal(t, int64(2*rowCount), getOffset(setID))

	// If we change number of rows, the number of times GetRecord is called changes. This finds the expected
	// Number without having to manually change it. Each time flush is called, GetRecord is called for the
	// Number of rows plus once to break the loop. Since flush is called twice, we multiply this by 2
	expectGotRecord := (rowCount + 1) * 2

	assert.Equal(t, output.FLB_OK, initRes)
	assert.Equal(t, output.FLB_OK, result)
	assert.Equal(t, 2, checks.appendRows)
	assert.Equal(t, 2, checks.calledGetContext)
	// Since exactly once has synchronous response checking, getResults should be called for each flush call
	assert.Equal(t, 2, checks.getResultsCount)
	assert.Equal(t, 2, checks.createDecoder)
	assert.Equal(t, expectGotRecord, checks.gotRecord)
}

// This function validates that the offset is not incremented when a server-side error occurs
func TestFLBPluginFlushCtxErrorHandling(t *testing.T) {
	checks := new(StreamChecks)
	var setID int

	testTableSchema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}

	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			return &storagepb.WriteStream{
				Name:        "mockstream",
				TableSchema: testTableSchema,
			}, nil
		},
	}

	originalFunc := getClient
	getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	testGetDescrip := func() (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
		table_schema := testTableSchema
		descriptor, _ := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
		messageDescriptor, _ := descriptor.(protoreflect.MessageDescriptor)
		dp, _ := adapt.NormalizeDescriptor(messageDescriptor)

		return messageDescriptor, dp
	}

	md, _ := testGetDescrip()
	mockMS := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			checks.appendRows++

			var combinedData []byte
			for _, tempData := range data {
				combinedData = append(combinedData, tempData...)
			}

			message := dynamicpb.NewMessage(md)
			err := proto.Unmarshal(combinedData, message)
			if err != nil {
				return nil, fmt.Errorf("Failed to unmarshal")
			}

			textField := message.Get(md.Fields().ByJSONName("Text"))
			timeField := message.Get(md.Fields().ByJSONName("Time"))

			assert.Equal(t, "FOO", textField.String())
			assert.Equal(t, "000", timeField.String())

			return nil, nil
		},
		FinalizeFunc: func(ctx context.Context, opts ...gax.CallOption) (int64, error) {
			return 0, nil
		},
		FlushRowsFunc: func(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error) {
			return 0, nil
		},
		StreamNameFunc: func() string {
			return ""
		},
	}

	origFunc := getWriter
	getWriter = func(client ManagedWriterClient, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
		return mockMS, nil
	}
	defer func() { getWriter = origFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		switch key {
		case "Exactly_Once":
			return "True"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patchSetContext := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		setID = ctx.(int)
	})
	defer patchSetContext.Unpatch()

	initRes := FLBPluginInit(nil)

	orgFunc := getFLBPluginContext
	getFLBPluginContext = func(ctx unsafe.Pointer) int {
		checks.calledGetContext++
		return setID
	}
	defer func() { getFLBPluginContext = orgFunc }()

	origReadyFunc := isReady
	isReady = func(queueHead *managedwriter.AppendResult) bool {
		// Response is always ready for test
		return true
	}
	defer func() { isReady = origReadyFunc }()

	origResultFunc := pluginGetResult
	var i int
	pluginGetResult = func(queueHead *managedwriter.AppendResult, ctx context.Context) (int64, error) {
		checks.getResultsCount++
		i++
		// Send a server-side error for every other request
		if i%2 == 0 {
			return -1, nil
		} else {
			return -1, errors.New("Mocked server-side error")
		}

	}
	defer func() { pluginGetResult = origResultFunc }()

	patchDecoder := monkey.Patch(output.NewDecoder, func(data unsafe.Pointer, length int) *output.FLBDecoder {
		checks.createDecoder++
		return nil
	})
	defer patchDecoder.Unpatch()

	var rowSent int = 0
	var rowCount int = 5
	patchRecord := monkey.Patch(output.GetRecord, func(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{}) {
		checks.gotRecord++
		dummyRecord := make(map[interface{}]interface{})
		if rowSent < rowCount {
			rowSent++
			// Represents "FOO" in bytes as the data for the Text field
			dummyRecord["Text"] = []byte{70, 79, 79}
			// Represents "000" in bytes as the data for the Time field
			dummyRecord["Time"] = []byte{48, 48, 48}
			return 0, nil, dummyRecord
		}
		// Reset to prepare for next call to Flush
		rowSent = 0
		return 1, nil, nil
	})
	defer patchRecord.Unpatch()

	// Converts id (int) to type unsafe.Pointer to be used as the ctx
	uintptrValue := uintptr(setID)
	pointerValue := unsafe.Pointer(uintptrValue)

	// Calls FlushCtx with this ID
	result := FLBPluginFlushCtx(pointerValue, nil, 0, nil)
	// Expect the offset not to increase due to mocked server-side error
	assert.Equal(t, int64(0), getOffset(setID))
	result = FLBPluginFlushCtx(pointerValue, nil, 0, nil)
	// Expect the offset to increase since no error was retured from pluginGetResult
	assert.Equal(t, int64(rowCount), getOffset(setID))

	// If we change number of rows, the number of times GetRecord is called changes. This finds the expected
	// Number without having to manually change it. Each time flush is called, GetRecord is called for the
	// Number of rows plus once to break the loop. Since flush is called twice, we multiply this by 2
	expectGotRecord := (rowCount + 1) * 2

	assert.Equal(t, output.FLB_OK, initRes)
	assert.Equal(t, output.FLB_OK, result)
	assert.Equal(t, 2, checks.appendRows)
	assert.Equal(t, 2, checks.calledGetContext)
	assert.Equal(t, 2, checks.getResultsCount)
	assert.Equal(t, 2, checks.createDecoder)
	assert.Equal(t, expectGotRecord, checks.gotRecord)
}
