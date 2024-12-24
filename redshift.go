package goredshiftclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

type (
	Client struct {
		svc                 ClientAPI
		workgroupName       *string
		defaultDatabaseName string
		interval            time.Duration
	}

	ClientAPI interface {
		ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
		DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
		GetStatementResult(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error)
	}
)

func New(svc ClientAPI, workgroupName, defaultDatabaseName string, interval time.Duration) (*Client, error) {
	return &Client{
		svc:                 svc,
		workgroupName:       aws.String(workgroupName),
		defaultDatabaseName: defaultDatabaseName,
		interval:            interval,
	}, nil
}

// NewClientAPI creates a new Redshift client.
func NewClientAPI(ctx context.Context) (ClientAPI, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := redshiftdata.NewFromConfig(cfg)

	return client, nil
}

// ExecQueryWithResult executes a query and returns the result as a JSON byte array.
func (c *Client) ExecQueryWithResult(ctx context.Context, query string) ([]byte, error) {
	queryID, err := c.ExecQuery(ctx, c.defaultDatabaseName, query)
	if err != nil {
		return nil, fmt.Errorf("execute statement:%w", err)
	}
	if err := c.WatchQuery(ctx, queryID); err != nil {
		return nil, fmt.Errorf("cannot WatchQuery: %v", err)
	}

	result, err := c.svc.GetStatementResult(ctx, &redshiftdata.GetStatementResultInput{
		Id: queryID,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot GetStatementResult: %v", err)
	}

	columnNames := c.getColumnName(result.ColumnMetadata)
	mappings := c.mapRecordsToColumn(columnNames, result.Records)
	jsonBytes, err := json.Marshal(mappings)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal json:%v", err)
	}
	return jsonBytes, nil
}

type UnloadOption struct {
	S3Path         string
	IAMRole        string
	Format         string
	PartitionBy    []string
	Header         bool
	Delimiter      string
	FlexedWidth    string
	AllowOverwrite bool
	Parallel       bool
	MaxFileSize    string
	Extension      string
}

// NewDefaultUnloadOption returns the default UnloadOption.
func NewDefaultUnloadOption(s3Path string) UnloadOption {
	return UnloadOption{
		S3Path:      s3Path,
		IAMRole:     "default",
		Format:      "CSV",
		PartitionBy: nil,
		Header:      true,
		// MANIFEST
		Delimiter:      ",",
		AllowOverwrite: true,
		Parallel:       false,
		MaxFileSize:    "1GB",
		Extension:      "csv",
	}
}

// ExecUnloadQuery executes an unload query and returns the queryID.
func (c *Client) ExecUnloadQuery(ctx context.Context, query string, opt UnloadOption) (*string, error) {
	unloadQuery, err := c.buildUnloadQuery(ctx, query, opt)
	if err != nil {
		return nil, fmt.Errorf("generate unload query:%w", err)
	}
	queryID, err := c.ExecQuery(ctx, c.defaultDatabaseName, unloadQuery)
	if err != nil {
		return nil, fmt.Errorf("execute statement:%w", err)
	}
	if err := c.WatchQuery(ctx, queryID); err != nil {
		return nil, fmt.Errorf("cannot WatchQuery(queryID: %s): %v", *queryID, err)
	}
	return queryID, nil
}

// ExecQuery executes a query and returns the queryID.
func (c *Client) ExecQuery(ctx context.Context, databaseName, query string) (*string, error) {
	executeOutput, err := c.svc.ExecuteStatement(ctx, &redshiftdata.ExecuteStatementInput{
		Database:      aws.String(databaseName),
		Sql:           aws.String(query),
		WorkgroupName: c.workgroupName,
	})
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	return executeOutput.Id, nil
}

// WatchQuery waits until the query is finished.
func (c *Client) WatchQuery(ctx context.Context, queryID *string) error {
	for {
		describeOutput, err := c.svc.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{Id: queryID})
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		// https://docs.aws.amazon.com/sdk-for-go/api/service/redshiftdataapiservice/#DescribeStatementOutput
		if describeOutput.Status == types.StatusStringFinished {
			return nil
		}
		if describeOutput.Status == types.StatusStringAborted {
			return fmt.Errorf("%v", *describeOutput.Error)
		}
		if describeOutput.Status == types.StatusStringFailed {
			return fmt.Errorf("%v", *describeOutput.Error)
		}
		time.Sleep(c.interval)
	}
}

// buildUnloadQuery generates an unload query.
func (c *Client) buildUnloadQuery(ctx context.Context, query string, opt UnloadOption) (string, error) {
	if opt.S3Path == "" {
		return "", fmt.Errorf("S3Path is required")
	}

	unloadQuery := fmt.Sprintf("UNLOAD ($$ %s $$)\nTO '%s'\nIAM_ROLE %s", query, opt.S3Path, opt.IAMRole)

	if opt.Header {
		unloadQuery += "\nHEADER"
	}

	if opt.AllowOverwrite {
		unloadQuery += "\nALLOWOVERWRITE"
	}

	if !opt.Parallel {
		unloadQuery += "\nPARALLEL OFF"
	}

	unloadQuery += fmt.Sprintf("\nDELIMITER '%s'", opt.Delimiter)
	unloadQuery += fmt.Sprintf("\nFORMAT AS %s", opt.Format)
	unloadQuery += fmt.Sprintf("\nMAXFILESIZE %s", opt.MaxFileSize)
	unloadQuery += fmt.Sprintf("\nEXTENSION '%s'", opt.Extension)

	return unloadQuery, nil
}

// parseFiled parses the field value.
func (c *Client) parseFiled(f types.Field) interface{} {
	switch f := f.(type) {
	case *types.FieldMemberBlobValue:
		return f.Value
	case *types.FieldMemberBooleanValue:
		return f.Value
	case *types.FieldMemberDoubleValue:
		return f.Value
	case *types.FieldMemberIsNull:
		return ""
	case *types.FieldMemberLongValue:
		return f.Value
	case *types.FieldMemberStringValue:
		return f.Value
	default:
		return ""
	}
}

// getColumnName returns the column names.
func (c *Client) getColumnName(columnMetadata []types.ColumnMetadata) []string {
	columnNames := make([]string, len(columnMetadata))
	for i, column := range columnMetadata {
		columnNames[i] = *column.Name
	}
	return columnNames
}

// mapRecordsToColumn maps the records to the column names.
func (c *Client) mapRecordsToColumn(columnNames []string, records [][]types.Field) []map[string]interface{} {
	mappings := make([]map[string]interface{}, len(records))
	for i, row := range records {
		mapping := make(map[string]interface{})
		for j, field := range row {
			mapping[columnNames[j]] = c.parseFiled(field)
		}
		mappings[i] = mapping
	}
	return mappings
}
