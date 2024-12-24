# Go Redshift Client

This repository contains a Go client for interacting with Amazon Redshift. It provides functionalities to execute queries and unload data to S3.

## Features

- Execute SQL queries on Amazon Redshift
- Unload query results to S3
- Map query results to Go structs

## Installation

To install the package, use `go get`:

```sh
go get knakazawa99/goredshiftclient
```


## Usage
### Executing Queries
To execute a query and get the results:

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    redshiftwrapper "knakazawa99/goredshiftclient"
)

type Weather struct {
    ID          int     `json:"id"`
    Temperature float64 `json:"temperature"`
    Humidity    float64 `json:"humidity"`
}

type GetWeather struct {
    WeatherTable string
}

func (w GetWeather) GetQuery() string {
    return fmt.Sprintf("SELECT id, temperature, humidity FROM %s", w.WeatherTable)
}

func main() {
    ctx := context.Background()
    client, _ := redshiftwrapper.NewClientAPI(ctx)
    redshiftClient, err := redshiftwrapper.New(client, "redshift-unload", "dev", time.Duration(1))
    if err != nil {
        fmt.Println(fmt.Sprintf("failed to create redshift client: %v", err))
    }
    weatherQuery := GetWeather{
        WeatherTable: "dev.public.Weather",
    }
    query := weatherQuery.GetQuery()

    fmt.Println("query: ", query)

    results, err := redshiftClient.ExecQueryWithResult(ctx, query)
    if err != nil {
        fmt.Println(fmt.Sprintf("failed to execute ExecQueryWithResult: %v", err))
        return
    }
    var weathers []Weather
    if err := json.Unmarshal(results, &weathers); err != nil {
        fmt.Println(fmt.Sprintf("failed to unmarshal json: %v", err))
        return
    }

    for _, weather := range weathers {
        fmt.Printf("ID: %d, Temperature: %f, Humidity: %f\n", weather.ID, weather.Temperature, weather.Humidity)
    }
}
```


### Unloading Data
To unload query results to S3:
```go

package main

import (
    "context"
    "fmt"
    "time"

    redshiftwrapper "knakazawa99/goredshiftclient"
)

type GetWeather struct {
    WeatherTable string
}

func (w GetWeather) GetQuery() string {
    return fmt.Sprintf("SELECT id, temperature, humidity FROM %s", w.WeatherTable)
}

func main() {
    ctx := context.Background()
    client, _ := redshiftwrapper.NewClientAPI(ctx)
    redshiftClient, err := redshiftwrapper.New(client, "redshift-unload", "dev", time.Duration(1))
    if err != nil {
        fmt.Println(fmt.Sprintf("failed to create redshift client: %v", err))
    }
    weatherQuery := GetWeather{
        WeatherTable: "dev.public.Weather",
    }
    query := weatherQuery.GetQuery()

    fmt.Println("query: ", query)

    unloadOption := redshiftwrapper.NewDefaultUnloadOption("s3://redshift-unload-verification/unloadwrapper/")

    queryID, err := redshiftClient.ExecUnloadQuery(ctx, query, unloadOption)
    if err != nil {
        fmt.Println(fmt.Sprintf("failed to execute ExecQueryWithResult: %v", err))
        return
    }

    fmt.Println("queryID: ", *queryID)
}
```


## Dependencies
Go 1.21.4
AWS SDK for Go v2

## License
This project is licensed under the MIT License.
