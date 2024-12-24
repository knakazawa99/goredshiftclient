package main

import (
	"context"
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

	unloadOption := redshiftwrapper.NewDefaultUnloadOption("s3://redshift-unload-verification/unloadwrapper/")

	queryID, err := redshiftClient.ExecUnloadQuery(ctx, query, unloadOption)
	if err != nil {
		fmt.Println(fmt.Sprintf("failed to execute ExecQueryWithResult: %v", err))
		return
	}

	fmt.Println("queryID: ", *queryID)
}
