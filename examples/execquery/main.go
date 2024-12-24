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
