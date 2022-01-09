package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Deps struct {
	DB influxdb2.Client
}

func main() {
	dbURL, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		dbURL = "http://localhost:8086"
	}

	dbToken, ok := os.LookupEnv("DATABASE_TOKEN")
	if !ok {
		dbToken = "Gr28ZC4m82jgyM"
	}

	mrand.Seed(time.Now().UnixMicro())
	db := influxdb2.NewClient(dbURL, dbToken)
	defer db.Close()

	deps := &Deps{DB: db}

	var data []Data
	for i := 0; i < 5; i++ {
		id := GenerateRandomID()
		for j := 0; j < mrand.Intn(15); j++ {
			d := Data{
				Measurement: "entity",
				ID:          id,
				Width:       mrand.Int63(),
				Height:      mrand.Int63(),
				Timestamp:   randomTimestamp(),
			}
			data = append(data, d)
		}
	}

	fmt.Printf("Generated %d data\n", len(data))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	err := deps.WriteData(ctx, data)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("data successfully inserted")

	n := time.Now()

	response, err := deps.ReadAllData("entity")
	if err != nil {
		fmt.Println(err)
		return
	}

	w := time.Now()

	jsonData, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(string(jsonData))

	fmt.Printf(
		"Time wasted for reading the data: %d ms for data length of %d and response length of %d\n",
		(w.UnixMilli() - n.UnixMilli()),
		len(data),
		len(response),
	)
}

func randomTimestamp() time.Time {
	randomTime := mrand.Int63n(time.Now().Unix()-94608000) + 94608000

	randomNow := time.Unix(randomTime, 0)

	return randomNow
}

func GenerateRandomID() string {
	out := make([]byte, 10)
	rand.Read(out)
	return hex.EncodeToString(out)
}

type Data struct {
	Measurement   string
	ID            string
	Width         int64
	Height        int64
	Timestamp     time.Time
	tablePosition int
}

func (d *Deps) WriteData(ctx context.Context, data []Data) error {

	writeAPI := d.DB.WriteAPI("root", "public")

	go func() {
		err := <-writeAPI.Errors()
		if err != nil {
			fmt.Println(err)
		}
	}()

	for _, datum := range data {
		point := influxdb2.NewPoint(
			datum.Measurement,
			map[string]string{
				"id": datum.ID,
			},
			map[string]interface{}{
				"width":  datum.Width,
				"height": datum.Height,
			},
			datum.Timestamp,
		)
		writeAPI.WritePoint(point)
	}

	writeAPI.Flush()

	return nil
}

func (d *Deps) ReadAllData(measurement string) ([]Data, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)

	queryAPI := d.DB.QueryAPI("root")

	wg := sync.WaitGroup{}
	wg.Add(2)

	var widthData []Data
	var heightData []Data

	// width
	go func() {
		rows, err := queryAPI.Query(
			ctx,
			`from (bucket: "public")
			|> range(start: 0)
			|> filter(fn: (r) => r["_measurement"] == "entity")
			|> filter(fn: (r) => r["_field"] == "width")
			|> aggregateWindow(every: 1h, fn: last, createEmpty: false)
			|> yield(name: "last")`,
		)
		if err != nil {
			cancel()
			log.Fatal(err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var tempData Data
			record := rows.Record()
			tempData.Measurement = record.Measurement()
			tempData.ID = record.ValueByKey("id").(string)
			tempData.Timestamp = record.Time()
			tempData.Width = record.Value().(int64)
			tempData.tablePosition = record.Table()
			widthData = append(widthData, tempData)
		}

		wg.Done()
	}()

	// height
	go func() {
		rows, err := queryAPI.Query(
			ctx,
			`from (bucket: "public")
			|> range(start: 0)
			|> filter(fn: (r) => r["_measurement"] == "entity")
			|> filter(fn: (r) => r["_field"] == "height")
			|> aggregateWindow(every: 1h, fn: last, createEmpty: false)
			|> yield(name: "last")`,
		)
		if err != nil {
			cancel()
			log.Fatal(err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var tempData Data
			record := rows.Record()
			tempData.Measurement = record.Measurement()
			tempData.ID = record.ValueByKey("id").(string)
			tempData.Timestamp = record.Time()
			tempData.Height = record.Value().(int64)
			tempData.tablePosition = record.Table()
			heightData = append(heightData, tempData)
		}

		wg.Done()
	}()

	wg.Wait()

	size := getBiggestInt(len(widthData), len(heightData))
	data := make([]Data, size)
	copy(data, widthData)

	for i, d := range heightData {
		if data[i].Timestamp.UnixMicro() == d.Timestamp.UnixMicro() && data[i].ID == d.ID {
			data[i].Height = d.Height
		} else {
			fmt.Printf("Something is wrong on index position %d.\n\nExpecting: %v\nGot value: %v\n", i, data[i], d)
		}
	}

	return data, nil
}

func getBiggestInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}
