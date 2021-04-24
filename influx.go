package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/influxdata/influxdb/client"
)

const influxVersion = "1.3.0+20210424"

type CsvLine struct {
	t time.Time
	Time,
	Temperature1,
	Temperature2,
	Temperature3,
	Temperature4,
	Out1,
	Out2 string
}

func main() {
	if len(currentConfig.Database.Name) == 0 {
		currentConfig.Database.Name = "mydb"
	}

	ic, err := connect(currentConfig.Server.Host, currentConfig.Server.Port, currentConfig.Server.User, currentConfig.Server.Password)
	if err != nil {
		panic(err)
	}
	log.Println("Successfully connect to influxdb ...")

	if err = insert(ic, currentConfig.Database.Name, currentConfig.Import.Path); err != nil {
		panic(err)
	}
}

func connect(host, port, user, password string) (*client.Client, error) {
	u, err := url.Parse(fmt.Sprintf("http://%s:%s", host, port))
	if err != nil {
		return nil, err
	}

	ic, err := client.NewClient(client.Config{URL: *u})
	if err != nil {
		return nil, err
	}

	if _, _, err := ic.Ping(); err != nil {
		return nil, err
	}

	ic.SetAuth(user, password)
	return ic, nil
}

func create(ic *client.Client, db string, drop bool) error {
	if drop {
		q := client.Query{
			Command:  fmt.Sprintf("drop database %s", db),
			Database: db,
		}
		_, _ = ic.Query(q)
	}

	q := client.Query{
		Command:  fmt.Sprintf("create database %s", db),
		Database: db,
	}

	// ignore the error of existing database
	_, err := ic.Query(q)
	return err
}

func insert(ic *client.Client, db, dir string) (err error) {
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = ';'
		return r
	})

	var files []string
	if files, err = filepath.Glob(filepath.Join(dir, "E*.csv")); err != nil {
		return
	}

	for _, fileName := range files {
		log.Printf("read File: %s\n", fileName)

		var csvFile *os.File
		if csvFile, err = os.OpenFile(fileName, os.O_RDONLY, os.ModePerm); err != nil {
			return
		}

		defer csvFile.Close()

		var data []*CsvLine
		if err = gocsv.UnmarshalFile(csvFile, &data); err != nil {
			return
		}

		if len(data) == 0 {
			continue
		}

		pts := make([]client.Point, 0, len(data))

		for _, d := range data {
			d.t, _ = time.ParseInLocation("2006-01-02 15:04:05", d.Time, time.Local)

			p := client.Point{
				Measurement: "solar",
				Time:        d.t,
				Fields: map[string]interface{}{
					"Tcol":  string2float(d.Temperature1),
					"Tboil": string2float(d.Temperature2),
					"Trl":   string2float(d.Temperature3),
					"Tvl":   string2float(d.Temperature4),
					"Psol":  string2int(d.Out2),
					"Phe":   string2int(d.Out2),
				},
				Precision: "",
				Raw:       "",
			}

			pts = append(pts, p)
		}

		if err = writeInflux(ic, db, pts); err != nil {
			return
		}
	}

	return
}

func writeInflux(ic *client.Client, db string, pts []client.Point) (err error) {
	const step = 200000
	for x := 0; x < len(pts); x += step {
		end := x + step
		if end > len(pts) {
			end = len(pts)
		}
		bps := client.BatchPoints{
			Points: pts[x:end],
			Tags: map[string]string{
				"location": "Wullersdorf",
			},
			Database:        db,
			RetentionPolicy: "",
		}
		log.Printf("write record %v to %v\n", x, end)

		if _, err = ic.Write(bps); err != nil {
			return
		}
	}
	return
}

func string2float(s string) float64 {
	f, _ := strconv.ParseFloat(strings.ReplaceAll(s, ",", "."), 32)
	return f
}

func string2int(s string) int {
	i, _ := strconv.ParseInt(s, 0, 8)
	return int(i)
}
