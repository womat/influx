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
	"github.com/influxdb/influxdb/client"
)

const influxVersion = "1.2.0+20191123"

type CsvLine struct {
	t time.Time
	Datum,
	Zeit,
	Temp1,
	Temp2,
	Temp3,
	Temp4,
	Ausg1,
	Ausg2,
	Dummy string
}

func main() {
	// workaround, daocloud influxdb have not privision db instance
	if len(currentConfig.Database.Name) == 0 {
		currentConfig.Database.Name = "mydb"
	}

	ic, err := connect(currentConfig.Server.Host, currentConfig.Server.Port, currentConfig.Server.User, currentConfig.Server.Password)
	if err != nil {
		panic(err)
	}
	log.Println("Successfully connect to influxdb ...")

	//	if err = create(ic, currentConfig.Database.Name, currentConfig.Database.Drop); err != nil {
	//		panic(err)
	//	}

	insert(ic, currentConfig.Database.Name, currentConfig.Import.Path)

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

func insert(ic *client.Client, db, dir string) error {
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = ';'
		return r
	})

	pattern := filepath.Join(dir, "*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return (err)
	}

	for _, fileName := range files {
		log.Printf("read File: %s\n", fileName)
		csvFile, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return err
		}

		defer csvFile.Close()

		var data []*CsvLine

		if err := gocsv.UnmarshalFile(csvFile, &data); err != nil { // Load clients from file
			return err
		}

		if len(data) == 0 {
			continue
		}

		pts := make([]client.Point, 0)

		for i := range data {
			var hh, mm, ss, yyyy, mo, dd int

			fmt.Sscanf(data[i].Datum, "%d.%d.%d", &dd, &mo, &yyyy)
			fmt.Sscanf(data[i].Zeit, "%d:%d:%d", &hh, &mm, &ss)
			yyyy +=2000
			t := time.Date(yyyy, time.Month(mo), dd, hh, mm, ss, 0, time.Local)

			p := client.Point{
				Measurement: "solar",
				Time:        t,
				Fields: map[string]interface{}{
					"Tcol": float32(string2float(data[i].Temp1)),
					"Tboil": float32(string2float(data[i].Temp2)),
					"Trl":   float32(string2float(data[i].Temp3)),
					"Tvl":   float32(string2float(data[i].Temp4)),
					"Psol":  int(string2int(data[i].Ausg1)),
					"Phe":   int(string2int(data[i].Ausg2)),
				},
				Precision: "",
				Raw:       "",
			}

			pts = append(pts, p)
		}

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
			_, err = ic.Write(bps)
			if err != nil {
				log.Println("Insert data error:")
				log.Fatal(err)
			}
		}
	}

	return nil
}

func string2float(s string) (f float64) {
	f, _ = strconv.ParseFloat(strings.ReplaceAll(s, ",", "."), 32)
	return
}

func string2int(s string) (i int64) {
	i, _ = strconv.ParseInt(s, 0, 8)
	return
}
