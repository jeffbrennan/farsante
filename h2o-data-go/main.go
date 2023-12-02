package main

import (
	"encoding/csv"
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/schollz/progressbar/v3"
)

type GroupBy struct {
    id1 int
    id2 int
}

func main() {
  	const n_rows int = 100_000_000
	records := []GroupBy{}
	for i := 0; i < n_rows; i++ {
		records = append(records, GroupBy{rand.Intn(100), rand.Intn(100)})
	}
	
    file, err := os.Create("records.csv")
    if err != nil {
        log.Fatalln("failed to open file", err)
		defer file.Close()
    }

	bar := progressbar.Default(int64(n_rows))
    w := csv.NewWriter(file)
	err = w.Write([]string{"id1", "id2"})
	if err != nil {
		log.Fatalln("error writing record to file", err)
		defer w.Flush()
	}

	for _, record := range records {
		bar.Add(1)
		row := []string{strconv.Itoa(record.id1), strconv.Itoa(record.id2)}		
		w.Write(row)
    }   
}