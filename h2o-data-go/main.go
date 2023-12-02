package main

import (
	"encoding/csv"
	"log"
	"math/rand"
	"os"
	"strconv"
)
type GroupBy struct {
    id1 int
    id2 int
}
func main() {
  
	n_rows := 1000

	val_id1 := rand.Perm(n_rows)
	val_id2 := rand.Perm(n_rows)
	
	records := []GroupBy{}
	for i := 0; i < n_rows; i++ {
		records = append(records, GroupBy{
			id1: val_id1[i],
			id2: val_id2[i],
		})


    file, err := os.Create("records.csv")
    if err != nil {
        log.Fatalln("failed to open file", err)
		defer file.Close()
    }
	
    w := csv.NewWriter(file)
	err = w.Write([]string{"id1", "id2"})
	if err != nil {
		log.Fatalln("error writing record to file", err)
	}

    defer w.Flush()
    for _, record := range records {
        row := []string{strconv.Itoa(record.id1), strconv.Itoa(record.id2)}
        if err := w.Write(row); err != nil {
            log.Fatalln("error writing record to file", err)
        }
    }   
}
}