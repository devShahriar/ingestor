package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/devShahriar/ingestor/pg"
)

func ReadFile(filePath string) []pg.Data {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return []pg.Data{}
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	scanner := bufio.NewScanner(file)

	var DataList []pg.Data = make([]pg.Data, 0)

	// Iterate over each line in the file
	for scanner.Scan() {

		// Parse each line as JSON
		var data pg.Data
		line := scanner.Bytes()
		err := json.Unmarshal(line, &data)
		if err != nil {
			fmt.Println("Error parsing JSON:", err)
			continue
		}
		DataList = append(DataList, data)

	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	return DataList
}

func main() {
	// Open the JSONL file
	inputFilePath := os.Args[1]
	dataList := ReadFile(inputFilePath)

	pgConn := pg.Pg{
		Host:     "10.140.0.63",
		Port:     5439,
		DbName:   "alexander",
		UserName: "alexander",
		Password: "Buceph@lus~de",
	}

	err := pgConn.DumpIntoPostgres(dataList)
	if err != nil {
		fmt.Println(err)
	}
}
