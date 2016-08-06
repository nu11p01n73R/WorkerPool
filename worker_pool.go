package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func worker(id int, jobs <-chan string, result chan<- bool) {
	for body := range jobs {
		fmt.Println(id, "sending data", body)
		jsonString := []byte(body)
		res, err := http.Post("http://127.0.0.1:9200/index/type", "application/json", bytes.NewBuffer(jsonString))

		if err != nil {
			fmt.Println("Post Request failed")
		}

		_, err = ioutil.ReadAll(res.Body)

		if err != nil {
			fmt.Println("Unable to read response body")
		}

		defer res.Body.Close()
	}
	result <- true
}

func main() {
	file, err := os.Open("input_file")

	if err != nil {
		fmt.Println("Unable to open file")
	}

	scanner := bufio.NewScanner(file)
	defer file.Close()

	var jsonString string

	jobs := make(chan string, 100)
	result := make(chan bool, 100)

	for i := 0; i < 50; i++ {
		go worker(i, jobs, result)
	}

	for scanner.Scan() {
		slices := strings.Split(scanner.Text(), "  ")
		jsonString = fmt.Sprintf(`{"url" : "%s", "time" : %s}`, slices[0], strings.TrimSpace(slices[1]))

		jobs <- jsonString
	}
	close(jobs)

	for r := 0; r < 50; r++ {
		<-result
	}
}
