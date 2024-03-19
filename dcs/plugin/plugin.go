package main

import (
	"dcs/job"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
)

type word_count struct{}

type log_analyzer_count struct{}

type log_analyzer_unique struct{}

func (w word_count) Execute(line_num int, line_txt string) ([]byte, []byte) {
	line_num_b := make([]byte, 8)
	binary.LittleEndian.PutUint64(line_num_b, uint64(line_num))
	word_count := make([]byte, 8)
	binary.LittleEndian.PutUint64(word_count, uint64(len(strings.Fields(line_txt))))
	return line_num_b, word_count
}

func (l log_analyzer_count) Execute(line_num int, line_txt string) ([]byte, []byte) {
	place_holder := make([]byte, 8)
	binary.LittleEndian.PutUint64(place_holder, uint64(1))
	words := strings.Fields(line_txt)
	if len(words) > 0 {
		raw_url := words[len(words)-1]
		url, err := url.Parse(raw_url)
		if err != nil {
			return []byte("ERROR_IGNORE"), place_holder
		}
		return []byte(url.Hostname()), place_holder
	} else {
		return []byte("ERROR_IGNORE"), place_holder
	}
}

func (l log_analyzer_unique) Execute(line_num int, line_txt string) ([]byte, []byte) {
	place_holder := make([]byte, 8)
	binary.LittleEndian.PutUint64(place_holder, uint64(1))
	words := strings.Fields(line_txt)
	if len(words) > 0 {
		raw_url := words[len(words)-1]
		url, err := url.Parse(raw_url)
		if err != nil {
			return []byte("ERROR_IGNORE"), place_holder
		}
		return []byte(url.Hostname()), place_holder
	} else {
		return []byte("ERROR_IGNORE"), place_holder
	}
}

func (w word_count) Reduce(key []byte, values [][]byte) ([]byte, []byte) {
	var count uint64 = 0
	for i := 0; i < len(values); i++ {
		count += binary.LittleEndian.Uint64(values[i])
	}
	count_b := make([]byte, 8)
	binary.LittleEndian.PutUint64(count_b, uint64(count))
	return key, count_b
}

func (l log_analyzer_count) Reduce(key []byte, values [][]byte) ([]byte, []byte) {
	count_b := make([]byte, 8)
	binary.LittleEndian.PutUint64(count_b, uint64(len(values)))
	return key, count_b
}

func (l log_analyzer_unique) Reduce(key []byte, values [][]byte) ([]byte, []byte) {
	count_b := make([]byte, 8)
	binary.LittleEndian.PutUint64(count_b, uint64(1))
	return key, count_b
}

func (w word_count) Results(key []byte, value []byte) string {
	return fmt.Sprintf("%d:\t%d\n", binary.LittleEndian.Uint64(key), binary.LittleEndian.Uint64(value))
}

func (l log_analyzer_count) Results(key []byte, value []byte) string {
	key_str := string(key)
	if key_str == "ERROR_IGNORE" {
		return ""
	}
	return fmt.Sprintf("%s:\t%d\n", key, binary.LittleEndian.Uint64(value))
}

func (l log_analyzer_unique) Results(key []byte, value []byte) string {
	return fmt.Sprintf("%s:\t%d\n", key, binary.LittleEndian.Uint64(value))
}

func WordCount() job.Executer {
	return &word_count{}
}

func LogAnalyzerCount() job.Executer {
	return &log_analyzer_count{}
}

func LogAnalyzerUnique() job.Executer {
	return &log_analyzer_unique{}
}
