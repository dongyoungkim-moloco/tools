package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

func recurse(value interface{}) interface{} {
	switch vv := value.(type) {
	case string:
		var obj interface{}
		err := json.Unmarshal([]byte(vv), &obj)
		if err != nil {
			log.Fatal(err)
		}
		// target was not a raw json message
		if _, ok := obj.(string); ok {
			return obj
		}
		// target was a raw json message and obj now holds a container
		return recurse(obj)
	case map[string]interface{}:
		for k, v := range vv {
			vv[k] = recurse(v)
		}
		return value
	case []interface{}:
		for k, v := range vv {
			vv[k] = recurse(v)
		}
		return value
	}
	return value
}

func do(reader io.Reader) {
	dec := json.NewDecoder(reader)
	var obj interface{}
	err := dec.Decode(&obj)
	if err != nil {
		log.Fatalf("%v", err)
	}
	obj = recurse(obj)
	ret, _ := json.MarshalIndent(obj, "", "  ")
	fmt.Println(string(ret))
}

func main() {
	do(os.Stdin)
}
