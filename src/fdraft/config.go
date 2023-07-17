package fdraft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	// "io/ioutil"
	"os"
)

type Config struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	Id      int    `json:"id"`
	Address string `json:"address"`
	Zone    int    `json:"zone"`
}


func (config Config) GetAtIndex(index int) (Node, error){
	if (index > len(config.Nodes)) {
		
		return Node{}, errors.New("index > len")
	}
	if index < 0 {
		return Node{}, errors.New("index < 0")
	}
	return config.Nodes[index], nil
}


 func BuildConfig() Config {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fd, err := os.Open(fmt.Sprintf("%s/files/config.json", path))
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	byteContent, err := io.ReadAll(fd)
	// byteContent, err := io.ReadAll(fd)
	if err != nil {
		panic(err)
	}
	// unmarshal the config.
	config := new(Config)
	ok := json.Unmarshal(byteContent, config)
	if ok != nil {	
		fmt.Println(config)	
		fmt.Println(ok)
		if e, ok := ok.(*json.SyntaxError); ok {
			fmt.Println("syntax error at byte offset", e.Offset)
		}
		panic(ok)
	}
	return *config
}