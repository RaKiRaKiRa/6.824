/*
 * @ Author        : RaKiRaKiRa
 * @ Email         : 763600693@qq.com
 * @ Create time   : 2019-10-14 13:52
 * @ Last modified : 2019-10-14 15:17
 * @ Filename      : 
 * @ Description   : 
 */
package mapreduce
import (
	"io"
	"os"
	"encoding/json"
	"log"
	"sort"
)
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	inputFile := make([] *os.File, nMap)
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTask)
		inputFile[i],_ = os.Open(filename)
	}
	
	kvs := make(map[string] []string)

	for _, file := range inputFile{
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil{
				if err == io.EOF{
					break
				}
				log.Fatal(err)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		} 
	}

	var keys []string
	for k := range kvs{
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out,_ := os.Create(outFile)
	enc := json.NewEncoder(out)
	for _,key := range keys {
		err := enc.Encode(KeyValue{key, reduceF(key, kvs[key])})
		if err != nil {
			log.Fatal(err)
		}
	}
	out.Close()

}
