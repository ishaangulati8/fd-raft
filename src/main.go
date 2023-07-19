package main

import (
	"fd-raft/fdraft"
	"fmt"
	// "sync/atomic"
	"time"

	"os"
	"strconv"

	"github.com/sasha-s/go-deadlock"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage prog-binary type")
		return
	}
	runType, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Type must be either 1(server) or 2(client)")
		return
	}
	if runType > 2 || runType < 1 {
		fmt.Println("Type must be either 1(server) or 2(client)")
		return
	}

	if runType == 1 {
		if len(os.Args) != 5{
			fmt.Println("Missing or extra arguments")
			return
		}
		nodeCount, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("Node count must be an integer and with value 5, 7, or 9.")
			return
		}
		if nodeCount != 5 && nodeCount != 7 && nodeCount != 9 {
			fmt.Println("Node count must be either 5, 7, or 9.")
			return
		}

		id, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Println("Id must be an integer")
			return
		}

		batching, err := strconv.Atoi(os.Args[4])
		if err != nil || (batching < 0 || batching > 1 ) {
			fmt.Println("Batching must be either 0 or 1 ", batching)
			return
		}

		// run the node
		// var server *fdraft.Server
		var wg deadlock.WaitGroup
		wg.Add(1)
		fdraft.MakeServer(id, batching, nodeCount, &wg)
		wg.Wait()
	} else {
		// client region
		if len(os.Args) != 5 {
			fmt.Println("Missing node count, thread count, and operations count")
			return
		}
		nodeCount, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("Node count must be an integer and with value 5, 7, or 9.")
			return
		}
		if nodeCount != 5 && nodeCount != 7 && nodeCount != 9 {
			fmt.Println("Node count must be either 5, 7, or 9.")
			return
		}

		threadCount, err := strconv.Atoi(os.Args[3])
		if err != nil || threadCount < 1 {
			fmt.Println("Thread count must be a number greater than 0")
			return
		}
		numberOfOperations, err := strconv.Atoi(os.Args[4])
		if err != nil || numberOfOperations < threadCount {
			fmt.Println("Number of operations should be a number greater than thread count")
			return
		}

		var wg deadlock.WaitGroup
		var ck []*fdraft.Clerk
		for i := 0; i < threadCount; i++ {
			ck = append(ck, fdraft.MakeClerk(nodeCount))
		}

		// warm-up of clients and consensus module.
		fmt.Println("Starting warm up")
		for i := 0; i < threadCount; i++ {
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("%d_%d_Test_Key", i, j)
				value := fmt.Sprintf("%d_%dTest_Value", i, j)
				ck[i].Put(key, value)
			}
		}

		time.Sleep(4 * time.Second)

		operationsPerThread := int(numberOfOperations / threadCount)
		startTime := time.Now().UnixNano()
		for i := 0; i < len(ck); i++ {
			wg.Add(1)
			go func(count int, client *fdraft.Clerk, wg *deadlock.WaitGroup) {
				defer wg.Done()
				lastKey := ""
				for j := 0; j < count; j++ {
					if j%2 == 1 {
						// even put
						key := fmt.Sprintf("Key: %d", j)
						value := fmt.Sprintf("Value: %d", j)
						client.Put(key, value)
						lastKey = key
					} else {
						// odd get
						client.Get(lastKey)

					}
				}
			}(operationsPerThread, ck[i], &wg)
		}

		wg.Wait()
		endTime := time.Now().UnixNano()

		fmt.Printf("Took %d ns to complete %d operations", (endTime - startTime), numberOfOperations)

	}

}



// var wg deadlock.WaitGroup

	// var servers [5]*fdraft.Server
	// for i := 0; i < 5; i++ {
	// 	wg.Add(1)
	// 	servers[i] = fdraft.MakeServer(i, 0, &wg)
	// }
	// fmt.Println("Created servers")
	// time.Sleep(3 * time.Second)
	// leader := -1

	// for leader == -1 {
	// 	for j := 0; j < 5; j++ {
	// 		fmt.Println("Leader Id", servers[j].Proxy.GetLeaderId())
	// 		if servers[j].Proxy.GetLeaderId() != -1 {
	// 			leader = servers[j].Proxy.GetLeaderId()
	// 		}
	// 	}
	// }
	// var ck [5]*fdraft.Clerk
	// for i := 0; i < len(ck); i++ {
	// 	ck[i] = fdraft.MakeClerk(0)

	// }

	// for i := 0; i < 100; i++ {
	// 	key := fmt.Sprintf("Key: %d", i)
	// 	value := fmt.Sprintf("Value: %d", i)
	// 	ck[0].Put(key, value)
	// }

	// // TODO: Create multiple go routines
	// // add the routine to wait group
	// // wait until all threads complete.
	// var clientWg deadlock.WaitGroup
	// start := time.Now().UnixNano()
	// var counter atomic.Int64
	// counter.Store(0)
	// for i := 0; i < len(ck); i++ {
	// 	clientWg.Add(1)
	// 	go func(client *fdraft.Clerk, index int) {
	// 		defer clientWg.Done()
	// 		for j := 0; j < 3500; j++ {
	// 			key := fmt.Sprintf("%d_Key: %d", index, j)
	// 			value := fmt.Sprintf("%d_Value: %d", index, j)
	// 			client.Put(key, value)
	// 			counter.Add(1)
	// 		}

	// 	}(ck[i], i)
	// }

	// time.Sleep(7 * time.Second)
	// fmt.Println("Killing leader")
	// servers[leader].Kill()
	// clientWg.Wait()
	// end := time.Now().UnixNano()

	// // for i := 0; i < 5; i++ {
	// // 	fmt.Println("\nLogs of server: ", i)
	// // 	servers[i].Proxy.PrintRfLogs()
	// // }

	// // fmt.Println("Single clint 1000 operations: ", (end - start))
	// fmt.Printf("%d Clients completed %d operations in %d ns", 5, counter.Load(), end-start)
	// key := fmt.Sprintf("0_Key: %d", 9999)
	// fmt.Println("Key 9999: ", ck[0].Get(key))

	// wg.Wait()
