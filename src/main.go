package main

import (
	"fd-raft/fdraft"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
	// "os"
)

func main() {
	var wg deadlock.WaitGroup

	var servers [5]*fdraft.Server
	for i := 0; i < 5; i++ {
		wg.Add(1)
		servers[i] = fdraft.MakeServer(i, &wg)
	}
	fmt.Println("Created servers")
	time.Sleep(3 * time.Second)
	leader := -1

	for leader == -1 {
		for j := 0; j < 5; j++ {
			fmt.Println("Leader Id", servers[j].Proxy.GetLeaderId())
			if servers[j].Proxy.GetLeaderId() != -1 {
				leader = servers[j].Proxy.GetLeaderId()
			}
		}
	}
	var ck [5]*fdraft.Clerk
	for i := 0; i < len(ck); i++ {
		ck[i] = fdraft.MakeClerk()

	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("Key: %d", i)
		value := fmt.Sprintf("Value: %d", i)
		ck[0].Put(key, value)
	}

	// TODO: Create multiple go routines
	// add the routine to wait group
	// wait until all threads complete.
	var clientWg deadlock.WaitGroup
	start := time.Now().UnixNano()
	var counter atomic.Int64
	counter.Store(0)
	for i := 0; i < len(ck); i++ {
		clientWg.Add(1)
		go func(client *fdraft.Clerk, index int) {
			defer clientWg.Done()
			for j := 0; j < 5000; j++ {
				key := fmt.Sprintf("%d_Key: %d", index, j)
				value := fmt.Sprintf("%d_Value: %d", index, j)
				client.Put(key, value)
				counter.Add(1)
			}

		}(ck[i], i)
	}

	time.Sleep(2 * time.Second)
	fmt.Println("Killing leader")
	servers[leader].Kill()
	clientWg.Wait()
	end := time.Now().UnixNano()

	// for i := 0; i < 5; i++ {
	// 	fmt.Println("\nLogs of server: ", i)
	// 	servers[i].Proxy.PrintRfLogs()
	// }

	// fmt.Println("Single clint 1000 operations: ", (end - start))
	fmt.Printf("%d Clients completed %d operations in %d ns", 5, counter.Load(), end-start)
	key := fmt.Sprintf("0_Key: %d", 9999)
	fmt.Println("Key 9999: ", ck[0].Get(key))

	wg.Wait()
}
