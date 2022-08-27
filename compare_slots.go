package main

import (
	"context"
	"flag"
	"github.com/portto/solana-go-sdk/client"
	"github.com/portto/solana-go-sdk/rpc"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

var Slot uint64

func InitFlags() {
	flag.Uint64Var(&Slot, "s", 0, "slot")
	flag.Parse()
}

func CleanStringSlice(s []string) (x []string) {
	for _, p := range s {
		p = strings.TrimSpace(p)
		if len(p) > 0 {
			x = append(x, p)
		}
	}

	return x
}

func GetPrivateEndpoints() []string {
	endpoints := os.Getenv("PRIVATE_ENDPOINTS")
	if endpoints == "" {
		log.Fatalf("PRIVATE_ENDPOINTS must be set with a `;` seperated list of endpoints.\n")
	}

	return CleanStringSlice(strings.Split(endpoints, ";"))
}

func GetBlock(endpoint string, slot uint64) (block client.GetBlockResponse, err error) {
	c := client.NewClient(endpoint)

	block, err = c.GetBlock(context.TODO(), slot)

	log.Printf("GetBlock(%v): %v, err %+v\n", endpoint, block, err)

	if err != nil {
		log.Printf("GetBlock(%v): failed to get block, err: %v\n", endpoint, err)
		return block, err
	} else {
		return block, nil
	}
}

func GetBlockHeight(endpoint string) (block rpc.JsonRpcResponse[uint64], err error) {
	c := client.NewClient(endpoint)

	block, err = c.RpcClient.GetBlockHeight(context.TODO())

	log.Printf("GetBlockHeight(%v): %v, err %+v\n", endpoint, block, err)

	if err != nil {
		log.Printf("GetBlockHeight(%v): failed to get block, err: %v\n", endpoint, err)
		return block, err
	} else {
		return block, nil
	}
}

func GetSlot(endpoint string) (slot rpc.JsonRpcResponse[uint64], err error) {
	c := client.NewClient(endpoint)

	slot, err = c.RpcClient.GetSlot(context.TODO())

	log.Printf("GetSlot(%v): %v, err %+v\n", endpoint, slot, err)

	if err != nil {
		log.Printf("GetSlot(%v): failed to get block, err: %v\n", endpoint, err)
		return slot, err
	} else {
		return slot, nil
	}
}

type SlotConsensus struct {
	Slot uint64
	N    int
	T    int
}

func GetSlotByConsensus() (slotConsensus []SlotConsensus) {
	endpoints := GetPrivateEndpoints()
	var wg sync.WaitGroup
	var results sync.Map
	for _, endpoint := range endpoints {
		wg.Add(1)

		go func(endpoint string) {
			defer wg.Done()
			slot, err := GetSlot(endpoint)
			if err == nil {
				results.Store(endpoint, slot.Result)
			}
		}(endpoint)
	}
	wg.Wait()

	slotByConsensus := make(map[uint64]SlotConsensus)
	results.Range(func(key, value any) bool {
		slot := value.(uint64)
		s, ok := slotByConsensus[slot]
		if !ok {
			s = SlotConsensus{Slot: slot, T: len(endpoints)}
		}
		s.N++
		slotByConsensus[slot] = s
		return true
	})

	for _, v := range slotByConsensus {
		slotConsensus = append(slotConsensus, v)
	}

	sort.SliceStable(slotConsensus, func(i, j int) bool {
		return slotConsensus[i].N < slotConsensus[j].N
	})

	return
}

func main() {
	InitFlags()

	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Starting up\n")

	log.Printf("%+v\n", GetSlotByConsensus())
}
