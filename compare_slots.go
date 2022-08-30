package main

import (
	"bytes"
	"context"
	"flag"
	"github.com/portto/solana-go-sdk/client"
	"github.com/portto/solana-go-sdk/rpc"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
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

	log.Printf("GetBlock(%v): %v, err %+v\n", endpoint, slot, err)

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

type CurrentSlotConsensus struct {
	Slot uint64
	N    int
	T    int
}

func GetCurrentSlotByConsensus() (slotConsensus []CurrentSlotConsensus) {
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

	slotByConsensus := make(map[uint64]CurrentSlotConsensus)
	results.Range(func(key, value any) bool {
		slot := value.(uint64)
		s, ok := slotByConsensus[slot]
		if !ok {
			s = CurrentSlotConsensus{Slot: slot, T: len(endpoints)}
		}
		s.N++
		slotByConsensus[slot] = s
		return true
	})

	for _, v := range slotByConsensus {
		slotConsensus = append(slotConsensus, v)
	}

	sort.SliceStable(slotConsensus, func(i, j int) bool {
		return slotConsensus[i].N > slotConsensus[j].N
	})

	return
}

type SlotConsensus struct {
	BlockData map[string]client.GetBlockResponse
	N         int
	T         int
}

func GetSlotByConsensus(slot uint64) (slotConsensus []SlotConsensus) {
	endpoints := GetPrivateEndpoints()
	var wg sync.WaitGroup
	var results sync.Map
	for _, endpoint := range endpoints {
		wg.Add(1)

		go func(endpoint string) {
			defer wg.Done()
			blockResp, err := GetBlock(endpoint, slot)
			if err == nil {
				results.Store(endpoint, blockResp)
			}
		}(endpoint)
	}
	wg.Wait()

	slotByConsensus := make(map[string]SlotConsensus)
	results.Range(func(key, value any) bool {
		blockResp := value.(client.GetBlockResponse)
		s, ok := slotByConsensus[blockResp.Blockhash]
		if !ok {
			s = SlotConsensus{BlockData: make(map[string]client.GetBlockResponse), T: len(endpoints)}
		}
		s.BlockData[key.(string)] = blockResp
		s.N++
		slotByConsensus[blockResp.Blockhash] = s
		return true
	})

	for _, v := range slotByConsensus {
		slotConsensus = append(slotConsensus, v)
	}

	sort.SliceStable(slotConsensus, func(i, j int) bool {
		return slotConsensus[i].N > slotConsensus[j].N
	})

	return
}

func main() {
	InitFlags()

	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Starting up\n")

	currentSlot := GetCurrentSlotByConsensus()
	log.Printf("Current slots: %+v\n", currentSlot)

	if len(currentSlot) > 0 {
		highSlot := currentSlot[0].Slot
		if Slot > 0 {
			highSlot = Slot
			log.Printf("Using specified slot: %v\n", highSlot)
		} else {
			log.Printf("Using quorum slot: %v\n", highSlot)
		}
		currentSlotData := GetSlotByConsensus(highSlot)
		log.Printf("%v data version(s) for slot %v\n", len(currentSlotData), highSlot)
		for _, v := range currentSlotData {
			var firstResp *client.GetBlockResponse
			for kk, vv := range v.BlockData {
				if firstResp == nil {
					firstResp = &vv
				}

				slotContentsMatch := CompareSlotContent(vv, *firstResp)
				blockTime := time.Unix(*vv.BlockTime, 0)
				log.Printf("Endpoint %v had blockhash %v (blockTime %v) for slot %v, content match: %v\n",
					kk, vv.Blockhash, blockTime, highSlot, slotContentsMatch)

			}
		}
	}
}

/*
	Blockhash         string
	BlockTime         *int64
	BlockHeight       *int64
	PreviousBlockhash string
	ParentSLot        uint64
	Transactions      []GetBlockTransaction
	Rewards           []rpc.GetBlockReward

*/

func CompareTransactions(j []client.GetBlockTransaction, k []client.GetBlockTransaction) bool {
	if len(j) != len(k) {
		return false
	}

	for i := 0; i < len(j); i++ {
		if j[i].Meta != k[i].Meta {
			return false
		}

		jT, _ := j[i].Transaction.Serialize()
		kT, _ := k[i].Transaction.Serialize()

		if !bytes.Equal(jT, kT) {
			return false
		}
	}

	return true
}

func CompareRewards(j []rpc.GetBlockReward, k []rpc.GetBlockReward) bool {
	if len(j) != len(k) {
		return false
	}

	for i := 0; i < len(j); i++ {
		if j[i] != k[i] {
			return false
		}
	}

	return true
}

func CompareSlotContent(j client.GetBlockResponse, k client.GetBlockResponse) bool {
	return j.Blockhash == k.Blockhash &&
		j.BlockTime == k.BlockTime &&
		j.BlockHeight == k.BlockHeight &&
		j.PreviousBlockhash == k.PreviousBlockhash &&
		j.ParentSLot == k.ParentSLot &&
		CompareTransactions(j.Transactions, k.Transactions) &&
		CompareRewards(j.Rewards, k.Rewards)
}
