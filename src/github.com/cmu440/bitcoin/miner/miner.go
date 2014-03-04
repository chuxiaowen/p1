package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	// TODO: implement this!
	// Start a bitcoin Miner
	hostport := os.Args[1]
	params := lsp.NewParams()
	miner, err := lsp.NewClient(hostport, params)
	if err != nil {
		printDisconnected()
		return
	}

	// Send Join request to server
	join := bitcoin.NewJoin()
	msg, err := json.Marshal(join)
	if err != nil {
		fmt.Println("failed to marshal!")
		return
	}
	err = miner.Write(msg)
	if err != nil {
		printDisconnected()
		return
	}

	// Read password request from server
	for {
		payload, err := miner.Read()
		if err != nil {
			printDisconnected()
			break
		}

		var req bitcoin.Message
		err = json.Unmarshal(payload, &req)
		if err != nil {
			fmt.Println("Unmarshal failed!")
			break
		}

		// Compute hash values
		message := req.Data
		minvalue := req.Lower
		maxvalue := req.Upper

		leasthvalue := bitcoin.Hash(message, minvalue)
		index := minvalue

		for i := minvalue + 1; i <= maxvalue; i++ {
			hashvalue := bitcoin.Hash(message, i)
			if hashvalue < leasthvalue {
				leasthvalue = hashvalue
				index = i
			}
		}

		// Send result to Server
		result := bitcoin.NewResult(leasthvalue, index)
		msg, err := json.Marshal(result)
		if err != nil {
			fmt.Println("marshal failed!")
			break
		}

		err = miner.Write(msg)
		if err != nil {
			printDisconnected()
			break
		}
	}

	miner.Close()
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
