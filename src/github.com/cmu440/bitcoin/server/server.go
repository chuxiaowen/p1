package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"math"
	"os"
	"strconv"
)

type signalChan chan struct{}

type client struct {
	connId         int
	leastHashvalue uint64
	nonce          uint64
	count          int
	active         int
}

type miner struct {
	connId         int
	worksforClient int
	job            *request
}

type request struct {
	clientid int
	msg      *bitcoin.Message
}

var clientList map[int]*client
var minerList map[int]*miner
var reqBuffer *list.List
var freeMinerList *list.List
var server lsp.Server
var schedulerChan signalChan

const minerLoad = 10000

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	// TODO: implement this!

	// Start a server
	port, _ := strconv.Atoi(os.Args[1])
	params := lsp.NewParams()

	server, _ = lsp.NewServer(port, params)

	// Make client and miner list
	clientList = make(map[int]*client)
	minerList = make(map[int]*miner)
	reqBuffer = list.New()
	freeMinerList = list.New()
	schedulerChan = make(signalChan)

	go handleScheduler()

	// handle network messages
	for {
		id, payload, err := server.Read()

		// First check if server has been shutdown
		if err != nil {

			// check the reason for error
			if id == 0 {
				fmt.Println("server shutdown!")
				return
			}

			if clientList[id] != nil {

				// remove client
				fmt.Println("client closed!")
				delete(clientList, id)
				server.CloseConn(id)
				continue
			}

			if minerList[id] != nil {

				// remove miner
				fmt.Println("miner closed!")
				fmt.Println(id)
				tempminer := minerList[id]
				delete(minerList, id)

				server.CloseConn(id)

				// reassign the task
				if tempminer.job != nil {
					fmt.Println("reassign job")
					reqBuffer.PushBack(tempminer.job)
					schedulerChan <- struct{}{}
				}
				continue
			}
			continue
		}

		var msg bitcoin.Message
		err = json.Unmarshal(payload, &msg)
		if err != nil {
			fmt.Println("Unmarshal error!")
			break
		}

		// check the type of messages
		if msg.Type == bitcoin.Join && minerList[id] == nil {

			// add a new miner to minerList
			newMiner := &miner{id, 0, nil}
			minerList[id] = newMiner
			freeMinerList.PushBack(id)
			schedulerChan <- struct{}{}
		}

		if msg.Type == bitcoin.Request {

			// Handle request from server
			clientList[id] = &client{id, 0, 0, 0, 0}
			handleClientRequest(id, &msg)
			schedulerChan <- struct{}{}
		}

		if msg.Type == bitcoin.Result {

			miner := minerList[id]
			clientid := miner.worksforClient

			freeMinerList.PushBack(id)
			schedulerChan <- struct{}{}

			client := clientList[clientid]
			if client == nil {
				continue
			}

			if client.active == client.count {
				client.leastHashvalue = msg.Hash
				client.nonce = msg.Nonce
			} else {
				if msg.Hash < client.leastHashvalue {
					client.leastHashvalue = msg.Hash
					client.nonce = msg.Nonce
				}
			}
			client.active--

			// Check whether need to send back to client
			if client.active == 0 {
				result := bitcoin.NewResult(client.leastHashvalue, client.nonce)
				payload, _ := json.Marshal(result)
				server.Write(clientid, payload)

			}

		}
	}

}

func handleScheduler() {
	for {
		<-schedulerChan
		success := true

		e := reqBuffer.Front()
		if e == nil {
			continue
		}

		freeminer := freeMinerList.Front()
		if freeminer == nil {
			continue
		} else {
			id := freeminer.Value.(int)

			// remove dead miners
			for minerList[id] == nil {
				freeMinerList.Remove(freeminer)
				fmt.Println("remove the dead miner", id)
				freeminer = freeMinerList.Front()

				if freeminer == nil {
					success = false
					break
				}

				id = freeminer.Value.(int)
				_ = minerList[id]
			}
		}

		if success == false {
			continue
		}

		// assign each miner with a new job
		minerId := freeminer.Value.(int)
		miner := minerList[minerId]

		freeMinerList.Remove(freeminer)
		reqBuffer.Remove(e)
		req := e.Value.(*request)

		miner.worksforClient = req.clientid
		miner.job = req

		payload, err := json.Marshal(req.msg)
		if err != nil {
			fmt.Println("marshal error!")
			continue
		}

		// write to the miner
		err = server.Write(minerId, payload)
		if err != nil {
			fmt.Println("connection to miner has lost!")
			server.CloseConn(minerId)
		}
	}
}

func handleClientRequest(connid int, msg *bitcoin.Message) {

	message := msg.Data
	upper := msg.Upper

	client := clientList[connid]

	if upper < minerLoad {
		// assign it to one miner
		req := &request{connid, msg}
		reqBuffer.PushBack(req)

		client.count = 1
		client.active = 1
		return
	}

	// divide the assignment into equal parts
	number := int(math.Ceil(float64(upper) / minerLoad))
	fmt.Println(number)
	increment := int(math.Ceil(float64(upper) / float64(number)))
	fmt.Println(increment)

	var i int
	for i = 0; i < int(upper)-increment; i = i + increment {
		fmt.Println(i + increment)
		reqmsg := bitcoin.NewRequest(message, uint64(i), uint64(i+increment))
		newreq := &request{connid, reqmsg}
		reqBuffer.PushBack(newreq)
	}

	fmt.Println(i)
	reqmsg := bitcoin.NewRequest(message, uint64(i), upper)
	newreq := &request{connid, reqmsg}
	reqBuffer.PushBack(newreq)

	client.count = number
	client.active = number

}
