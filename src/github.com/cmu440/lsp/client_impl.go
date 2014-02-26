// Contains the implementation of a LSP client.

package lsp

import "errors"

import (
	"container/list"
	"encoding/json"
	"github.com/cmu440/lspnet"
	"time"
	//"fmt"
)

type signalChan chan struct{}

type client struct {
	// TODO: implement this!
	conn    *lspnet.UDPConn //connection
	connid  int             //connection id
	connAdr *lspnet.UDPAddr // connection address

	receiveseqid int
	writeseqid   int // write sequence id
	epochtimes   int // record epoch times
	lastepoch    int // record last epoch time which receives data

	parameter *Params // parameters

	shutdown     signalChan    // Judge whether connection is closed
	epochChan    signalChan    // Triger epochs
	appcloseChan signalChan    // used for application to call closed
	receiveChan  chan *Message // Inputs from the network
	readChan     chan *Message // Supply results for Read() function
	writeChan    chan *Message // Requests to write
	connectChan  chan *Message // connection chan

	closeReplyChan chan error // get result for close
	readReplyChan  chan error
	writeReplyChan chan error // get result for write

	readList  *list.List // Buffer for data read from network
	writeList *list.List // Buffer for data write to network

	ackSlidingWindow     []*Message
	writeSlidingWindow   []*Message // sliding window for sending messages
	writeWindowIndicator []bool     // sliding window to indicate for sending messages
	ackWindowIndicator   []bool     // sliding window for ack messages
	writePendingNum      int        // Frist message in write sliding window
	ackPendingNum        int        // First message in ack sliding window

	connectPending bool // indicate whether connection pending

	networkStopFlag bool
	appStopFlag     bool
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	// Create a new client
	//fmt.Println("create a new client")
	connid := 0

	receiveseqid := 0
	writeseqid := 1
	epochtimes := 0
	lastepoch := 0

	shutdown := make(signalChan, 1)
	epochChan := make(signalChan, 1)
	appcloseChan := make(signalChan, 1)

	receiveChan := make(chan *Message, 1)
	readChan := make(chan *Message, 1)
	writeChan := make(chan *Message, 1)
	connectChan := make(chan *Message, 1)

	closeReplyChan := make(chan error, 1)
	readReplyChan := make(chan error, 1)
	writeReplyChan := make(chan error, 1)

	readList := list.New()
	writeList := list.New()

	ackSlidingWindow := make([]*Message, params.WindowSize)
	writeSlidingWindow := make([]*Message, params.WindowSize)
	writeWindowIndicator := make([]bool, params.WindowSize)
	ackWindowIndicator := make([]bool, params.WindowSize)
	writePendingNum := 0
	ackPendingNum := 0

	connectPending := true

	networkStopFlag := false
	appStopFlag := false

	raddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	conn, err := lspnet.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}

	Client := &client{conn, connid, raddr, receiveseqid, writeseqid, epochtimes, lastepoch, params,
		shutdown, epochChan, appcloseChan, receiveChan, readChan, writeChan, connectChan, closeReplyChan,
		readReplyChan, writeReplyChan, readList, writeList, ackSlidingWindow,
		writeSlidingWindow, writeWindowIndicator,
		ackWindowIndicator, writePendingNum, ackPendingNum, connectPending, networkStopFlag, appStopFlag}

	// send connection request to server
	go ClientnetworkHandler(Client)
	go ClientepochHandler(Client)
	go ClientmasterEvenHandler(Client)

	connMsg := NewConnect()
	connMsg.ConnID = 0
	connMsg.SeqNum = 0

	p, err := json.Marshal(connMsg)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write(p)
	if err != nil {
		return nil, err
	}

	val := <-connectChan

	if val != nil {
		Client.connid = val.ConnID
		return Client, nil
	} else {
		return nil, errors.New("failed to connect")
	}
}

func (c *client) ConnID() int {
	return c.connid
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	val := <-c.readChan

	if val != nil {
		return val.Payload, nil
	} else {
		c.readChan <- nil
		return nil, errors.New("connection lost!")
	}
}

func (c *client) Write(payload []byte) error {
	msg := NewData(c.connid, c.writeseqid, payload)
	//fmt.Println(msg)
	c.writeChan <- msg
	c.writeseqid++

	val := <-c.writeReplyChan
	return val
}

func (c *client) Close() error {
	c.appcloseChan <- struct{}{}

	val := <-c.closeReplyChan
	return val
}

func ClientnetworkHandler(c *client) {
	var buffer [1500]byte

	for {
		select {
		case <-c.shutdown:
			//fmt.Println("client network exit!")
			return
			//break

		default:
			n, _, err := c.conn.ReadFromUDP(buffer[0:])
			if err != nil {
				continue
			}

			var msg Message
			err = json.Unmarshal(buffer[0:n], &msg)
			if err != nil {
				continue
			}

			//fmt.Println("get data!")
			c.receiveChan <- &msg
			if msg.Type == MsgAck && msg.SeqNum > 0 {
				//fmt.Println("get ack for sequence: ",msg.SeqNum)
			}
		}
	}
}

func ClientepochHandler(c *client) {
	d := time.Duration(c.parameter.EpochMillis) * time.Millisecond

	for {
		select {
		case <-c.shutdown:
			//fmt.Println("client epoch exit!")
			return
			//break

		default:
			time.Sleep(d)
			c.epochChan <- struct{}{}
		}
	}
}

func ClientmasterEvenHandler(c *client) {
	for !(c.networkStopFlag && c.appStopFlag) {

		// Judege if readList is empty
		if c.readList.Len() == 0 {
			if c.networkStopFlag == true {
				c.appStopFlag = true
				break
			}

			select {
			case msg := <-c.receiveChan:
				c.handleReceiveMessage(msg)

			case msg := <-c.writeChan:
				c.handleWriteMessage(msg)

			case <-c.epochChan:
				c.handleEpoch()

			case <-c.appcloseChan:
				c.appStopFlag = true
			}
		} else {
			e := c.readList.Front()
			select {
			case msg := <-c.receiveChan:
				c.handleReceiveMessage(msg)

			case msg := <-c.writeChan:
				c.handleWriteMessage(msg)

			case <-c.epochChan:
				c.handleEpoch()

			case c.readChan <- e.Value.(*Message):
				c.readList.Remove(e)

			case <-c.appcloseChan:
				c.appStopFlag = true
			}
		}

		// Handle the write buffer to see if we need send data
		c.handleWriteBuffer()
	}

	// when network and application are both stoped
	//fmt.Println("client exit!")
	close(c.shutdown)
	c.closeReplyChan <- nil
	c.writeReplyChan <- errors.New("connection closed!")
	c.readChan <- nil
	//fmt.Println("clinet do close!")
}

func (c *client) handleReceiveMessage(msg *Message) {
	c.lastepoch = c.epochtimes

	switch msg.Type {
	case MsgData:
		// data received but connection haven't been established
		if c.connid == 0 {
			return
		}

		number := c.ackPendingNum
		// first ACKs must have been received by server, so we could shift ACK window
		if msg.SeqNum >= number+c.parameter.WindowSize {
			c.shiftAckWindow(msg.SeqNum)
		}

		number = c.ackPendingNum
		// messages smaller than the window desired will be ignored in case of duplicate
		if msg.SeqNum >= number && msg.SeqNum < number+c.parameter.WindowSize {
			index := msg.SeqNum - number

			//check if we have received before in case of duplicate
			if !c.ackWindowIndicator[index] {
				// Insert into readList
				c.ackWindowIndicator[index] = true
				c.ackSlidingWindow[index] = msg

				if msg.SeqNum == c.receiveseqid {
					c.insertReadList(msg)
				}

				// send ack message for this data
				ack := NewAck(c.connid, msg.SeqNum)
				p, _ := json.Marshal(ack)

				c.conn.Write(p)
			}
		}

	case MsgAck:
		number := c.writePendingNum

		// check for connection
		if number == 0 {
			if c.connectPending == true {
				c.connectChan <- msg
				c.writePendingNum += 1
				c.ackPendingNum = 1

				c.connectPending = false
				c.receiveseqid = 1
			}
		} else if msg.SeqNum >= number && msg.SeqNum < number+c.parameter.WindowSize {
			// ACKs smaller than sliding window will be ingnored
			index := msg.SeqNum - number
			c.writeWindowIndicator[index] = true

			// check if we can shift writing window
			if msg.SeqNum == number {
				c.shiftWriteWindow()
			}
		}
	}
}

func (c *client) handleWriteMessage(msg *Message) {
	c.writeList.PushBack(msg)

	if c.networkStopFlag == true {
		c.writeReplyChan <- errors.New("connection lost!")
	} else {
		//fmt.Println("has sent!")
		c.writeReplyChan <- nil
	}
}

func (c *client) handleEpoch() {
	c.epochtimes += 1

	// First Judge if exceed epoch limit
	//fmt.Println(c.connid, c.epochtimes)
	//fmt.Println("last epoch: ", c.lastepoch)
	if (c.epochtimes - c.lastepoch) > c.parameter.EpochLimit {
		// network connection has been lost!
		//fmt.Println("connection close!")
		c.networkStopFlag = true
		c.conn.Close()
		//fmt.Println("connection lost due to epoch!")

		if c.connid == 0 {
			c.connectChan <- nil
		}

		return
	} else {
		// First check if connection has been set up
		if c.connid == 0 {
			connMsg := NewConnect()
			p, _ := json.Marshal(connMsg)

			c.conn.Write(p)
			return //TODO: how to judge connection is closed?
		} else if c.receiveseqid == 0 {
			ack := NewAck(c.connid, 0)
			p, _ := json.Marshal(ack)

			c.conn.Write(p)
		}

		// send data message haven't been acknowledged
		for i := 0; i < c.parameter.WindowSize; i++ {
			if c.writeSlidingWindow[i] != nil {
				if c.writeWindowIndicator[i] == false {
					msg := c.writeSlidingWindow[i]
					p, _ := json.Marshal(msg)

					c.conn.Write(p)
				}
			}
		}

		// resend acknowledgement mesages
		for i := 0; i < c.parameter.WindowSize; i++ {
			if c.ackWindowIndicator[i] == true {
				SeqNum := c.ackPendingNum + i
				ack := NewAck(c.connid, SeqNum)
				p, _ := json.Marshal(ack)

				c.conn.Write(p)
			}
		}
	}

}

func (c *client) handleWriteBuffer() {
	// Read data from buffer until sliding window is full
	if c.networkStopFlag == true {
		//fmt.Println("write buffer exist!")
		return
	}

	// check if all the messages have been sent and acknowledged
	if c.writeList.Len() == 0 && c.writeSlidingWindow[0] == nil {
		if c.appStopFlag == true {
			c.networkStopFlag = true
		}
	}

	for c.writeList.Len() > 0 && c.networkStopFlag == false {
		e := c.writeList.Front()
		msg := e.Value.(*Message)
		//fmt.Println("want to write: ", msg.SeqNum)
		index := msg.SeqNum - c.writePendingNum
		if index < c.parameter.WindowSize {
			c.writeList.Remove(e)
			p, _ := json.Marshal(msg)

			c.conn.Write(p)
			c.writeSlidingWindow[index] = msg
		} else {
			break
		}
	}
}

func (c *client) insertReadList(msg *Message) {
	index := msg.SeqNum - c.ackPendingNum

	for index < c.parameter.WindowSize {
		msg := c.ackSlidingWindow[index]
		if msg != nil {
			c.insertReadBuffer(msg)
			c.receiveseqid += 1
		} else {
			break
		}
		index += 1
	}
}

func (c *client) insertReadBuffer(msg *Message) {
	// TODO: insert data according to order
	number := msg.SeqNum
	success := false

	for e := c.readList.Front(); e != nil; e = e.Next() {
		tempmsg := e.Value.(*Message)
		if tempmsg.SeqNum > number {
			c.readList.InsertBefore(msg, e)
			success = true
			break
		}
	}

	if success == false {
		c.readList.PushBack(msg)
		success = true
	}
}

func (c *client) shiftWriteWindow() {
	firstnumber := c.writePendingNum
	shift := 0

	// Get the shift size
	for shift < c.parameter.WindowSize && c.writeWindowIndicator[shift] {
		shift += 1
	}

	// Shift write sliding window
	if shift < c.parameter.WindowSize {
		for i := shift; i < c.parameter.WindowSize; i++ {
			c.writeWindowIndicator[i-shift] = c.writeWindowIndicator[i]
			c.writeSlidingWindow[i-shift] = c.writeSlidingWindow[i]
		}

		i := c.parameter.WindowSize - shift
		for i < c.parameter.WindowSize {
			c.writeWindowIndicator[i] = false
			c.writeSlidingWindow[i] = nil
			i += 1
		}
	} else {
		for i := 0; i < c.parameter.WindowSize; i++ {
			c.writeWindowIndicator[i] = false
			c.writeSlidingWindow[i] = nil
		}
	}

	// update Frist pending message number
	c.writePendingNum = firstnumber + shift

}

func (c *client) shiftAckWindow(number int) {
	firstnumber := c.ackPendingNum
	shift := number - (firstnumber + c.parameter.WindowSize - 1)

	//Shift ack sliding window
	if shift < c.parameter.WindowSize {
		for i := shift; i < c.parameter.WindowSize; i++ {
			c.ackWindowIndicator[i-shift] = c.ackWindowIndicator[i]
			c.ackSlidingWindow[i-shift] = c.ackSlidingWindow[i]
		}

		i := c.parameter.WindowSize - shift
		for i < c.parameter.WindowSize {
			c.ackWindowIndicator[i] = false
			c.ackSlidingWindow[i] = nil
			i += 1
		}
	} else {
		for i := 0; i < c.parameter.WindowSize; i++ {
			c.ackWindowIndicator[i] = false
			c.ackSlidingWindow[i] = nil
		}
	}

	c.ackPendingNum = firstnumber + shift
}
