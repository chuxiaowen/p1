// Contains the implementation of a LSP server.

package lsp

import "errors"

import (
	"container/list"
	"encoding/json"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
	//"fmt"
)

type netpackage struct {
	msg  *Message
	addr *lspnet.UDPAddr
}

type lspcon struct {
	connadr *lspnet.UDPAddr
	connid  int

	receiveseqid int //number of sequence receieved so far
	writeseqid   int
	lastepoch    int // number of last epoch for each client

	writeList *list.List // write buffer for each client
	lastAck   *Message

	networkStopFlag bool
	appStopFlag     bool

	ackSlidingWindow     []*Message
	ackWindowIndicator   []bool
	ackPendingNum        int
	writeSlidingWindow   []*Message
	writeWindowIndicator []bool
	writePendingNum      int
}

type server struct {
	// TODO: implement this!
	receiveChan       chan *netpackage // Channel to receive netpackage from network
	readChan          chan *Message    // feed back read for application
	writeChan         chan *Message    // Channel to receive message from application
	epochChan         signalChan       // Epoch signal channel
	shutdown          signalChan       // close signal channel
	appcloseChan      signalChan       // application close signal channel
	localappcloseChan chan int         // close for a client

	writeReplyChan         chan error
	closeAllReplyChan      chan error
	localappcloseReplyChan chan error

	conn    *lspnet.UDPConn // connection for server
	connAdr *lspnet.UDPAddr // server address

	connMap   map[string]*lspcon // map for each client connection by address
	connidMap map[int]*lspcon    // map fro each client connection by id

	allnetworkStopFlag bool // flag for all network stop
	appStopFlag        bool // flag for application stop

	readList  *list.List // read buffer for data from network
	parameter *Params    // parameters

	clientid   int // number of clients connected to server, start from 1
	epochtimes int // number of epoch on server

}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	// Initialize a new server
	server := new(server)

	addr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(port))
	//fmt.Println(addr)
	if err != nil {
		return nil, err
	}

	conn, err := lspnet.ListenUDP("udp", addr)
	//fmt.Println(conn)
	if err != nil {
		return nil, err
	}

	server.receiveChan = make(chan *netpackage, 1)
	server.readChan = make(chan *Message, 2)
	server.writeChan = make(chan *Message, 1)
	server.epochChan = make(signalChan)
	server.shutdown = make(signalChan)
	server.appcloseChan = make(signalChan)
	server.localappcloseChan = make(chan int, 1)

	server.writeReplyChan = make(chan error, 1)
	server.closeAllReplyChan = make(chan error, 1)
	server.localappcloseReplyChan = make(chan error, 1)

	server.conn = conn
	server.connAdr = addr

	server.connMap = make(map[string]*lspcon)
	server.connidMap = make(map[int]*lspcon)

	server.allnetworkStopFlag = false
	server.appStopFlag = false

	server.readList = list.New()
	server.parameter = params

	server.clientid = 1
	server.epochtimes = 0

	go ServernetworkHandler(server)
	go ServerepochHandler(server)
	go ServermasterEvenHandler(server)

	return server, nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	// Blocks indefinitely.
	msg := <-s.readChan

	/*if msg != nil {
		return msg.ConnID, msg.Payload, nil
	} else {
		return -1, nil, errors.New("read failed!")
	}*/

	if msg.SeqNum > 0 {
		return msg.ConnID, msg.Payload, nil
	} else {
		return msg.ConnID, nil, errors.New("read failed!")
	}
}

func (s *server) Write(connID int, payload []byte) error {
	msg := NewData(connID, 0, payload)
	s.writeChan <- msg

	val := <-s.writeReplyChan
	return val
}

func (s *server) CloseConn(connID int) error {
	s.localappcloseChan <- connID
	val := <-s.localappcloseReplyChan

	return val
}

func (s *server) Close() error {
	s.appcloseChan <- struct{}{}

	val := <-s.closeAllReplyChan
	return val
}

func ServernetworkHandler(s *server) {
	// TODO: Reads packets from the UDP connection and notifies the event handler
	// when messages are received
	var buffer [1500]byte

	for {
		select {
		case <-s.shutdown:
			return
			//break

		default:
			n, addr, err := s.conn.ReadFromUDP(buffer[0:])
			if err != nil {
				continue
			}

			var msg Message
			err = json.Unmarshal(buffer[0:n], &msg)
			if err != nil {
				continue
			}

			data := &netpackage{&msg, addr}
			//fmt.Println(msg.Type)
			s.receiveChan <- data
		}
	}
}

func ServerepochHandler(s *server) {
	// TODO: Triggers an epoch event once every δ milliseconds and notifies the event
	// handler when one occurs.
	d := time.Duration(s.parameter.EpochMillis) * time.Millisecond

	for {
		select {
		case <-s.shutdown:
			return
			//break

		default:
			time.Sleep(d)
			s.epochChan <- struct{}{}
		}
	}
}

func ServermasterEvenHandler(s *server) {
	// TODO: Manages the execution of all possible events
	for !(s.allnetworkStopFlag && s.appStopFlag) {
		//e := s.GetReadList()
		// Check if readList is empty
		if s.readList.Len() == 0 {
			if s.allnetworkStopFlag == true {
				s.appStopFlag = true
			}
			select {
			case pkg := <-s.receiveChan:
				s.handleReceiveMessage(pkg)

			case msg := <-s.writeChan:
				s.handleWriteMessage(msg)

			case <-s.epochChan:
				s.handleEpoch()

			case <-s.appcloseChan:
				s.appStopFlag = true

			case id := <-s.localappcloseChan:
				s.handleLocalClose(id)
			}
		} else {
			//e := s.GetReadList()
			e := s.readList.Front()
			select {
			case pkg := <-s.receiveChan:
				s.handleReceiveMessage(pkg)

			case msg := <-s.writeChan:
				s.handleWriteMessage(msg)

			case <-s.epochChan:
				s.handleEpoch()

			case s.readChan <- e.Value.(*Message):
				s.readList.Remove(e)

			case <-s.appcloseChan:
				s.appStopFlag = true

			case id := <-s.localappcloseChan:
				s.handleLocalClose(id)
			}
		}

		// Handle the write buffer
		s.handleWriteBuffer()
	}

	//fmt.Println("server exist")
	close(s.shutdown)
	s.readChan <- nil
	s.writeReplyChan <- errors.New("connection lost!")
	s.closeAllReplyChan <- errors.New("all connection lost!")
	//fmt.Println("server do close!")
}

func (s *server) handleReceiveMessage(pkg *netpackage) {
	msg := pkg.msg
	addr := pkg.addr

	id := msg.ConnID
	con := s.connidMap[id]

	if con == nil {
		if msg.Type != MsgConnect {
			return
		}
	} else {
		con.lastepoch = s.epochtimes
	}

	if con != nil && (con.networkStopFlag == true || s.allnetworkStopFlag == true) {
		return
	}

	switch msg.Type {
	case MsgConnect:
		if msg.SeqNum != 0 {
			return
		}

		// chekc if we have received the connection message before
		if con != nil {
			con.lastepoch = s.epochtimes

			// we need resend acknowledgements
			ack := con.lastAck
			p, _ := json.Marshal(ack)

			s.conn.WriteToUDP(p, addr)
			return
		} else {
			// it is a new connection
			id := s.clientid
			s.clientid += 1

			//send ack to client
			ack := NewAck(id, 0)
			p, _ := json.Marshal(ack)
			s.conn.WriteToUDP(p, addr)

			// modify the client map
			con := newLspConnection(id, addr, s.epochtimes, s.parameter)
			//fmt.Println("get new connection: ", id)
			con.lastAck = ack
			s.connMap[addr.String()] = con
			s.connidMap[id] = con
			con.receiveseqid = 1
		}

	case MsgData:
		// first check whether network for this port has stopped
		if con.networkStopFlag == true {
			return
		}

		number := con.ackPendingNum
		// first ACK must have been received by client, thus we could shift ack sliding window
		if msg.SeqNum >= number+s.parameter.WindowSize {
			s.shiftAckWindw(msg.SeqNum, con)
		}

		number = con.ackPendingNum
		if msg.SeqNum >= number && msg.SeqNum < number+s.parameter.WindowSize {
			index := msg.SeqNum - number

			//check if we have received before in case of duplicate
			if !con.ackWindowIndicator[index] {
				// Insert into readList
				//fmt.Println("get new data")
				con.ackWindowIndicator[index] = true
				con.ackSlidingWindow[index] = msg

				if msg.SeqNum == con.receiveseqid {
					s.insertReadList(con, msg)
				}

				// send ack message for this data
				ack := NewAck(con.connid, msg.SeqNum)
				p, _ := json.Marshal(ack)

				s.conn.WriteToUDP(p, con.connadr)
			}
		}

	case MsgAck:
		number := con.writePendingNum

		// check for there is message need to be acknowledged
		if con.writeSlidingWindow[0] == nil {
			return
		}

		if msg.SeqNum >= number && msg.SeqNum < number+s.parameter.WindowSize {
			index := msg.SeqNum - number
			con.writeWindowIndicator[index] = true

			//check if we can shift writing window
			if msg.SeqNum == number {
				s.shiftWriteWindow(con)
			}
		}

	}
}

func (s *server) handleWriteMessage(msg *Message) {
	id := msg.ConnID
	con := s.connidMap[id]

	if con == nil {
		s.writeReplyChan <- errors.New("connection does not exist!")
	} else {
		msg.SeqNum = con.writeseqid
		con.writeseqid += 1

		con.writeList.PushBack(msg)
		s.writeReplyChan <- nil
	}
}

func (s *server) handleEpoch() {
	s.epochtimes += 1

	for _, con := range s.connMap {
		//fmt.Println(s.epochtimes)
		//fmt.Println(con.lastepoch)
		if (s.epochtimes - con.lastepoch) > s.parameter.EpochLimit {
			con.networkStopFlag = true
			//fmt.Println("clinet lost, number: ",con.connid)

			//insert a invalid message
			msg := NewData(con.connid, -1, nil)
			//fmt.Println("insert invalid message", msg)
			s.insertReadBuffer(msg)

			s.deleteConnection(con)

			if len(s.connMap) == 0 {
				s.allnetworkStopFlag = true
				//fmt.Println("all connection lost from server!")
				s.conn.Close()
			}
			//con.conn.Close()

			continue
		} else {
			if con.receiveseqid == 0 {
				//haven't received messages from clients,resend ack
				ack := NewAck(con.connid, 0)
				p, _ := json.Marshal(ack)

				s.conn.WriteToUDP(p, con.connadr)
			}

			//resend data message which haven't been acknowledged
			for i := 0; i < s.parameter.WindowSize; i++ {
				if con.writeSlidingWindow[i] != nil {
					if con.writeWindowIndicator[i] == false {
						msg := con.writeSlidingWindow[i]
						p, _ := json.Marshal(msg)

						s.conn.WriteToUDP(p, con.connadr)
					}
				}
			}

			// resend acknowledgement messages
			for i := 0; i < s.parameter.WindowSize; i++ {
				if con.ackWindowIndicator[i] == true {
					SeqNum := con.ackPendingNum + i
					ack := NewAck(con.connid, SeqNum)
					p, _ := json.Marshal(ack)

					s.conn.WriteToUDP(p, con.connadr)
				}
			}
		}
	}

	// check if we need to shutdown all network
	if s.appStopFlag == true && len(s.connMap) == 0 {
		s.allnetworkStopFlag = true
		s.conn.Close()
	}

}

func (s *server) handleLocalClose(id int) {
	con := s.connidMap[id]

	if con == nil {
		s.localappcloseReplyChan <- errors.New("no such id!")
	} else {
		con.appStopFlag = true

		// check if we need set the global one
		count := 0
		for _, con := range s.connMap {
			if con.appStopFlag == true {
				count += 1
			}
		}

		if count == len(s.connMap) {
			s.appStopFlag = true
		}
		s.localappcloseReplyChan <- nil
	}
}

func (s *server) handleWriteBuffer() {
	if len(s.connMap) == 0 && s.appStopFlag == true {
		s.allnetworkStopFlag = true
		s.conn.Close()
		//fmt.Println("server shut all the network!")
	}

	for _, con := range s.connMap {
		if con.networkStopFlag == true && con.appStopFlag == true {
			s.deleteConnection(con)
			continue
		}

		if con.writeList.Len() == 0 && con.writeSlidingWindow[0] == nil {
			//fmt.Println("send all messages!")
			if con.appStopFlag == true || s.appStopFlag == true {
				con.networkStopFlag = true
				s.deleteConnection(con)
				//fmt.Println("delete connection: ",con.connid)
			}
		}

		for con.writeList.Len() > 0 && con.networkStopFlag == false {
			e := con.writeList.Front()
			msg := e.Value.(*Message)
			index := msg.SeqNum - con.writePendingNum

			if index < s.parameter.WindowSize {
				con.writeList.Remove(e)
				p, _ := json.Marshal(msg)

				s.conn.WriteToUDP(p, con.connadr)
				con.writeSlidingWindow[index] = msg
			} else {
				break
			}
		}

		/*if con.writeList.Len() == 0  {
			//fmt.Println("send all messages!")
			if con.appStopFlag == true || s.appStopFlag == true {
				con.networkStopFlag = true
				s.deleteConnection(con)
				fmt.Println("delete connection: ",con.connid)
			}
		}*/

	}
}

func (s *server) deleteConnection(con *lspcon) {
	delete(s.connidMap, con.connid)
	delete(s.connMap, con.connadr.String())
}

func newLspConnection(id int, addr *lspnet.UDPAddr, epochtimes int, params *Params) *lspcon {

	con := new(lspcon)

	con.connadr = addr
	con.connid = id

	con.receiveseqid = 0
	con.writeseqid = 1
	con.lastepoch = epochtimes

	con.writeList = list.New()

	con.networkStopFlag = false
	con.appStopFlag = false

	con.ackSlidingWindow = make([]*Message, params.WindowSize)
	con.ackWindowIndicator = make([]bool, params.WindowSize)
	con.ackPendingNum = 1
	con.writeSlidingWindow = make([]*Message, params.WindowSize)
	con.writeWindowIndicator = make([]bool, params.WindowSize)
	con.writePendingNum = 1

	return con
}

func (s *server) insertReadList(con *lspcon, msg *Message) {
	index := msg.SeqNum - con.ackPendingNum

	for index < s.parameter.WindowSize {
		msg := con.ackSlidingWindow[index]
		if msg != nil {
			s.insertReadBuffer(msg)
			con.receiveseqid += 1
		} else {
			break
		}
		index += 1
	}
}

func (s *server) insertReadBuffer(msg *Message) {
	id := msg.ConnID
	number := msg.SeqNum
	success := false

	if number == -1 {
		//s.readList.PushBack(msg)
		//return
		for e := s.readList.Front(); e != nil; e = e.Next() {
			tempmsg := e.Value.(*Message)
			if tempmsg.ConnID == id && tempmsg.SeqNum == -1 {
				return
			}
		}
	}

	for e := s.readList.Front(); e != nil; e = e.Next() {
		tempmsg := e.Value.(*Message)
		if tempmsg.ConnID != id {
			continue
		} else {
			if tempmsg.SeqNum > number {
				s.readList.InsertBefore(msg, e)
				success = true
				break
			}
		}
	}

	if success == false {
		s.readList.PushBack(msg)
		success = true
	}
}

func (s *server) shiftAckWindw(number int, con *lspcon) {
	firstnumber := con.ackPendingNum
	shift := number - (firstnumber + s.parameter.WindowSize - 1)

	//shift ack sliding window
	if shift < s.parameter.WindowSize {
		for i := shift; i < s.parameter.WindowSize; i++ {
			con.ackWindowIndicator[i-shift] = con.ackWindowIndicator[i]
			con.ackSlidingWindow[i-shift] = con.ackSlidingWindow[i]
		}

		i := s.parameter.WindowSize - shift
		for i < s.parameter.WindowSize {
			con.ackWindowIndicator[i] = false
			con.ackSlidingWindow[i] = nil
			i += 1
		}
	} else {
		for i := 0; i < s.parameter.WindowSize; i++ {
			con.ackWindowIndicator[i] = false
			con.ackSlidingWindow[i] = nil
		}
	}

	con.ackPendingNum = firstnumber + shift
}

func (s *server) shiftWriteWindow(con *lspcon) {
	firstnumber := con.writePendingNum
	shift := 0

	//Get shift size
	for shift < s.parameter.WindowSize && con.writeWindowIndicator[shift] {
		shift += 1
	}

	//shift write sliding window
	if shift < s.parameter.WindowSize {
		for i := shift; i < s.parameter.WindowSize; i++ {
			con.writeWindowIndicator[i-shift] = con.writeWindowIndicator[i]
			con.writeSlidingWindow[i-shift] = con.writeSlidingWindow[i]
		}

		i := s.parameter.WindowSize - shift
		for i < s.parameter.WindowSize {
			con.writeWindowIndicator[i] = false
			con.writeSlidingWindow[i] = nil
			i += 1
		}
	} else {
		for i := 0; i < s.parameter.WindowSize; i++ {
			con.writeWindowIndicator[i] = false
			con.writeSlidingWindow[i] = nil
		}
	}

	// update First pending message number
	con.writePendingNum = firstnumber + shift
}
