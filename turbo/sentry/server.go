package sentry

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/p2p/enr"
	"github.com/ledgerwatch/turbo-geth/params"
	"google.golang.org/protobuf/types/known/emptypb"
)

type StreamMsg struct {
	b       []byte
	peerID  string
	msgName string
	msgId   sentry.MessageId
}

func NewServer(ctx context.Context, receiveCh chan StreamMsg, receiveUploadCh chan StreamMsg) *SentryServerImpl {
	return &SentryServerImpl{ctx: ctx, receiveCh: receiveCh, receiveUploadCh: receiveUploadCh}
}

type SentryServerImpl struct {
	sentry.UnimplementedSentryServer
	ctx             context.Context
	NatSetting      string
	Port            int
	StaticPeers     []string
	Discovery       bool
	NetRestrict     string
	Peers           sync.Map
	PeerHeightMap   sync.Map
	PeerRwMap       sync.Map
	PeerTimeMap     sync.Map
	statusData      *sentry.StatusData
	P2pServer       *p2p.Server
	receiveCh       chan StreamMsg
	receiveUploadCh chan StreamMsg
	lock            sync.RWMutex
}

func (ss *SentryServerImpl) PenalizePeer(_ context.Context, req *sentry.PenalizePeerRequest) (*empty.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	strId := string(gointerfaces.ConvertH512ToBytes(req.PeerId))
	ss.PeerRwMap.Delete(strId)
	ss.PeerTimeMap.Delete(strId)
	ss.PeerHeightMap.Delete(strId)
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) PeerMinBlock(_ context.Context, req *sentry.PeerMinBlockRequest) (*empty.Empty, error) {
	peerID := string(gointerfaces.ConvertH512ToBytes(req.PeerId))
	x, _ := ss.PeerHeightMap.Load(peerID)
	highestBlock, _ := x.(uint64)
	if req.MinBlock > highestBlock {
		ss.PeerHeightMap.Store(peerID, req.MinBlock)
	}
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) findPeer(minBlock uint64) (string, bool) {
	// Choose a peer that we can send this request to
	var peerID string
	var found bool
	timeNow := time.Now().Unix()
	ss.PeerHeightMap.Range(func(key, value interface{}) bool {
		valUint, _ := value.(uint64)
		if valUint >= minBlock {
			peerID = key.(string)
			timeRaw, _ := ss.PeerTimeMap.Load(peerID)
			t, _ := timeRaw.(int64)
			// If request is large, we give 5 second pause to the peer before sending another request, unless it responded
			if t <= timeNow {
				found = true
				return false
			}
		}
		return true
	})
	return peerID, found
}

func (ss *SentryServerImpl) SendMessageByMinBlock(_ context.Context, inreq *sentry.SendMessageByMinBlockRequest) (*sentry.SentPeers, error) {
	peerID, found := ss.findPeer(inreq.MinBlock)
	if !found {
		return &sentry.SentPeers{}, nil
	}
	rwRaw, _ := ss.PeerRwMap.Load(peerID)
	rw, _ := rwRaw.(p2p.MsgReadWriter)
	if rw == nil {
		ss.PeerHeightMap.Delete(peerID)
		ss.PeerTimeMap.Delete(peerID)
		ss.PeerRwMap.Delete(peerID)
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock find rw for peer %s", peerID)
	}
	var msgcode uint64
	switch inreq.Data.Id {
	case sentry.MessageId_GetBlockHeaders:
		msgcode = eth.GetBlockHeadersMsg
	case sentry.MessageId_GetBlockBodies:
		msgcode = eth.GetBlockBodiesMsg
	default:
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s", inreq.Data.Id)
	}
	if err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		ss.PeerHeightMap.Delete(peerID)
		ss.PeerTimeMap.Delete(peerID)
		ss.PeerRwMap.Delete(peerID)
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock to peer %s: %v", peerID, err)
	}
	ss.PeerTimeMap.Store(peerID, time.Now().Unix()+5)
	return &sentry.SentPeers{Peers: []*types.H512{gointerfaces.ConvertBytesToH512([]byte(peerID))}}, nil
}

func (ss *SentryServerImpl) SendMessageById(_ context.Context, inreq *sentry.SendMessageByIdRequest) (*sentry.SentPeers, error) {
	peerID := string(gointerfaces.ConvertH512ToBytes(inreq.PeerId))
	rwRaw, ok := ss.PeerRwMap.Load(peerID)
	if !ok {
		return &sentry.SentPeers{}, fmt.Errorf("peer not found: %s", peerID)
	}
	rw, _ := rwRaw.(p2p.MsgReadWriter)
	var msgcode uint64
	switch inreq.Data.Id {
	case sentry.MessageId_GetBlockHeaders:
		msgcode = eth.GetBlockHeadersMsg
	case sentry.MessageId_BlockHeaders:
		msgcode = eth.BlockHeadersMsg
	case sentry.MessageId_BlockBodies:
		msgcode = eth.BlockBodiesMsg
	case sentry.MessageId_GetReceipts:
		msgcode = eth.GetReceiptsMsg
	case sentry.MessageId_Receipts:
		msgcode = eth.ReceiptsMsg
	case sentry.MessageId_PooledTransactions:
		msgcode = eth.PooledTransactionsMsg
	default:
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageById not implemented for message Id: %s", inreq.Data.Id)
	}

	if err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		ss.PeerHeightMap.Delete(peerID)
		ss.PeerTimeMap.Delete(peerID)
		ss.PeerRwMap.Delete(peerID)
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageById to peer %s: %v", peerID, err)
	}
	return &sentry.SentPeers{Peers: []*types.H512{inreq.PeerId}}, nil
}

func (ss *SentryServerImpl) SendMessageToRandomPeers(ctx context.Context, req *sentry.SendMessageToRandomPeersRequest) (*sentry.SentPeers, error) {
	var msgcode uint64
	switch req.Data.Id {
	case sentry.MessageId_NewBlock:
		msgcode = eth.NewBlockMsg
	case sentry.MessageId_NewBlockHashes:
		msgcode = eth.NewBlockHashesMsg
	default:
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Data.Id)
	}

	amount := uint64(0)
	ss.PeerRwMap.Range(func(key, value interface{}) bool {
		amount++
		return true
	})
	if req.MaxPeers > amount {
		amount = req.MaxPeers
	}

	// Send the block to a subset of our Peers
	sendToAmount := int(math.Sqrt(float64(amount)))
	i := 0
	var innerErr error
	reply := &sentry.SentPeers{Peers: []*types.H512{}}
	ss.PeerRwMap.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		rw, _ := value.(p2p.MsgReadWriter)
		if err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(req.Data.Data)), Payload: bytes.NewReader(req.Data.Data)}); err != nil {
			ss.PeerHeightMap.Delete(peerID)
			ss.PeerTimeMap.Delete(peerID)
			ss.PeerRwMap.Delete(peerID)
			innerErr = err
			return false
		}
		reply.Peers = append(reply.Peers, gointerfaces.ConvertBytesToH512([]byte(peerID)))
		i++
		return sendToAmount <= i
	})
	if innerErr != nil {
		return reply, fmt.Errorf("sendMessageToRandomPeers to peer %w", innerErr)
	}
	return reply, nil
}

func (ss *SentryServerImpl) SendMessageToAll(ctx context.Context, req *sentry.OutboundMessageData) (*sentry.SentPeers, error) {
	var msgcode uint64
	switch req.Id {
	case sentry.MessageId_NewBlock:
		msgcode = eth.NewBlockMsg
	case sentry.MessageId_NewBlockHashes:
		msgcode = eth.NewBlockHashesMsg
	default:
		return &sentry.SentPeers{}, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Id)
	}

	var innerErr error
	reply := &sentry.SentPeers{Peers: []*types.H512{}}
	ss.PeerRwMap.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		rw, _ := value.(p2p.MsgReadWriter)
		if err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(req.Data)), Payload: bytes.NewReader(req.Data)}); err != nil {
			ss.PeerHeightMap.Delete(peerID)
			ss.PeerTimeMap.Delete(peerID)
			ss.PeerRwMap.Delete(peerID)
			innerErr = err
			return false
		}
		reply.Peers = append(reply.Peers, gointerfaces.ConvertBytesToH512([]byte(peerID)))
		return true
	})
	if innerErr != nil {
		return reply, fmt.Errorf("sendMessageToRandomPeers to peer %w", innerErr)
	}
	return reply, nil
}

func (ss *SentryServerImpl) SetStatus(_ context.Context, statusData *sentry.StatusData) (*emptypb.Empty, error) {
	genesisHash := gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)

	ss.lock.Lock()
	defer ss.lock.Unlock()
	init := ss.statusData == nil
	if init {
		//var err error
		//ss.P2pServer, err = download.P2pServer(ss.ctx, ss, genesisHash, ss.NatSetting, ss.Port, ss.StaticPeers, ss.Discovery, ss.NetRestrict)
		//if err != nil {
		//	return &empty.Empty{}, err
		//}
		// Add protocol
		if err := ss.P2pServer.Start(); err != nil {
			return &empty.Empty{}, fmt.Errorf("could not start server: %w", err)
		}
	}
	genesisHash = gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)
	ss.P2pServer.LocalNode().Set(eth.CurrentENREntryFromForks(statusData.ForkData.Forks, genesisHash, statusData.MaxBlock))
	ss.statusData = statusData
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) getStatus() *sentry.StatusData {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.statusData
}

func (ss *SentryServerImpl) receive(msg *StreamMsg) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	select {
	case ss.receiveCh <- *msg:
	default:
		// TODO make a warning about dropped messages
	}
}

func (ss *SentryServerImpl) recreateReceive() {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	// Close previous channel and recreate
	close(ss.receiveCh)
	ss.receiveCh = make(chan StreamMsg, 1024)
}

func (ss *SentryServerImpl) ReceiveMessages(_ *emptypb.Empty, server sentry.Sentry_ReceiveMessagesServer) error {
	ss.recreateReceive()
	for streamMsg := range ss.receiveCh {
		outreq := sentry.InboundMessage{
			PeerId: gointerfaces.ConvertBytesToH512([]byte(streamMsg.peerID)),
			Id:     streamMsg.msgId,
			Data:   streamMsg.b,
		}
		if err := server.Send(&outreq); err != nil {
			log.Error("Sending msg to core P2P failed", "msg", streamMsg.msgName, "error", err)
			return err
		}
		//fmt.Printf("Sent message %s\n", streamMsg.msgName)
	}
	log.Warn("Finished receive messages")
	return nil
}

func (ss *SentryServerImpl) receiveUpload(msg *StreamMsg) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	select {
	case ss.receiveUploadCh <- *msg:
	default:
		// TODO make a warning about dropped messages
	}
}

func (ss *SentryServerImpl) recreateReceiveUpload() {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	// Close previous channel and recreate
	close(ss.receiveUploadCh)
	ss.receiveUploadCh = make(chan StreamMsg, 1024)
}

func (ss *SentryServerImpl) ReceiveUploadMessages(_ *emptypb.Empty, server sentry.Sentry_ReceiveUploadMessagesServer) error {
	// Close previous channel and recreate
	ss.recreateReceiveUpload()
	for streamMsg := range ss.receiveUploadCh {
		outreq := sentry.InboundMessage{
			PeerId: gointerfaces.ConvertBytesToH512([]byte(streamMsg.peerID)),
			Id:     streamMsg.msgId,
			Data:   streamMsg.b,
		}
		if err := server.Send(&outreq); err != nil {
			log.Error("Sending msg to core P2P failed", "msg", streamMsg.msgName, "error", err)
			return err
		}
		//fmt.Printf("Sent upload message %s\n", streamMsg.msgName)
	}
	log.Warn("Finished receive upload messages")
	return nil
}

func MakeProtocols(
	ctx context.Context,
	readNodeInfo func() *eth.NodeInfo,
	peers *sync.Map,
	peerHeightMap *sync.Map,
	peerTimeMap *sync.Map,
	peerRwMap *sync.Map,
	ss *SentryServerImpl,
	dnsdisc enode.Iterator, chainConfig *params.ChainConfig, genesisHash common.Hash,
	headHeight uint64) []p2p.Protocol {

	protocols := make([]p2p.Protocol, 1)
	for i, version := range []uint{eth.ETH66} {
		version := version // Closure

		protocols[i] = p2p.Protocol{
			Name:    eth.ProtocolName,
			Version: version,
			Length:  17,
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
				peerID := p.ID().String()
				log.Info(fmt.Sprintf("[%s] Start with peer", peerID))
				peers.Store(peerID, p)
				peerRwMap.Store(peerID, rw)
				if err := RunPeer(
					ctx,
					peerHeightMap,
					peerTimeMap,
					peerRwMap,
					p,
					version, // version == eth66
					version, // minVersion == eth66
					ss,
				); err != nil {
					return fmt.Errorf("[%s] Error while running peer: %w", peerID, err)
				}
				peerHeightMap.Delete(peerID)
				peerTimeMap.Delete(peerID)
				peerRwMap.Delete(peerID)
				peers.Delete(peerID)
				return nil
			},
			NodeInfo: func() interface{} {
				return readNodeInfo()
			},
			PeerInfo: func(id enode.ID) interface{} {
				p, ok := peers.Load(id.String())
				if !ok {
					return fmt.Errorf("peer has been penalized")
				}
				peer, _ := p.(*p2p.Peer)
				return peer.Info()
			},
			Attributes:     []enr.Entry{eth.CurrentENREntry(chainConfig, genesisHash, headHeight)},
			DialCandidates: dnsdisc,
		}
	}
	return protocols
}
