package mysql

import (
	"fmt"
	"net"
)

type Packets struct {
	Sequence uint8
	conn     net.Conn
	buf      buffer
}

func NewPackets(conn net.Conn) *Packets {
	p := &Packets{conn: conn}
	p.buf = newBuffer(p.conn)
	return p

}

func (p *Packets) ReadPacket() ([]byte, error) {
	var payload []byte
	for {
		data, err := p.buf.readNext(4)
		if err != nil {
			return nil, ErrBadConn
		}
		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)
		if pktLen < 1 {
			return nil, fmt.Errorf("invalid payload length %d", pktLen)
		}
		if data[3] != p.Sequence {
			return nil, fmt.Errorf("invalid sequence %d != %d", data[3], p.Sequence)
		}
		p.Sequence++
		data, err = p.buf.readNext(pktLen)
		if err != nil {
			return nil, ErrBadConn
		}
		isLastPacket := (pktLen < MaxPayloadLen)
		if isLastPacket && payload == nil {
			return data, nil
		}

		payload = append(payload, data...)

		if isLastPacket {
			return payload, nil
		}

	}

}

func (p *Packets) WritePacket(data []byte) error {
	pktLen := len(data) - 4
	for {
		var size int
		if pktLen > MaxPayloadLen {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = MaxPayloadLen
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		data[3] = p.Sequence
		n, err := p.conn.Write(data[:4+size])
		if err == nil && n == 4+size {
			p.Sequence++
			if size != MaxPayloadLen {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

	}
	return ErrBadConn
}

func (p *Packets) TakeSmallBuffer(length int) []byte {
	return p.buf.takeSmallBuffer(length)
}
