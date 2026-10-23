// - Encode：消息 ID + protobuf 二进制 → 组装成 `[4字节长度][2字节msgId][payload]` 的包（大端）
// - Decode：从字节缓冲区里不断读，切出完整包，处理半包、粘包
package common

import (
	"bytes"
	"encoding/binary"
)

type Packet struct {
	MessageID uint16
	Payload   []byte
}

// 编码
func Encode(MessageID uint16, Payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	//长度
	BodyLen := uint32(len(Payload))

	err := binary.Write(&buf, binary.BigEndian, BodyLen)
	if err != nil {
		return nil, err
	}
	//id
	err = binary.Write(&buf, binary.BigEndian, MessageID)
	if err != nil {
		return nil, err
	}
	//内容
	_, err = buf.Write(Payload)
	return buf.Bytes(), err
}

// 解码
// 数据不够时返回 nil, nil，且不消费缓冲区里的数据，等下次读到更多字节再解
func Decode(buf *bytes.Buffer) (*Packet, error) {
	if buf.Len() < 6 {
		return nil, nil
	}

	data := buf.Bytes()
	BodyLen := binary.BigEndian.Uint32(data[:4])
	total := 6 + int(BodyLen)
	//半包：长度头 + msgId + payload 还没收全
	if buf.Len() < total {
		return nil, nil
	}

	msgId := binary.BigEndian.Uint16(data[4:6])
	payload := make([]byte, BodyLen)
	copy(payload, data[6:total])
	//从缓冲区里删掉已消费的整个包，剩下的留给粘包的下一个包
	buf.Next(total)

	return &Packet{
		MessageID: msgId,
		Payload:   payload,
	}, nil
}
