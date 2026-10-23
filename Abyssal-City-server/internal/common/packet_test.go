package common

import (
	"bytes"
	"testing"
)

func TestDecode_Normalpacket(t *testing.T) {
	payload := []byte("hello game")
	pkt, err := Encode(1001, payload)
	if err != nil {
		t.Fatalf("encode err: %v", err)
	}
	buf := bytes.NewBuffer(pkt)
	p, err := Decode(buf)
	if err != nil {
		t.Fatalf("decode err: %v", err)
	}
	if p.MessageID != 1001 {
		t.Errorf("数据不存在")
	}
	if !bytes.Equal(p.Payload, payload) {
		t.Errorf("data not match")
	}
}

func TestDecode_HalfPacket(t *testing.T) {
	payload := []byte("test half")
	pkt, err := Encode(1002, payload)
	if err != nil {
		t.Fatalf("encode err: %v", err)
	}

	// 只发前 3 个字节（半个包），Decode 应该返回 nil, nil，且不吃掉数据
	buf := bytes.NewBuffer(pkt[:3])
	p, err := Decode(buf)
	if err != nil {
		t.Fatalf("decode err: %v", err)
	}
	if p != nil {
		t.Errorf("半包应该解不出数据")
	}
	if buf.Len() != 3 {
		t.Errorf("半包数据应该保留在缓冲区")
	}

	// 把剩下的字节补上，应该能完整解出来
	buf.Write(pkt[3:])
	p, err = Decode(buf)
	if err != nil {
		t.Fatalf("decode err: %v", err)
	}
	if p == nil {
		t.Fatal("数据不存在")
	}
	if p.MessageID != 1002 {
		t.Errorf("want msgid 1002, got %d", p.MessageID)
	}
	if !bytes.Equal(p.Payload, payload) {
		t.Errorf("data not match")
	}
}

func TestDecode_StickyPacket(t *testing.T) {
	b1, _ := Encode(2001, []byte("msg1"))
	b2, _ := Encode(2002, []byte("msg222"))
	// 粘包：两个包拼在一起
	stick := append(b1, b2...)

	buf := bytes.NewBuffer(stick)
	// 拆第一个包
	p1, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if p1 == nil || p1.MessageID != 2001 || !bytes.Equal(p1.Payload, []byte("msg1")) {
		t.Error("first packet wrong")
	}
	// 用剩下的buf继续拆第二个包
	p2, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if p2 == nil || p2.MessageID != 2002 || !bytes.Equal(p2.Payload, []byte("msg222")) {
		t.Error("second packet wrong")
	}
	if buf.Len() != 0 {
		t.Error("remain not empty")
	}
}
