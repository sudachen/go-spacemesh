package types

import (
	"bytes"
	"encoding/gob"
	"github.com/spacemeshos/go-spacemesh/common/util"
)

func (id BlockID) ToBytes() []byte { return id.AsHash32().Bytes() }

func (l LayerID) ToBytes() []byte { return util.Uint64ToBytes(uint64(l)) }

func BlockIdsAsBytes(ids []BlockID) ([]byte, error) {
	SortBlockIds(ids)
	return InterfaceToBytes(ids)
}

func BytesToBlockIds(blockIds []byte) ([]BlockID, error) {
	var ids []BlockID
	err := BytesToInterface(blockIds, &ids)
	return ids, err
}

func BytesAsAtx(b []byte) (*ActivationTx, error) {
	var atx ActivationTx
	err := BytesToInterface(b, atx)
	return &atx, err
}

func TxIdsAsBytes(ids []TransactionId) ([]byte, error) {
	return InterfaceToBytes(ids)
}

func NIPSTChallengeAsBytes(challenge *NIPSTChallenge) ([]byte, error) {
	return InterfaceToBytes(challenge)
}

func BytesAsTransaction(buf []byte) (*Transaction, error) {
	b := Transaction{}
	err := BytesToInterface(buf, b)
	return &b, err
}

// ⚠️ Pass the interface by reference
func BytesToInterface(buf []byte, i interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(buf)) // Will read from network.
	return dec.Decode(i)
}

// ⚠️ Pass the interface by reference
func InterfaceToBytes(i interface{}) ([]byte, error) {
	var w bytes.Buffer
	enc := gob.NewEncoder(&w)
	err := enc.Encode(i)
	return w.Bytes(), err
}
