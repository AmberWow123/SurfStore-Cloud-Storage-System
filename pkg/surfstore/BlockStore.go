package surfstore

import (
	context "context"
	"fmt"
	sync "sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

var mub sync.Mutex

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	mub.Lock()
	// defer mub.Unlock()

	// check if data exists
	if b, ok := bs.BlockMap[blockHash.Hash]; ok {
		mub.Unlock()
		return &Block{
			BlockData: b.BlockData,
			BlockSize: b.BlockSize,
		}, nil
	}
	mub.Unlock()
	return nil, fmt.Errorf("hash value doesn't exist")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	mub.Lock()
	// defer mub.Unlock()

	hash_value := GetBlockHashString(block.BlockData)
	if bs.BlockMap == nil {
		bs.BlockMap = make(map[string]*Block)
	}

	bs.BlockMap[hash_value] = block
	mub.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")

	mub.Lock()
	// defer mub.Unlock()

	input_hash_list := blockHashesIn.Hashes
	var output_hash_list []string

	for _, h := range input_hash_list {
		// check if hash value is in blockMap
		if _, ok := bs.BlockMap[h]; ok {
			// if corresponding block data exists
			output_hash_list = append(output_hash_list, h)
		}
	}
	mub.Unlock()
	return &BlockHashes{
		Hashes: output_hash_list,
	}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
