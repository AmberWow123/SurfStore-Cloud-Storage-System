package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")

	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	suc, err := c.PutBlock(ctx, &Block{
		BlockData: block.BlockData,
		BlockSize: block.BlockSize,
	})
	if err != nil {
		conn.Close()
		return err
	}

	*succ = suc.Flag

	if suc.Flag != true {
		// not sure about here
		log.Println("[RPCClient] putblock not successful")
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")

	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hash_out, err := c.HasBlocks(ctx, &BlockHashes{
		Hashes: blockHashesIn,
	})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = hash_out.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")

	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileInfoM, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	*serverFileInfoMap = fileInfoM.FileInfoMap

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")

	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ver, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = ver.Version
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// panic("todo")

	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	*blockStoreAddr = addr.Addr

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
