package network

import (
	"context"

	"github.com/AliSyed2006/distributed-kv/api/proto"
	"github.com/AliSyed2006/distributed-kv/internal/storage"
)

// Server implements the KVService gRPC server.
type Server struct {
	proto.UnimplementedKVServiceServer
	engine *storage.StorageEngine
}

// NewServer creates a new gRPC server wrapping the given storage engine.
func NewServer(engine *storage.StorageEngine) *Server {
	return &Server{
		engine: engine,
	}
}

// Put maps the gRPC Put call to the storage engine.
func (s *Server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	err := s.engine.Put(req.Key, req.Value)
	if err != nil {
		return &proto.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &proto.PutResponse{Success: true}, nil
}

// Get maps the gRPC Get call to the storage engine.
func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	val, ok := s.engine.Get(req.Key)
	if !ok {
		return &proto.GetResponse{
			Found: false,
		}, nil
	}
	return &proto.GetResponse{
		Value: val,
		Found: true,
	}, nil
}

// Delete maps the gRPC Delete call to the storage engine.
func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	err := s.engine.Delete(req.Key)
	if err != nil {
		return &proto.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &proto.DeleteResponse{Success: true}, nil
}

// Stats maps the gRPC Stats call to the storage engine.
func (s *Server) Stats(ctx context.Context, req *proto.StatsRequest) (*proto.StatsResponse, error) {
	stats := s.engine.Stats()
	return &proto.StatsResponse{
		MemTableSize: int64(stats.MemTableSize),
		SstableCount: int32(stats.SSTableCount),
		MaxMemSize:   int64(stats.MaxMemSize),
	}, nil
}
