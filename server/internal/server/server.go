package server

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log/slog"
	"net"
	"protos/gen/fileservice"
	"server/internal/config"
	"server/internal/service"
	"time"
)

type FileServer struct {
	fileservice.UnimplementedFileServiceServer
	fileService *service.FileService
	log         *slog.Logger
}

func NewFileServer(fileService *service.FileService, log *slog.Logger) *FileServer {
	return &FileServer{
		fileService: fileService,
		log:         log,
	}
}

func Start(cfg *config.Config, log *slog.Logger) error {
	fileService, err := service.New(
		cfg.UploadDir,
		int64(cfg.Limits.Upload),
		int64(cfg.Limits.Download),
		int64(cfg.Limits.List),
		log,
	)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	fileServer := NewFileServer(fileService, log)
	fileservice.RegisterFileServiceServer(grpcServer, fileServer)

	log.Info("server is running", "port", cfg.Port)

	return grpcServer.Serve(lis)
}

func (s *FileServer) UploadFile(stream fileservice.FileService_UploadFileServer) error {
	req, err := stream.Recv()
	if err != nil {
		s.log.Error("failed to receive file info", "error", err)
		return err
	}

	info := req.GetInfo()
	if info == nil {
		s.log.Error("invalid first message, expected file info")
		return io.ErrUnexpectedEOF
	}

	filename := info.Filename
	if filename == "" {
		s.log.Error("empty filename")
		return io.ErrUnexpectedEOF
	}

	pr, pw := io.Pipe()
	defer pr.Close()

	go func() {
		defer pw.Close()
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				s.log.Error("failed to receive chunk", "error", err)
				pw.CloseWithError(err)
				return
			}

			chunk := req.GetChunk()
			if chunk == nil {
				s.log.Error("invalid message, expected chunk")
				pw.CloseWithError(io.ErrUnexpectedEOF)
				return
			}

			if _, err := pw.Write(chunk); err != nil {
				s.log.Error("failed to write chunk", "error", err)
				pw.CloseWithError(err)
				return
			}
		}
	}()

	if err := s.fileService.UploadFile(stream.Context(), filename, pr); err != nil {
		return err
	}

	if err := stream.SendAndClose(&fileservice.UploadResponse{
		Filename: filename,
	}); err != nil {
		s.log.Error("failed to send response", "error", err)
		return err
	}

	s.log.Info("file uploaded successfully", "filename", filename)
	return nil
}

func (s *FileServer) DownloadFile(
	req *fileservice.DownloadRequest,
	stream fileservice.FileService_DownloadFileServer,
) error {

	filename := req.Filename
	if filename == "" {
		s.log.Error("empty filename")
		return io.ErrUnexpectedEOF
	}

	file, err := s.fileService.DownloadFile(stream.Context(), filename)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 1024*32) // 32KB chunks
	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			s.log.Error("failed to read file", "error", err, "filename", filename)
			return err
		}

		if err := stream.Send(&fileservice.DownloadResponse{
			Chunk: buf[:n],
		}); err != nil {
			s.log.Error("failed to send chunk", "error", err, "filename", filename)
			return err
		}
	}

	s.log.Info("file downloaded successfully", "filename", filename)

	return nil
}

func (s *FileServer) ListFiles(
	ctx context.Context,
	req *fileservice.ListRequest,
) (*fileservice.ListResponse, error) {

	files, err := s.fileService.ListFiles(ctx)
	if err != nil {
		return nil, err
	}

	response := &fileservice.ListResponse{}
	for _, file := range files {
		response.Files = append(response.Files, &fileservice.File{
			Filename:  file.Filename,
			CreatedAt: file.CreatedAt.Format(time.RFC3339),
			UpdatedAt: file.UpdatedAt.Format(time.RFC3339),
		})
	}

	s.log.Info("listed files", "count", len(files))
	return response, nil
}
