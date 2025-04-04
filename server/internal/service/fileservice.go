package service

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type FileMetadata struct {
	Filename  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type FileService struct {
	uploadDir    string
	uploadSem    *semaphore.Weighted
	downloadSem  *semaphore.Weighted
	listSem      *semaphore.Weighted
	metadata     map[string]FileMetadata
	metadataLock sync.RWMutex
	log          *slog.Logger
}

func New(
	uploadDir string,
	uploadLimit, downloadLimit, listLimit int64,
	log *slog.Logger,
) (*FileService, error) {

	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return nil, err
	}

	fs := &FileService{
		uploadDir:    uploadDir,
		uploadSem:    semaphore.NewWeighted(uploadLimit),
		downloadSem:  semaphore.NewWeighted(downloadLimit),
		listSem:      semaphore.NewWeighted(listLimit),
		metadata:     make(map[string]FileMetadata),
		metadataLock: sync.RWMutex{},
		log:          log,
	}

	if err := fs.loadExistingFiles(); err != nil {
		return nil, err
	}

	return fs, nil
}

func (fs *FileService) loadExistingFiles() error {
	files, err := os.ReadDir(fs.uploadDir)
	if err != nil {
		fs.log.Error("failed to read upload directory", "error", err)
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		info, err := file.Info()
		if err != nil {
			fs.log.Error("failed to get file info", "error", err, "filename", file.Name())
			continue
		}

		fs.metadata[file.Name()] = FileMetadata{
			Filename:  file.Name(),
			CreatedAt: info.ModTime(),
			UpdatedAt: info.ModTime(),
		}
	}

	return nil
}

type semaphoreReadCloser struct {
	io.ReadCloser
	sem *semaphore.Weighted
}

func (src *semaphoreReadCloser) Close() error {
	defer src.sem.Release(1)
	return src.ReadCloser.Close()
}

func (fs *FileService) UploadFile(ctx context.Context, filename string, data io.Reader) error {
	if err := fs.uploadSem.Acquire(ctx, 1); err != nil {
		fs.log.Info("upload maximum connections reached")
		return err
	}
	defer fs.uploadSem.Release(1)

	fp := filepath.Join(fs.uploadDir, filename)
	file, err := os.Create(fp)
	if err != nil {
		fs.log.Error("failed to create file", "error", err)
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, data); err != nil {
		fs.log.Error("failed to write file", "error", err)
		return err
	}

	now := time.Now()
	fs.metadataLock.Lock()
	defer fs.metadataLock.Unlock()
	fs.metadata[filename] = FileMetadata{
		Filename:  filename,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return nil
}

func (fs *FileService) DownloadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if err := fs.downloadSem.Acquire(ctx, 1); err != nil {
		fs.log.Info("download maximum connections reached")
		return nil, err
	}

	filePath := filepath.Join(fs.uploadDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		fs.downloadSem.Release(1)
		fs.log.Error("failed to open file", "error", err)
		return nil, err
	}

	return &semaphoreReadCloser{
		ReadCloser: file,
		sem:        fs.downloadSem,
	}, nil
}

func (fs *FileService) ListFiles(ctx context.Context) ([]FileMetadata, error) {
	if err := fs.listSem.Acquire(ctx, 1); err != nil {
		fs.log.Info("list files maximum connections reached")
		return nil, err
	}
	defer fs.listSem.Release(1)

	fs.metadataLock.RLock()
	defer fs.metadataLock.RUnlock()

	files := make([]FileMetadata, 0, len(fs.metadata))
	for _, meta := range fs.metadata {
		files = append(files, meta)
	}

	return files, nil
}
