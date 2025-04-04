package main

import (
	"bufio"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"path/filepath"
	"protos/gen/fileservice"
)

const (
	serverAddr   = "localhost:50051"
	downloadPath = "./downloads"
)

func main() {
	client, err := NewClient(serverAddr)
	if err != nil {
		fmt.Printf("failed to create client: %s\n", err)
		os.Exit(1)
	}
	defer client.Close()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\nChoose an operation:")
		fmt.Println("1. Upload file")
		fmt.Println("2. Download file")
		fmt.Println("3. List files")
		fmt.Println("4. Exit")
		fmt.Print("Enter your choice (1-4): ")

		scanner.Scan()
		choice := scanner.Text()

		switch choice {
		case "1":
			fmt.Print("Enter file path to upload: ")
			scanner.Scan()
			filePath := scanner.Text()

			if err := client.UploadFile(filePath); err != nil {
				fmt.Printf("upload failed: %s\n", err)
			}

		case "2":
			fmt.Print("Enter filename to download: ")
			scanner.Scan()
			filename := scanner.Text()

			if err := client.DownloadFile(filename); err != nil {
				fmt.Printf("download failed: %s\n", err)
			}

		case "3":
			if err := client.ListFiles(); err != nil {
				fmt.Printf("list files failed: %s\n", err)
			}

		case "4":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("Invalid choice. Please try again.")
		}
	}
}

type Client struct {
	conn   *grpc.ClientConn
	client fileservice.FileServiceClient
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v\n", err)
	}

	client := fileservice.NewFileServiceClient(conn)

	return &Client{
		conn:   conn,
		client: client,
	}, nil
}

func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		fmt.Printf("failed to close connection: %v\n", err)
	}
}

func (c *Client) UploadFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	stream, err := c.client.UploadFile(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create upload stream: %v", err)
	}

	// Send file info first
	filename := filepath.Base(filePath)
	if err := stream.Send(&fileservice.UploadRequest{
		Data: &fileservice.UploadRequest_Info{
			Info: &fileservice.FileInfo{Filename: filename},
		},
	}); err != nil {
		return fmt.Errorf("failed to send file info: %v", err)
	}

	buf := make([]byte, 1024*32) // 32KB chunks
	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read file chunk: %v", err)
		}

		if err := stream.Send(&fileservice.UploadRequest{
			Data: &fileservice.UploadRequest_Chunk{
				Chunk: buf[:n],
			},
		}); err != nil {
			return fmt.Errorf("failed to send file chunk: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to receive response: %v", err)
	}

	fmt.Printf("file '%v' uploaded successfully", resp.Filename)

	return nil
}

func (c *Client) DownloadFile(filename string) error {
	stream, err := c.client.DownloadFile(context.Background(), &fileservice.DownloadRequest{
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to create download stream: %v", err)
	}

	fp := filepath.Join(downloadPath, filename)

	if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	file, err := os.Create(fp)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk: %v", err)
		}

		if _, err := file.Write(resp.Chunk); err != nil {
			return fmt.Errorf("failed to write chunk: %v", err)
		}
	}

	fmt.Printf("file '%v' downloaded successfully", filename)

	return nil
}

func (c *Client) ListFiles() error {
	resp, err := c.client.ListFiles(context.Background(), &fileservice.ListRequest{})
	if err != nil {
		return fmt.Errorf("failed to list files: %v", err)
	}

	fmt.Println("Files on server:")
	fmt.Printf("%-30s | %-20s | %-20s\n", "Filename", "Created At", "Updated At")
	for _, file := range resp.Files {
		fmt.Printf("%-30s | %-20s | %-20s\n",
			file.Filename,
			file.CreatedAt,
			file.UpdatedAt)
	}

	return nil
}
