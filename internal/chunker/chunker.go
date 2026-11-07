package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
)

const ChunkSize int64 = 4 * 1024 * 1024 // 4 MiB

type ChunkData struct {
	Metadata models.Chunk
	Data     []byte
}

func Process(
	reader io.Reader,
	fileID string,
	processChunk func(ChunkData) error,
) error {

	if reader == nil {
		return errors.New("reader cannot be nil")
	}

	if fileID == "" {
		return errors.New("file ID cannot be empty")
	}

	if processChunk == nil {
		return errors.New("chunk processor cannot be nil")
	}

	buffer := make([]byte, ChunkSize)

	var index int

	for {
		n, err := io.ReadFull(reader, buffer)

		if n > 0 {
			data := make([]byte, n)
			copy(data, buffer[:n])

			hash := sha256.Sum256(data)

			chunk := ChunkData{
				Metadata: models.Chunk{
					FileID:    fileID,
					Index:     index,
					Size:      int64(n),
					Hash:      hex.EncodeToString(hash[:]),
					Status:    models.ChunkStatusPending,
					CreatedAt: time.Now().UTC(),
				},
				Data: data,
			}

			if processErr := processChunk(chunk); processErr != nil {
				return processErr
			}

			index++
		}

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}

		if err != nil {
			return err
		}
	}

	return nil
}
