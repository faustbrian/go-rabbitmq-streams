package stream

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
)

const (
	None   = byte(0)
	GZIP   = byte(1)
	SNAPPY = byte(2)
	LZ4    = byte(3)
	ZSTD   = byte(4)
)

type Compression struct {
	enabled bool
	value   byte
}

func (compression Compression) String() string {
	return fmt.Sprintf("value %d (enable: %t)", compression.value,
		compression.enabled)
}

func (compression Compression) None() Compression {
	return Compression{value: None, enabled: true}
}

func (compression Compression) Gzip() Compression {
	return Compression{value: GZIP, enabled: true}
}

func (compression Compression) Snappy() Compression {
	return Compression{value: SNAPPY, enabled: true}
}

func (compression Compression) Zstd() Compression {
	return Compression{value: ZSTD, enabled: true}
}

func (compression Compression) Lz4() Compression {
	return Compression{value: LZ4, enabled: true}
}

type subEntry struct {
	messages     []*messageSequence
	publishingId int64 // need to store the publishingId useful in case of aggregation

	unCompressedSize int
	sizeInBytes      int
	dataInBytes      []byte
}
type subEntries struct {
	items            []*subEntry
	totalSizeInBytes int
}

type iCompress interface {
	Compress(subEntries *subEntries) error
	UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error)
}

func compressByValue(value byte) iCompress {
	switch value {
	case GZIP:
		return compressGZIP{}
	case SNAPPY:
		return compressSnappy{}
	case LZ4:
		return compressLZ4{}
	case ZSTD:
		return compressZSTD{}
	}

	return compressNONE{}
}

type compressNONE struct{}

func (es compressNONE) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		for _, msg := range entry.messages {
			if err := writeInt(&tmp, len(msg.messageBytes)); err != nil {
				return err
			}
			tmp.Write(msg.messageBytes)
		}
		entry.dataInBytes = tmp.Bytes()
		entry.sizeInBytes += len(entry.dataInBytes)
		subEntries.totalSizeInBytes += len(entry.dataInBytes)
	}

	return nil
}

func (es compressNONE) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	limits := DefaultDecoderLimits()
	if int64(dataSize) > int64(limits.MaxFrameBytes) || int64(uncompressedDataSize) > int64(limits.MaxChunkBytes) {
		return nil, ErrDecoderLimit
	}
	data := make([]byte, int(dataSize))
	if _, err := io.ReadFull(source, data); err != nil {
		return nil, err
	}
	decoded, err := decodeCompressed(data, None, int(uncompressedDataSize), limits.MaxChunkBytes)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(bytes.NewReader(decoded)), nil
}

type compressGZIP struct {
}

func (es compressGZIP) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := gzip.NewWriter(&tmp)

		for _, msg := range entry.messages {
			prefixedMsg, err := bytesLengthPrefixed(msg.messageBytes)
			if err != nil {
				return err
			}
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to gzip writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush gzip writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close gzip writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressGZIP) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	limits := DefaultDecoderLimits()
	if int64(dataSize) > int64(limits.MaxFrameBytes) || int64(uncompressedDataSize) > int64(limits.MaxChunkBytes) {
		return nil, ErrDecoderLimit
	}
	data := make([]byte, int(dataSize))
	if _, err := io.ReadFull(source, data); err != nil {
		return nil, err
	}
	decoded, err := decodeCompressed(data, GZIP, int(uncompressedDataSize), limits.MaxChunkBytes)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(bytes.NewReader(decoded)), nil
}

type compressSnappy struct{}

func (es compressSnappy) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := snappy.NewBufferedWriter(&tmp)
		for _, msg := range entry.messages {
			prefixedMsg, err := bytesLengthPrefixed(msg.messageBytes)
			if err != nil {
				return err
			}
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to snappy writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush snappy writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close snappy writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressSnappy) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	limits := DefaultDecoderLimits()
	if int64(dataSize) > int64(limits.MaxFrameBytes) || int64(uncompressedDataSize) > int64(limits.MaxChunkBytes) {
		return nil, ErrDecoderLimit
	}
	data := make([]byte, int(dataSize))
	if _, err := io.ReadFull(source, data); err != nil {
		return nil, err
	}
	decoded, err := decodeCompressed(data, SNAPPY, int(uncompressedDataSize), limits.MaxChunkBytes)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(bytes.NewReader(decoded)), nil
}

type compressLZ4 struct{}

func (es compressLZ4) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := lz4.NewWriter(&tmp)

		for _, msg := range entry.messages {
			prefixedMsg, err := bytesLengthPrefixed(msg.messageBytes)
			if err != nil {
				return err
			}
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to LZ4 writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush LZ4 writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close LZ4 writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressLZ4) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	limits := DefaultDecoderLimits()
	if int64(dataSize) > int64(limits.MaxFrameBytes) || int64(uncompressedDataSize) > int64(limits.MaxChunkBytes) {
		return nil, ErrDecoderLimit
	}
	data := make([]byte, int(dataSize))
	if _, err := io.ReadFull(source, data); err != nil {
		return nil, err
	}
	decoded, err := decodeCompressed(data, LZ4, int(uncompressedDataSize), limits.MaxChunkBytes)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(bytes.NewReader(decoded)), nil
}

type compressZSTD struct{}

func (es compressZSTD) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w, err := zstd.NewWriter(&tmp)
		if err != nil {
			return fmt.Errorf("error creating ZSTD compression algorithm writer %w", err)
		}

		for _, msg := range entry.messages {
			prefixedMsg, err := bytesLengthPrefixed(msg.messageBytes)
			if err != nil {
				return err
			}
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to ZSTD writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush ZSTD writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close ZSTD writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressZSTD) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	limits := DefaultDecoderLimits()
	if int64(dataSize) > int64(limits.MaxFrameBytes) || int64(uncompressedDataSize) > int64(limits.MaxChunkBytes) {
		return nil, ErrDecoderLimit
	}
	data := make([]byte, int(dataSize))
	if _, err := io.ReadFull(source, data); err != nil {
		return nil, err
	}
	decoded, err := decodeCompressed(data, ZSTD, int(uncompressedDataSize), limits.MaxChunkBytes)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(bytes.NewReader(decoded)), nil
}

// decodeCompressed bounds actual codec output independently of the peer's
// advertised size. Codec windows are finite as well as the returned byte slice.
func decodeCompressed(data []byte, codec byte, expected, limit int) ([]byte, error) {
	if expected < 0 || expected > limit || limit <= 0 {
		return nil, ErrDecoderLimit
	}
	var reader io.Reader
	switch codec {
	case None:
		reader = bytes.NewReader(data)
	case GZIP:
		gz, e := gzip.NewReader(bytes.NewReader(data))
		if e != nil {
			return nil, e
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	case SNAPPY:
		reader = snappy.NewReader(bytes.NewReader(data))
	case LZ4:
		reader = lz4.NewReader(bytes.NewReader(data))
	case ZSTD:
		memory := limit
		if memory < 1<<20 {
			memory = 1 << 20
		}
		z, e := zstd.NewReader(bytes.NewReader(data), zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(uint64(memory)), zstd.WithDecoderMaxWindow(uint64(memory)))
		if e != nil {
			return nil, e
		}
		defer z.Close()
		reader = z
	default:
		return nil, fmt.Errorf("unsupported chunk compression")
	}
	decoded, e := finiteDecodedReader(reader, limit)
	if e != nil {
		return nil, e
	}
	if len(decoded) != expected {
		return nil, fmt.Errorf("chunk decoded size mismatch")
	}
	return decoded, nil
}
