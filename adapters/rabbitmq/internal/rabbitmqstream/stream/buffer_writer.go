package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

func writeLong(inputBuff *bytes.Buffer, value int64) {
	writeULong(inputBuff, uint64(value)) // #nosec G115 -- Same-width signed wire bit representation, including negative offsets.
}

func writeULong(inputBuff *bytes.Buffer, value uint64) {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	inputBuff.Write(buff)
}

func writeBLong(inputBuff *bufio.Writer, value int64) error {
	return writeBULong(inputBuff, uint64(value)) // #nosec G115 -- Same-width signed publishing-ID wire bits.
}

func writeBULong(inputBuff *bufio.Writer, value uint64) error {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	_, err := inputBuff.Write(buff)
	return err
}

func writeShort(inputBuff *bytes.Buffer, value int16) {
	writeUShort(inputBuff, uint16(value)) // #nosec G115 -- Same-width protocol short bit representation.
}

func writeUShort(inputBuff *bytes.Buffer, value uint16) {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	inputBuff.Write(buff)
}

func writeBShort(inputBuff *bufio.Writer, value int16) error {
	return writeBUShort(inputBuff, uint16(value)) // #nosec G115 -- Same-width protocol short bit representation.
}
func writeBUShort(inputBuff *bufio.Writer, value uint16) error {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	_, err := inputBuff.Write(buff)
	return err
}

var ErrProtocolStringTooLong = errors.New("protocol string exceeds uint16 wire length")

func validateProtocolString(value string) error {
	if len(value) > math.MaxUint16 {
		return ErrProtocolStringTooLong
	}
	return nil
}

func validateProtocolStrings(values ...string) error {
	for _, value := range values {
		if err := validateProtocolString(value); err != nil {
			return err
		}
	}
	return nil
}

func writeBString(inputBuff *bufio.Writer, value string) error {
	if err := validateProtocolString(value); err != nil {
		return err
	}
	err := writeBUShort(inputBuff, uint16(len(value))) // #nosec G115 -- validateProtocolString rejected lengths above MaxUint16 before mutation.
	if err != nil {
		return err
	}

	_, err = inputBuff.Write([]byte(value))
	return err
}

func writeByte(inputBuff *bytes.Buffer, value byte) {
	var buff = make([]byte, 1)
	buff[0] = value
	inputBuff.Write(buff)
}

func writeBByte(inputBuff *bufio.Writer, value byte) error {
	var buff = make([]byte, 1)
	buff[0] = value
	_, err := inputBuff.Write(buff)
	return err
}

var ErrProtocolNumberRange = errors.New("protocol count or byte length outside uint32 range")

func validateProtocolCount(value int) error {
	if value < 0 || int64(value) > math.MaxUint32 {
		return ErrProtocolNumberRange
	}
	return nil
}

func writeInt(inputBuff *bytes.Buffer, value int) error {
	if err := validateProtocolCount(value); err != nil {
		return err
	}
	writeUInt(inputBuff, uint32(value)) // #nosec G115 -- validateProtocolCount admitted only 0..MaxUint32.
	return nil
}
func writeUInt(inputBuff *bytes.Buffer, value uint32) {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	inputBuff.Write(buff)
}

func writeBInt(inputBuff *bufio.Writer, value int) error {
	if err := validateProtocolCount(value); err != nil {
		return err
	}
	return writeBUInt(inputBuff, uint32(value)) // #nosec G115 -- validateProtocolCount admitted only 0..MaxUint32.
}

func writeBUInt(inputBuff *bufio.Writer, value uint32) error {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	_, err := inputBuff.Write(buff)
	return err
}

func writeString(inputBuff *bytes.Buffer, value string) error {
	if err := validateProtocolString(value); err != nil {
		return err
	}
	writeUShort(inputBuff, uint16(len(value))) // #nosec G115 -- validateProtocolString rejected lengths above MaxUint16 before mutation.
	inputBuff.WriteString(value)
	return nil
}

func writeStringArray(inputBuff *bytes.Buffer, array []string) error {
	if err := validateProtocolCount(len(array)); err != nil {
		return err
	}
	for _, value := range array {
		if err := validateProtocolString(value); err != nil {
			return err
		}
	}
	if err := writeInt(inputBuff, len(array)); err != nil {
		return err
	}
	for _, value := range array {
		if err := writeString(inputBuff, value); err != nil {
			return err
		}
	}
	return nil
}

func writeMapStringString(inputBuff *bytes.Buffer, values map[string]string) error {
	if err := validateProtocolCount(len(values)); err != nil {
		return err
	}
	for key, value := range values {
		if err := validateProtocolString(key); err != nil {
			return err
		}
		if err := validateProtocolString(value); err != nil {
			return err
		}
	}
	if err := writeInt(inputBuff, len(values)); err != nil {
		return err
	}
	for key, value := range values {
		if err := writeString(inputBuff, key); err != nil {
			return err
		}
		if err := writeString(inputBuff, value); err != nil {
			return err
		}
	}
	return nil
}

// writeProtocolHeader protocol utils functions
func writeProtocolHeader(inputBuff *bytes.Buffer,
	length int, command uint16,
	correlationId ...uint32) error {
	if err := writeInt(inputBuff, length); err != nil {
		return err
	}
	writeUShort(inputBuff, command)
	writeShort(inputBuff, version1)
	if len(correlationId) > 0 {
		writeUInt(inputBuff, correlationId[0])
	}
	return nil
}

func writeBProtocolHeader(inputBuff *bufio.Writer,
	length int, command int16,
	correlationId ...uint32) error {
	return writeBProtocolHeaderVersion(inputBuff, length, command, version1, correlationId...)
}

func writeBProtocolHeaderVersion(inputBuff *bufio.Writer, length int, command int16,
	version int16, correlationId ...uint32) error {
	if err := writeBInt(inputBuff, length); err != nil {
		return err
	}
	if err := writeBShort(inputBuff, command); err != nil {
		return err
	}
	if err := writeBShort(inputBuff, version); err != nil {
		return err
	}

	if len(correlationId) > 0 {
		if err := writeBUInt(inputBuff, correlationId[0]); err != nil {
			return err
		}
	}

	return nil
}

func sizeOfStringArray(array []string) int {
	size := 0
	for _, s := range array {
		size += 2 + len(s)
	}
	return size
}

func sizeOfMapStringString(mapString map[string]string) int {
	size := 0
	for k, v := range mapString {
		size += 2 + len(k) + 2 + len(v)
	}
	return size
}

func bytesLengthPrefixed(msg []byte) ([]byte, error) {
	size := len(msg)
	if err := validateProtocolCount(size); err != nil {
		return nil, err
	}
	if size > int(^uint(0)>>1)-4 {
		return nil, ErrProtocolNumberRange
	}
	buff := make([]byte, 4+size)

	binary.BigEndian.PutUint32(buff, uint32(size)) // #nosec G115 -- validateProtocolCount admitted size before allocation.
	copy(buff[4:], msg)

	return buff, nil
}

// newProtocolBuffer admits the frame length and allocation sum before allocation.
func newProtocolBuffer(length int) (*bytes.Buffer, error) {
	if err := validateProtocolCount(length); err != nil {
		return nil, err
	}
	if length < 4 || length > int(^uint(0)>>1)-4 {
		return nil, ErrProtocolNumberRange
	}
	return bytes.NewBuffer(make([]byte, 0, length+4)), nil
}
