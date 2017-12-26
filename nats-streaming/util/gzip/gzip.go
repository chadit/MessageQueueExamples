// Package gzip provides utility functions for compressing and decompressing
// data. There are functions for compressing byte slices and arbitrary JSON
// using gzip compression.
package gzip

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
)

// InflateGZip uses gzip to compress data.
func InflateGZip(v []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	z, err := gzip.NewWriterLevel(b, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err = z.Write(v); err != nil {
		z.Close()
		return nil, err
	}

	err = z.Close()
	return b.Bytes(), err
}

// DeflateGZip uses gzip to decompress data.
func DeflateGZip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(ioutil.NopCloser(bytes.NewBuffer(data)))
	defer r.Close()
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(r)
}

// Compress marshals the given data into JSON and compresses it.
func Compress(v interface{}) ([]byte, error) {
	m, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return InflateGZip(m)
}

// Decompress uses gzip to decompress the given data and unmarshalls it into provided parameter v.
func Decompress(data []byte, v interface{}) error {
	b, err := DeflateGZip(data)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}

// CompressXML marshals the given data into XML and compresses it.
func CompressXML(v interface{}) ([]byte, error) {
	m, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return InflateGZip(m)
}

// DecompressXML uses gzip to decompress the given data and unmarshalls it into provided parameter v.
func DecompressXML(data []byte, v interface{}) error {
	b, err := DeflateGZip(data)
	if err != nil {
		return err
	}

	return xml.Unmarshal(b, v)
}
