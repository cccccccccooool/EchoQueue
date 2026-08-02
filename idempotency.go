package echoqueue

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

type commandProjection struct {
	BatchID   string            `json:"batch_id"`
	RequestID string            `json:"request_id"`
	Results   []canonicalResult `json:"results"`
	Failures  []Failure         `json:"failures"`
}

type canonicalResult struct {
	TaskID string          `json:"task_id"`
	Data   json.RawMessage `json:"data"`
}

func commandHash(batchID string, outcome Outcome) (string, error) {
	results := make([]canonicalResult, 0, len(outcome.Results))
	for _, result := range outcome.Results {
		data, err := canonicalJSON(result.Data)
		if err != nil {
			return "", fmt.Errorf("result %q: %w", result.TaskID, err)
		}
		results = append(results, canonicalResult{TaskID: result.TaskID, Data: data})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].TaskID < results[j].TaskID })
	failures := append([]Failure(nil), outcome.Failures...)
	sort.Slice(failures, func(i, j int) bool { return failures[i].TaskID < failures[j].TaskID })
	projection := commandProjection{
		BatchID:   batchID,
		RequestID: outcome.RequestID,
		Results:   results,
		Failures:  failures,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func recoverCommandHash(batchID string) string {
	digest := sha256.Sum256([]byte(`{"operation":"recover","batch_id":"` + batchID + `"}`))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func effectID(operation, batchID, taskID string, retryCount int) string {
	value := fmt.Sprintf("%d\x00%s\x00%s\x00%s\x00%d", protocolVersion, operation, batchID, taskID, retryCount)
	digest := sha256.Sum256([]byte(value))
	return "v1:" + operation + ":sha256:" + hex.EncodeToString(digest[:])
}

func canonicalJSON(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}
