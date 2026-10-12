package dashboard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"

	baqueue "github.com/basalam/baqueue-golang"
)

type jobQuery struct {
	filter        baqueue.JobFilter
	page, perPage int
}

func invalid(message string) error { return &httpError{422, message} }

func parseDates(values url.Values) (*float64, *float64, error) {
	from, err := optionalFloat(values, "created_from")
	if err != nil {
		return nil, nil, err
	}
	to, err := optionalFloat(values, "created_to")
	if err != nil {
		return nil, nil, err
	}
	if from != nil && to != nil && *from > *to {
		return nil, nil, invalid("created_from must not exceed created_to")
	}
	return from, to, nil
}

func optionalFloat(values url.Values, key string) (*float64, error) {
	raw := values.Get(key)
	if raw == "" {
		return nil, nil
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return nil, invalid(key + " must be a finite number")
	}
	return &n, nil
}

func parseFilter(values url.Values) (baqueue.JobFilter, error) {
	f := baqueue.JobFilter{Queue: values.Get("queue"), Status: values.Get("status"), Tag: values.Get("tag"), BatchID: values.Get("batch_id")}
	if err := validateStatus(f.Status); err != nil {
		return f, err
	}
	var err error
	f.CreatedFrom, f.CreatedTo, err = parseDates(values)
	return f, err
}

func validateStatus(status string) error {
	switch status {
	case "", baqueue.Pending, baqueue.Processing, baqueue.Completed, baqueue.Failed, baqueue.Cancelled:
		return nil
	}
	return invalid("Invalid job status")
}

func parseJobs(values url.Values) (jobQuery, error) {
	q := jobQuery{page: 1, perPage: 25}
	var err error
	q.filter, err = parseFilter(values)
	if err != nil {
		return q, err
	}
	for _, field := range []struct {
		key   string
		value *int
	}{{"page", &q.page}, {"per_page", &q.perPage}} {
		if raw, ok := values[field.key]; ok {
			if len(raw) == 0 {
				return q, invalid(field.key + " must be a positive integer")
			}
			*field.value, err = strconv.Atoi(raw[0])
			if err != nil || *field.value < 1 {
				return q, invalid(field.key + " must be a positive integer")
			}
		}
	}
	if q.perPage > 1000 {
		return q, invalid("per_page must not exceed 1000")
	}
	if q.page-1 > math.MaxInt/q.perPage {
		return q, invalid("page is too large")
	}
	q.filter.Offset, q.filter.Limit = (q.page-1)*q.perPage, q.perPage
	return q, nil
}

// A subscription is a partial update; null clears optional filters but leaves
// pagination unchanged, matching the original frontend's subscription messages.
func subscription(data []byte, current url.Values) (url.Values, bool, error) {
	var message struct {
		Type    string                     `json:"type"`
		Filters map[string]json.RawMessage `json:"filters"`
	}
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, false, invalid("Invalid subscription JSON")
	}
	if message.Type != "subscribe" {
		return current, false, nil
	}
	next := make(url.Values, len(current))
	for k, v := range current {
		next[k] = append([]string(nil), v...)
	}
	for _, key := range []string{"queue", "status", "tag", "batch_id", "page", "per_page", "created_from", "created_to"} {
		raw, ok := message.Filters[key]
		if !ok {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			if key != "page" && key != "per_page" {
				next.Del(key)
			}
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			if key == "queue" || key == "status" || key == "tag" || key == "batch_id" {
				return nil, false, invalid(key + " must be a string")
			}
			var number json.Number
			if err := json.Unmarshal(raw, &number); err != nil {
				return nil, false, invalid(fmt.Sprintf("Invalid %s", key))
			}
			value = number.String()
		}
		next.Set(key, value)
	}
	if _, err := parseJobs(next); err != nil {
		return nil, false, err
	}
	return next, true, nil
}
