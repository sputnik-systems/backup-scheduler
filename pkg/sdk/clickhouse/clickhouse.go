package clickhouse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"

	"github.com/sputnik-systems/backup-scheduler/pkg/sdk"
)

const (
	createUrl = "/backup/create"
	uploadUrl = "/backup/upload"
	statusUrl = "/backup/status"
)

type ApiClient struct {
	url    *url.URL
	client *http.Client
	logger *log.Logger
}

func New(apiUrl string, l *log.Logger) (sdk.ApiClient, error) {
	u, err := url.Parse(apiUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clickhouse backuper api url: %s", err)
	}

	c := &http.Client{}

	return &ApiClient{url: u, client: c, logger: l}, nil
}

func (c *ApiClient) Create(ctx context.Context, name string) error {
	uri := *c.url
	uri.Path = path.Join(uri.Path, createUrl)

	args := map[string]string{
		"name": name,
	}

	_, err := c.request(ctx, http.MethodPost, uri.String(), "backup creating", args)

	return err
}

func (c *ApiClient) Status(ctx context.Context, name string) (string, error) {
	uri := *c.url
	uri.Path = path.Join(uri.Path, statusUrl)

	args := map[string]string{
		"name": name,
	}

	resp, err := c.request(ctx, http.MethodGet, uri.String(), "backups status", args)
	if err != nil {
		return "", fmt.Errorf("failed get clickhouse backups status: %s", err)
	}

	type bs struct {
		Command, Status string
	}

	b := bufio.NewScanner(resp.Body)
	defer resp.Body.Close()

	// iterate over each response row
	// get last backup status with given name
	var status string
	for b.Scan() {
		var s bs
		err = json.Unmarshal(b.Bytes(), &s)
		if err != nil {
			return "", fmt.Errorf("failed to unmarshal clickhouse backups status response body: %s", err)
		}

		if s.Command == fmt.Sprintf("create %s", name) {
			status = s.Status
		}
	}

	if err := b.Err(); err != nil {
		return "", fmt.Errorf("failed to read clickhouse backups status response body: %s", err)
	}

	if status == "" {
		return "", fmt.Errorf("given backup name not found in clickhouse backups status response")
	}

	return status, nil
}

func (c *ApiClient) Upload(ctx context.Context, name string) error {
	args := map[string]string{
		"name": name,
	}

	uri := *c.url
	uri.Path = path.Join(uri.Path, uploadUrl, name)

	_, err := c.request(ctx, http.MethodPost, uri.String(), "backup creating", args)

	return err
}

// request make different requests into backuper api
// meta var contain request type, e.g. "backup creating", "backups status"
func (c *ApiClient) request(ctx context.Context, method, uri, meta string, args map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, uri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to generate clickhouse %s request: %s", meta, err)
	}

	q := req.URL.Query()
	for key, value := range args {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make clickhouse %s request: %s", meta, err)
	}

	if resp.StatusCode != 200 {
		b, err := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read clieckhouse %s request body: %s", meta, err)
		}

		return nil, fmt.Errorf(
			"clickhouse %s request failed with status code %d and body: \"%s\"",
			meta, resp.StatusCode, string(b),
		)
	}

	c.logger.Printf("clickhouse %s request success", meta)

	return resp, nil
}
