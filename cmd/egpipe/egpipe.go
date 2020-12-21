package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/google/uuid"
	"github.com/jmespath/go-jmespath"
)

func main() {
	var cli cliOptions
	k := kong.Parse(&cli)
	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()
	err := run(ctx, &cli, scanner)
	k.FatalIfErrorf(err)
}

type cliOptions struct {
	TopicHost     string            `kong:"arg,required"`
	Header        map[string]string `kong:"short=H"`
	ID            string            `kong:"short=i"`
	Subject       string            `kong:"required,short=s"`
	EventType     string            `kong:"required,short=t,name='type'"`
	EventTime     string            `kong:"name='timestamp',short=T,default='now'"`
	DataVersion   string            `kong:"default=1.0"`
	QueueSize     int               `kong:"default=10"`
	FlushInterval int               `kong:"default=2000,help='milliseconds between queue flushes'"`

	jmespaths map[string]*jmespath.JMESPath
	optDefs   map[string]string
}

const jmespathPrefix = "jp:"

type lineData struct {
	data  []byte
	iface interface{}
}

func (l lineData) unmarshalled() (interface{}, error) {
	if l.iface == nil {
		err := json.Unmarshal(l.data, &l.iface)
		if err != nil {
			return nil, err
		}
	}
	return l.iface, nil
}

func (c *cliOptions) url() (string, error) {
	th := c.TopicHost
	if !strings.Contains(th, `://`) {
		th = "https://" + th
	}
	pURL, err := url.Parse(th)
	if err != nil {
		return "", err
	}

	if pURL.Path == "" {
		pURL.Path = `api/events`
	}
	query := pURL.Query()
	if query.Get("api-version") == "" {
		query.Set("api-version", "2018-01-01")
	}
	pURL.RawQuery = query.Encode()

	return pURL.String(), nil
}

func run(ctx context.Context, cli *cliOptions, scanner *bufio.Scanner) error {
	header := http.Header{}
	if cli.Header != nil {
		for k, v := range cli.Header {
			header.Set(k, v)
		}
	}

	thURL, err := cli.url()
	if err != nil {
		return err
	}
	publisher := &eventGridPublisher{
		resetTicker:  func() {},
		maxQueueSize: cli.QueueSize,
		endpoint:     thURL,
		reqHeader:    header,
	}

	doneMutex := new(sync.Mutex)
	done := false
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		doneMutex.Lock()
		done = true
		doneMutex.Unlock()
	}()

	if cli.FlushInterval != 0 {
		interval := time.Duration(cli.FlushInterval) * time.Millisecond
		ticker := time.NewTicker(interval)
		publisher.resetTicker = func() {
			ticker.Reset(interval)
		}
		go func() {
			for range ticker.C {
				err2 := publisher.flushIfNeeded(ctx, 0)
				if err2 != nil {
					os.Exit(1)
				}
			}
		}()
	}

	for scanner.Scan() {
		b := scanner.Bytes()
		b = bytes.TrimSpace(b)
		if len(b) == 0 {
			continue
		}
		var ev *event
		ev, err = buildEvent(cli, scanner.Bytes())
		if err != nil {
			return err
		}
		err = publisher.addEvent(ctx, ev)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	err = publisher.flushIfNeeded(ctx, 0)
	if err != nil {
		return err
	}
	return scanner.Err()
}

func (c *cliOptions) jmespath(name, val string) (*jmespath.JMESPath, error) {
	var err error
	if !strings.HasPrefix(val, jmespathPrefix) {
		return nil, nil
	}
	if c.jmespaths == nil {
		c.jmespaths = map[string]*jmespath.JMESPath{}
	}
	if c.jmespaths[name] == nil {
		c.jmespaths[name], err = jmespath.Compile(strings.TrimPrefix(val, jmespathPrefix))
		if err != nil {
			return nil, err
		}
	}
	return c.jmespaths[name], nil
}

func (c *cliOptions) optDef(name string) string {
	if c.optDefs == nil {
		c.optDefs = map[string]string{
			"subject":     c.Subject,
			"id":          c.ID,
			"eventType":   c.EventType,
			"eventTime":   c.EventTime,
			"dataVersion": c.DataVersion,
		}
	}
	return c.optDefs[name]
}

func (c *cliOptions) getVal(valName string, data lineData) (string, error) {
	optDef := c.optDef(valName)

	if strings.HasPrefix(optDef, jmespathPrefix) {
		jp, err := c.jmespath(valName, optDef)
		if err != nil {
			return "", err
		}
		jd, err := data.unmarshalled()
		if err != nil {
			return "", err
		}
		return jmespathString(jp, jd)
	}
	return optDef, nil
}

func buildEvent(cli *cliOptions, data []byte) (*event, error) {
	ev := new(event)

	ld := lineData{
		data: data,
	}
	var err error
	ev.ID, err = cli.getVal("id", ld)
	if err != nil {
		return nil, err
	}
	if ev.ID == "" {
		ev.ID = uuid.New().String()
	}

	ev.Subject, err = cli.getVal("subject", ld)
	if err != nil {
		return nil, err
	}

	ev.DataVersion, err = cli.getVal("dataVersion", ld)
	if err != nil {
		return nil, err
	}

	ev.EventTime, err = cli.eventTime(ld)
	if err != nil {
		return nil, err
	}

	ev.EventType, err = cli.getVal("eventType", ld)
	if err != nil {
		return nil, err
	}

	ev.Data = json.RawMessage(data)

	return ev, nil
}

func (c *cliOptions) eventTime(ld lineData) (string, error) {
	strVal, err := c.getVal("eventTime", ld)
	if err != nil {
		return "", err
	}
	if strVal == "now" {
		return time.Now().UTC().Format(time.RFC3339Nano), nil
	}
	iVal, err := strconv.ParseInt(strVal, 10, 64)
	if err != nil {
		return "", err
	}
	secs := iVal / 1000
	ms := iVal % 1000
	ns := ms * int64(time.Millisecond)
	return time.Unix(secs, ns).UTC().Format(time.RFC3339Nano), nil
}

func jmespathString(jp *jmespath.JMESPath, data interface{}) (string, error) {
	got, err := jp.Search(data)
	if err != nil {
		return "", err
	}
	switch val := got.(type) {
	case string:
		return val, nil
	case float64:
		return fmt.Sprintf("%.0f", val), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

type eventGridPublisher struct {
	mutex        sync.Mutex
	endpoint     string
	httpClient   *http.Client
	reqHeader    http.Header
	maxQueueSize int
	cache        []*event
	resetTicker  func()
}

func (p *eventGridPublisher) addEvent(ctx context.Context, ev *event) error {
	p.mutex.Lock()
	p.cache = append(p.cache, ev)
	if len(p.cache) == 1 {
		p.resetTicker()
	}
	p.mutex.Unlock()
	return p.flushIfNeeded(ctx, p.maxQueueSize)
}

func (p *eventGridPublisher) flushIfNeeded(ctx context.Context, maxQueueSize int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if len(p.cache) == 0 || len(p.cache) < maxQueueSize {
		return nil
	}
	err := p.flush(ctx)
	if err != nil {
		return err
	}
	p.cache = p.cache[:0]
	return nil
}

func (p *eventGridPublisher) flush(ctx context.Context) error {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(p.cache)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, &buf)
	if err != nil {
		return err
	}
	req.Header = p.reqHeader
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	httpClient := p.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("not OK")
	}
	return nil
}

// event properties of an event published to an event Grid topic using the EventGrid Schema.
type event struct {
	// ID - An unique identifier for the event.
	ID string `json:"id,omitempty"`
	// Topic - The resource path of the event source.
	Topic string `json:"topic,omitempty"`
	// Subject - A resource path relative to the topic path.
	Subject string `json:"subject,omitempty"`
	// Data - event data specific to the event type.
	Data interface{} `json:"data,omitempty"`
	// EventType - The type of the event that occurred.
	EventType string `json:"eventType,omitempty"`
	// EventTime - The time (in UTC) the event was generated.
	EventTime string `json:"eventTime,omitempty"`
	// DataVersion - The schema version of the data object.
	DataVersion string `json:"dataVersion,omitempty"`
}
