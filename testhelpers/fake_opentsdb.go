package testhelpers

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
)

type FakeOpenTSDB struct {
	server           *httptest.Server
	ReceivedContents chan []byte
}

func NewFakeOpenTSDB() *FakeOpenTSDB {
	return &FakeOpenTSDB{
		ReceivedContents: make(chan []byte, 100),
	}
}

func (f *FakeOpenTSDB) Start() {
	f.server = httptest.NewUnstartedServer(f)
	f.server.Start()
}

func (f *FakeOpenTSDB) Close() {
	f.server.Close()
}

func (f *FakeOpenTSDB) URL() string {
	return f.server.URL
}

func (f *FakeOpenTSDB) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	contents, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	go func() {
		f.ReceivedContents <- contents
	}()
}
