package aibot

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// quietLogger keeps the download logs out of the test output.
type quietLogger struct{}

func (quietLogger) Debug(string, ...interface{}) {}
func (quietLogger) Info(string, ...interface{})  {}
func (quietLogger) Warn(string, ...interface{})  {}
func (quietLogger) Error(string, ...interface{}) {}

func newTestApiClient() *WeComApiClient {
	return NewWeComApiClient(quietLogger{}, 0)
}

func serveBytes(t *testing.T, body []byte, disposition string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if disposition != "" {
			w.Header().Set("Content-Disposition", disposition)
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// 企业微信不给出附件大小，超限只能在读取过程中发现；调用方需要能把它与网络失败
// 区分开，因此错误必须可用 errors.Is 判定。
func TestDownloadFileRawLimited_RejectsOversize(t *testing.T) {
	srv := serveBytes(t, make([]byte, 4096), "")
	client := newTestApiClient()

	_, err := client.DownloadFileRawLimited(srv.URL, 1024)
	if err == nil {
		t.Fatal("expected an error for a body over the limit")
	}
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("error = %v, want it to wrap ErrFileTooLarge", err)
	}
}

// 恰好等于上限是合法的，不能误判。
func TestDownloadFileRawLimited_AllowsExactlyAtLimit(t *testing.T) {
	const size = 1024
	srv := serveBytes(t, make([]byte, size), "")
	client := newTestApiClient()

	result, err := client.DownloadFileRawLimited(srv.URL, size)
	if err != nil {
		t.Fatalf("DownloadFileRawLimited() error = %v, want a body exactly at the limit to pass", err)
	}
	if len(result.Buffer) != size {
		t.Fatalf("buffer = %d bytes, want %d", len(result.Buffer), size)
	}
}

func TestDownloadFileRawLimited_ZeroMeansUnlimited(t *testing.T) {
	const size = 8192
	srv := serveBytes(t, make([]byte, size), `attachment; filename="big.bin"`)
	client := newTestApiClient()

	result, err := client.DownloadFileRawLimited(srv.URL, 0)
	if err != nil {
		t.Fatalf("DownloadFileRawLimited() error = %v", err)
	}
	if len(result.Buffer) != size {
		t.Fatalf("buffer = %d bytes, want the whole body", len(result.Buffer))
	}
	if result.Filename != "big.bin" {
		t.Fatalf("filename = %q, want big.bin", result.Filename)
	}
}

// 回归用例：非 200 时原先 `return nil, err` 而 err 为 nil，调用方拿到 (nil, nil)
// 后解引用 FileResult 直接 panic。媒体 URL 是短期有效的，过期返回 404 很常见。
func TestDownloadFileRaw_NonOKStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	client := newTestApiClient()

	result, err := client.DownloadFileRaw(srv.URL)
	if err == nil {
		t.Fatal("expected an error on 404; returning (nil, nil) makes the caller panic")
	}
	if result != nil {
		t.Fatalf("result = %+v, want nil alongside the error", result)
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error = %v, want it to name the status", err)
	}
}

// 同一路径经 WSClient 也不能 panic：它在 DownloadFileRaw 之后立刻读
// result.Buffer。
func TestDownloadFileLimited_NonOKStatusDoesNotPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	client := &WSClient{apiClient: newTestApiClient(), logger: quietLogger{}}

	data, filename, err := client.DownloadFileLimited(srv.URL, "", 1024)
	if err == nil {
		t.Fatal("expected an error on 403")
	}
	if data != nil || filename != "" {
		t.Fatalf("data/filename = %v/%q, want zero values alongside the error", data, filename)
	}
}

// 明文贴着上限的合法文件不能因为密文的填充而被误判，故下载上界要放宽
// aesPaddingSlack。这里不带 aesKey，直接验证放宽本身生效。
func TestDownloadFileLimited_AllowsPaddingSlack(t *testing.T) {
	const maxBytes = 1024
	srv := serveBytes(t, make([]byte, maxBytes+aesPaddingSlack), "")
	client := &WSClient{apiClient: newTestApiClient(), logger: quietLogger{}}

	data, _, err := client.DownloadFileLimited(srv.URL, "", maxBytes)
	if err != nil {
		t.Fatalf("DownloadFileLimited() error = %v, want the padding slack to be allowed", err)
	}
	if len(data) != maxBytes+aesPaddingSlack {
		t.Fatalf("data = %d bytes, want %d", len(data), maxBytes+aesPaddingSlack)
	}
}

func TestDownloadFileLimited_RejectsOversize(t *testing.T) {
	const maxBytes = 1024
	srv := serveBytes(t, make([]byte, maxBytes*8), "")
	client := &WSClient{apiClient: newTestApiClient(), logger: quietLogger{}}

	_, _, err := client.DownloadFileLimited(srv.URL, "", maxBytes)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("error = %v, want it to wrap ErrFileTooLarge", err)
	}
}
