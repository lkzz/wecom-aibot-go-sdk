package aibot

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

// ErrFileTooLarge 表示文件超出调用方给定的大小上限。调用方可用 errors.Is 判断，
// 以便把"文件太大"与网络失败区分开来告知用户。
var ErrFileTooLarge = errors.New("wecom: file exceeds size limit")

// aesPaddingSlack 是 AES-256-CBC + PKCS#7 填充可能带来的最大额外字节数
// （DecryptFile 按 32 字节块处理），密文因此最多比明文长这么多。
const aesPaddingSlack = 32

// FileResult 文件下载结果
type FileResult struct {
	Buffer   []byte
	Filename string
}

// WeComApiClient 企业微信 API 客户端
// 仅负责文件下载等 HTTP 辅助功能，消息收发均走 WebSocket 通道
type WeComApiClient struct {
	httpClient *http.Client
	logger     Logger
}

// NewWeComApiClient 创建 API 客户端
func NewWeComApiClient(logger Logger, timeout int) *WeComApiClient {
	if timeout <= 0 {
		timeout = 10000
	}

	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Millisecond,
	}

	return &WeComApiClient{
		httpClient: client,
		logger:     logger,
	}
}

// DownloadFileRaw 下载文件（返回原始数据及文件名），不限制大小。
//
// 企业微信不在消息体里给出附件大小，因此响应体有多大只有读完才知道。若调用方
// 对内存占用敏感，应改用 DownloadFileRawLimited。
func (c *WeComApiClient) DownloadFileRaw(fileURL string) (*FileResult, error) {
	return c.DownloadFileRawLimited(fileURL, 0)
}

// DownloadFileRawLimited 下载文件，并把读入内存的字节数限制在 maxBytes 以内；
// maxBytes <= 0 表示不限制。超限时返回包装了 ErrFileTooLarge 的错误，且不会把
// 整个响应体读进内存。
func (c *WeComApiClient) DownloadFileRawLimited(fileURL string, maxBytes int64) (*FileResult, error) {
	c.logger.Info("Downloading file...")

	resp, err := c.httpClient.Get(fileURL)
	if err != nil {
		c.logger.Error("File download failed: " + err.Error())
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			c.logger.Error("Failed to close response body: " + err.Error())
		}
	}()

	if resp.StatusCode != http.StatusOK {
		c.logger.Error("File download failed, status: " + resp.Status)
		// 此处 err 必为 nil，必须自行构造错误：否则调用方会拿到 (nil, nil)，
		// 随后解引用 FileResult 时 panic。
		return nil, fmt.Errorf("wecom: file download failed with status %s", resp.Status)
	}

	body, err := readAtMost(resp.Body, maxBytes)
	if err != nil {
		c.logger.Error("Failed to read response body: " + err.Error())
		return nil, err
	}

	// 从 Content-Disposition 头中解析文件名
	filename := parseFilename(resp.Header.Get("Content-Disposition"))

	c.logger.Info("File downloaded successfully")

	return &FileResult{
		Buffer:   body,
		Filename: filename,
	}, nil
}

// readAtMost 最多读取 maxBytes 字节；maxBytes <= 0 表示不限制。
//
// 多读 1 字节用于判断是否超限：读满 maxBytes 本身是合法的，只有还能再读出内容
// 才说明确实超了。Content-Length 仅用于提前失败，它可以缺失（分块传输）或造假，
// 因此不能作为唯一依据。
func readAtMost(body io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return io.ReadAll(body)
	}
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("wecom: file exceeds %d bytes: %w", maxBytes, ErrFileTooLarge)
	}
	return data, nil
}

// parseFilename 从 Content-Disposition 头解析文件名
func parseFilename(contentDisposition string) string {
	if contentDisposition == "" {
		return ""
	}

	// 优先匹配 filename*=UTF-8''xxx 格式 (RFC 5987)
	if strings.Contains(contentDisposition, "filename*=") {
		// 尝试解析 filename*=UTF-8''
		parts := strings.Split(contentDisposition, "filename*=")
		if len(parts) > 1 {
			value := strings.TrimSpace(parts[1])
			// 可能是 UTF-8''xxx 格式
			if idx := strings.Index(value, "''"); idx != -1 {
				encodedName := value[idx+2:]
				decoded, err := url.QueryUnescape(encodedName)
				if err == nil {
					return decoded
				}
			}
		}
	}

	// 匹配 filename="xxx" 或 filename=xxx 格式
	// 先去掉可能存在的多个 filename= 的情况，取最后一个
	parts := strings.Split(contentDisposition, "filename=")
	if len(parts) > 1 {
		lastPart := parts[len(parts)-1]
		// 去掉引号和分号
		lastPart = strings.Trim(lastPart, "\"; ")
		// 处理可能还有引号的情况
		lastPart = strings.Trim(lastPart, "\"")
		decoded, err := url.QueryUnescape(lastPart)
		if err == nil {
			return decoded
		}
		return lastPart
	}

	return ""
}

// GetFilenameFromURL 从 URL 解析文件名
func GetFilenameFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	// 尝试从路径获取
	filename := filepath.Base(parsed.Path)
	if filename != "" && filename != "/" {
		// 去除查询参数
		if idx := strings.Index(filename, "?"); idx != -1 {
			filename = filename[:idx]
		}
		// 尝试解码
		decoded, err := url.QueryUnescape(filename)
		if err == nil {
			filename = decoded
		}
		return filename
	}

	// 尝试从 Content-Type 获取扩展名
	ext := filepath.Ext(filename)
	if ext != "" {
		return filename
	}

	// 尝试从查询参数获取
	query := parsed.Query()
	if filename := query.Get("filename"); filename != "" {
		decoded, _ := url.QueryUnescape(filename)
		return decoded
	}

	return ""
}

// GetMimeType 获取文件的 MIME 类型
func GetMimeType(filename string) string {
	ext := filepath.Ext(filename)
	return mime.TypeByExtension(ext)
}
