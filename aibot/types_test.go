package aibot

import (
	"encoding/json"
	"testing"
)

// TestUploadMediaFinishResultParsesNumericCreatedAt 锁住企微完成上传响应里
// created_at 是**数字**这一事实。
//
// 该 payload 取自真机：CreatedAt 曾被声明为 string，线上报
// "cannot unmarshal number into Go struct field
// UploadMediaFinishResult.created_at of type string"。这一个字段类型不符会让
// json.Unmarshal 整体返回错误，调用方拿不到 media_id，媒体消息完全发不出去 ——
// 即便 media_id 本身在报文里是好的。
func TestUploadMediaFinishResultParsesNumericCreatedAt(t *testing.T) {
	const payload = `{"type":"image","media_id":"MEDIA-ID-123","created_at":1785999999}`

	var got UploadMediaFinishResult
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("解析完成上传响应失败: %v", err)
	}
	if got.MediaID != "MEDIA-ID-123" {
		t.Errorf("MediaID = %q, want %q", got.MediaID, "MEDIA-ID-123")
	}
	if got.Type != WeComMediaTypeImage {
		t.Errorf("Type = %q, want %q", got.Type, WeComMediaTypeImage)
	}
	if got.CreatedAt != 1785999999 {
		t.Errorf("CreatedAt = %d, want %d", got.CreatedAt, 1785999999)
	}
}

// TestUploadMediaFinishResultToleratesAbsentCreatedAt 保证 created_at 缺失时仍能拿到
// media_id：该字段无人读取，不该成为发送成功的前提。
func TestUploadMediaFinishResultToleratesAbsentCreatedAt(t *testing.T) {
	var got UploadMediaFinishResult
	if err := json.Unmarshal([]byte(`{"type":"file","media_id":"M-1"}`), &got); err != nil {
		t.Fatalf("解析缺少 created_at 的响应失败: %v", err)
	}
	if got.MediaID != "M-1" {
		t.Errorf("MediaID = %q, want %q", got.MediaID, "M-1")
	}
	if got.CreatedAt != 0 {
		t.Errorf("CreatedAt = %d, want 0", got.CreatedAt)
	}
}
