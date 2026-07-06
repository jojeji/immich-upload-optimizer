package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func withChecksumMaps(t *testing.T, fakeToOriginal, originalToFake map[string]string) {
	t.Helper()
	oldFakeToOriginal := fakeToOriginalChecksum
	oldOriginalToFake := originalToFakeChecksum
	fakeToOriginalChecksum = fakeToOriginal
	originalToFakeChecksum = originalToFake
	t.Cleanup(func() {
		fakeToOriginalChecksum = oldFakeToOriginal
		originalToFakeChecksum = oldOriginalToFake
	})
}

func withDownloadFlags(t *testing.T, fromJxl, fromAvif bool) {
	t.Helper()
	oldJxl := downloadJpgFromJxl
	oldAvif := downloadJpgFromAvif
	downloadJpgFromJxl = fromJxl
	downloadJpgFromAvif = fromAvif
	t.Cleanup(func() {
		downloadJpgFromJxl = oldJxl
		downloadJpgFromAvif = oldAvif
	})
}

func withUpstreamURL(t *testing.T, url string) {
	t.Helper()
	oldUpstreamURL := upstreamURL
	upstreamURL = url
	t.Cleanup(func() {
		upstreamURL = oldUpstreamURL
	})
}

func newTestLogger() *customLogger {
	return newCustomLogger(log.New(io.Discard, "", 0), "")
}

func parseJSONLines(t *testing.T, body string) []map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(body), "\n")
	items := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var item map[string]any
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			t.Fatalf("unmarshal json line: %v", err)
		}
		items = append(items, item)
	}
	return items
}

func TestReplaceStreamSyncRewritesV1AndV2Assets(t *testing.T) {
	withChecksumMaps(t,
		map[string]string{
			"fake-v1":          "orig-v1",
			"fake-v2":          "orig-v2",
			"fake-partner-v2":  "orig-partner-v2",
			"fake-album-v2":    "orig-album-v2",
			"fake-ignored-v2":  "orig-ignored-v2",
		},
		map[string]string{},
	)
	withDownloadFlags(t, false, true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w,
			`{"type":"AssetV2","data":{"checksum":"fake-v2","originalFileName":"asset.avif"}}`+"\n"+
				`{"type":"AlbumAssetCreateV2","data":{"checksum":"fake-album-v2","originalFileName":"album.avif"}}`+"\n"+
				`{"type":"PartnerAssetBackfillV2","data":{"checksum":"fake-partner-v2"}}`+"\n"+
				`{"type":"AssetV1","data":{"checksum":"fake-v1","originalFileName":"legacy.avif"}}`+"\n"+
				`{"type":"MemoryV1","data":{"checksum":"fake-ignored-v2","originalFileName":"ignored.avif"}}`+"\n")
	}))
	defer server.Close()
	withUpstreamURL(t, server.URL)

	req := httptest.NewRequest(http.MethodPost, "/api/sync/stream", nil)
	rec := httptest.NewRecorder()

	replacer := Replacer{w: rec, r: req, logger: newTestLogger(), typeId: TypeStream}
	if err := replacer.Replace(); err != nil {
		t.Fatalf("replace stream: %v", err)
	}

	items := parseJSONLines(t, rec.Body.String())
	if got := items[0]["data"].(map[string]any)["checksum"]; got != "orig-v2" {
		t.Fatalf("expected AssetV2 checksum rewrite, got %v", got)
	}
	if got := items[0]["data"].(map[string]any)["originalFileName"]; got != "asset.avif.jpg" {
		t.Fatalf("expected AssetV2 filename rewrite, got %v", got)
	}
	if got := items[1]["data"].(map[string]any)["checksum"]; got != "orig-album-v2" {
		t.Fatalf("expected AlbumAssetCreateV2 checksum rewrite, got %v", got)
	}
	if got := items[2]["data"].(map[string]any)["checksum"]; got != "orig-partner-v2" {
		t.Fatalf("expected PartnerAssetBackfillV2 checksum rewrite, got %v", got)
	}
	if got := items[3]["data"].(map[string]any)["checksum"]; got != "orig-v1" {
		t.Fatalf("expected AssetV1 checksum rewrite, got %v", got)
	}
	if got := items[4]["data"].(map[string]any)["checksum"]; got != "fake-ignored-v2" {
		t.Fatalf("expected non-asset stream type to remain unchanged, got %v", got)
	}
}

func TestReplaceBulkUploadCheckRewritesBase64AndHexChecksums(t *testing.T) {
	rawChecksum := bytes.Repeat([]byte{0xAB}, sha1.Size)
	base64Checksum := base64.StdEncoding.EncodeToString(rawChecksum)
	hexChecksum := hex.EncodeToString(rawChecksum)
	withChecksumMaps(t, map[string]string{}, map[string]string{base64Checksum: "fake-checksum"})

	var seen bulkUploadCheckRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Errorf("decode upstream body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[]}`)
	}))
	defer server.Close()
	withUpstreamURL(t, server.URL)

	body := `{"assets":[{"id":"1","checksum":"` + base64Checksum + `"},{"id":"2","checksum":"` + hexChecksum + `"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/assets/bulk-upload-check", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if err := replaceBulkUploadCheck(rec, req, newTestLogger()); err != nil {
		t.Fatalf("replace bulk upload check: %v", err)
	}

	if len(seen.Assets) != 2 {
		t.Fatalf("expected 2 assets upstream, got %d", len(seen.Assets))
	}
	if seen.Assets[0].Checksum != "fake-checksum" {
		t.Fatalf("expected base64 checksum rewrite, got %s", seen.Assets[0].Checksum)
	}
	if seen.Assets[1].Checksum != "fake-checksum" {
		t.Fatalf("expected hex checksum rewrite, got %s", seen.Assets[1].Checksum)
	}
}

func TestReplaceAssetViewRewritesChecksum(t *testing.T) {
	withChecksumMaps(t, map[string]string{"fake-asset": "orig-asset"}, map[string]string{})
	withDownloadFlags(t, false, true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"checksum":"fake-asset","originalFileName":"detail.avif"}`)
	}))
	defer server.Close()
	withUpstreamURL(t, server.URL)

	req := httptest.NewRequest(http.MethodGet, "/api/assets/123", nil)
	rec := httptest.NewRecorder()

	replacer := Replacer{w: rec, r: req, logger: newTestLogger(), typeId: TypeAssetView}
	if err := replacer.Replace(); err != nil {
		t.Fatalf("replace asset view: %v", err)
	}

	var asset Asset
	if err := json.Unmarshal(rec.Body.Bytes(), &asset); err != nil {
		t.Fatalf("unmarshal asset response: %v", err)
	}
	if got := asset["checksum"]; got != "orig-asset" {
		t.Fatalf("expected asset checksum rewrite, got %v", got)
	}
	if got := asset["originalFileName"]; got != "detail.avif.jpg" {
		t.Fatalf("expected asset filename rewrite, got %v", got)
	}
}

func TestWebSocketUploadReadyAssetRewrite(t *testing.T) {
	withChecksumMaps(t, map[string]string{"fake-websocket": "orig-websocket"}, map[string]string{})
	withDownloadFlags(t, false, true)

	message := []byte(`["AssetUploadReadyV1",{"asset":{"checksum":"fake-websocket","originalFileName":"ready.avif"}}]`)
	var wsMsg WebSocket42
	if err := json.Unmarshal(message, &wsMsg); err != nil {
		t.Fatalf("unmarshal websocket message: %v", err)
	}

	asset := wsMsg.getUploadReadyAsset()
	if asset == nil {
		t.Fatal("expected upload ready asset")
	}
	mapLock.RLock()
	asset.toOriginalAsset()
	mapLock.RUnlock()

	if got := asset["checksum"]; got != "orig-websocket" {
		t.Fatalf("expected websocket checksum rewrite, got %v", got)
	}
	if got := asset["originalFileName"]; got != "ready.avif.jpg" {
		t.Fatalf("expected websocket filename rewrite, got %v", got)
	}
}
