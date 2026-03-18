package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"Velduct.GoClient/internal/config"
	"Velduct.GoClient/internal/constants"
	jwtutil "Velduct.GoClient/internal/jwt"
	"Velduct.GoClient/internal/protocol"
	gosync "Velduct.GoClient/internal/sync"
	"Velduct.GoClient/internal/transfer"
	"github.com/gorilla/websocket"
)

type Client struct {
	Config config.Config

	conn    *websocket.Conn
	mu      sync.Mutex
	session *transfer.Session

	receivingArchive bool // Fast path: true when receiving TAR stream
}

func NewClient(cfg config.Config) *Client {
	return &Client{Config: cfg}
}

func (c *Client) Connect() error {
	token, err := jwtutil.Generate(jwtutil.TokenOptions{
		Issuer:   c.Config.JwtIssuer,
		Audience: c.Config.JwtAudience,
		Key:      c.Config.JwtKey,
		TTL:      time.Duration(c.Config.JwtTTLSeconds) * time.Second,
	})
	if err != nil {
		return fmt.Errorf("jwt generation failed: %w", err)
	}

	// Query parameter is required since browser WebSocket API doesn't support custom headers.
	connectURL, err := url.Parse(c.Config.ServerURL)
	if err != nil {
		return fmt.Errorf("invalid server_url %q: %w", c.Config.ServerURL, err)
	}
	q := connectURL.Query()
	q.Set("access_token", token)
	connectURL.RawQuery = q.Encode()

	// WriteBufferSize must match ChunkSizeBytes to avoid 1000+ syscalls per large chunk.
	dialer := websocket.Dialer{
		WriteBufferSize: c.Config.ChunkSizeBytes + c.Config.WriteBufferOverhead,
		ReadBufferSize:  c.Config.ReadBufferSize,
	}

	c.conn, _, err = dialer.Dial(connectURL.String(), nil)
	if err != nil {
		return err
	}

	c.session = transfer.NewSession(c.Config.Shares, c.SafeSend, c.Config)

	gosync.RegisterShares(c.SafeSend, c.Config.Shares, c.Config.TargetShares)

	slog.Info("[Client] Connected", "url", c.Config.ServerURL, "issuer", c.Config.JwtIssuer)
	return nil
}

func (c *Client) SafeSend(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (c *Client) Run() {
	slog.Info("[Client] Session started")

	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			slog.Error("[Client] WebSocket disconnected", "err", err)
			return
		}

		if len(payload) == 0 {
			continue
		}

		opCode := payload[0]

		// Fast path for TAR data: most frequent message type during archive reception.
		if c.receivingArchive {
			switch opCode {
			case protocol.CmdArchiveData:
				c.session.HandleArchiveData(payload[1:])
				continue
			case protocol.CmdArchiveDone:
				c.session.HandleArchiveDone()
				c.receivingArchive = false
				continue
			}
		}

		switch opCode {

		case protocol.CmdPullFile:
			p := append([]byte(nil), payload...)
			go c.session.HandlePull(p)

		case protocol.CmdBatchPull:
			p := append([]byte(nil), payload...)
			go c.session.HandleBatchPull(p)

		case protocol.CmdOfferFile:
			c.session.HandleOffer(payload)

		case protocol.SrvPullStream:
			creditCount := 1
			if len(payload) > 1 {
				creditCount = int(payload[1])
				if creditCount <= 0 {
					creditCount = 1
				}
			}
			for i := 0; i < creditCount; i++ {
				c.session.AddCredit()
			}

		case protocol.CmdFileData:
			c.session.HandleData(payload[1:])

		case protocol.CmdFileDone:
			c.session.HandleDone()

		case protocol.CmdSkipFile:
			slog.Debug("[Client] Remote: file already up-to-date, skipped")

		case protocol.CmdDeleteConfirm:
			p := append([]byte(nil), payload...)
			c.session.HandleDeleteConfirm(p)

		case protocol.CmdFileMtimeAck:
			c.session.HandleFileMtimeAck(payload[1:])

		case protocol.CmdCheckFiles:
			p := append([]byte(nil), payload...)
			go c.session.HandleServerCheckFiles(p[1:])

		case protocol.CmdArchiveStart:
			c.receivingArchive = true
			c.session.HandleArchiveStart()

		case protocol.CmdArchiveData:
			c.session.HandleArchiveData(payload[1:])

		case protocol.CmdArchiveDone:
			c.session.HandleArchiveDone()
			c.receivingArchive = false

		default:
			slog.Warn("[Client] Unknown opcode received", "opcode", opCode)
		}
	}
}

func (c *Client) Close() {
	if c.session != nil {
		c.session.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
	slog.Debug("[Client] Closed")
}

// RequestFile sends a single file download request.
func (c *Client) RequestFile(key, path string) {
	if c.session != nil {
		c.session.SendPull(key, path)
	}
}

// SyncAllShares scans all shares and sends metadata to server.
func (c *Client) SyncAllShares() {
	if c.session == nil {
		return
	}

	for key, baseDir := range c.Config.Shares {
		if isTargetShare(key, c.Config.TargetShares) {
			slog.Debug("[Client] Skipping TargetOnly share in sync", "key", key)
			continue
		}

		go func(k, dir string) {
			slog.Debug("[Client] Scanning share", "key", k, "path", dir)

			var batch []fileInfo
			totalFiles := 0

			tempDir := ""
			if td, ok := c.Config.TempDirs[k]; ok {
				tempDir = td
			}

			err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					slog.Debug("[Client] WalkDir error, skipping entry", "path", p, "err", err)
					return nil
				}
				if d.IsDir() && d.Name() == constants.TempDirName {
					return filepath.SkipDir
				}
				if d.IsDir() && tempDir != "" && p == tempDir {
					return filepath.SkipDir
				}
				if d.IsDir() {
					return nil
				}

				if strings.HasSuffix(d.Name(), constants.TempExt) {
					return nil
				}

				info, err := d.Info()
				if err != nil {
					slog.Debug("[Client] Could not stat entry, skipping", "path", p, "err", err)
					return nil
				}

				rel, _ := filepath.Rel(dir, p)
				fsMtimeMs := info.ModTime().UnixNano() / 1e6
				batch = append(batch, fileInfo{
					Key:   k,
					Path:  filepath.ToSlash(rel),
					Size:  info.Size(),
					MTime: c.session.ResolveReportMtime(p, fsMtimeMs),
				})
				totalFiles++

				if len(batch) >= c.Config.ScanBatchSize {
					c.sendBatch(batch)
					batch = nil
				}
				return nil
			})

			if len(batch) > 0 {
				c.sendBatch(batch)
			}

			if err != nil {
				slog.Error("[Client] Walk error", "key", k, "err", err)
			} else {
				slog.Debug("[Client] Share scan complete", "key", k, "files", totalFiles)
			}
		}(key, baseDir)
	}
}

// UploadAllShares uploads all shares to server as a single TAR stream.
func (c *Client) UploadAllShares() {
	if c.session == nil {
		return
	}
	slog.Info("[Client] Initiating TAR upload of all shares")
	if err := c.session.SendAllSharesAsTar(); err != nil {
		slog.Error("[Client] TAR upload failed", "err", err)
	}
}

// --- Internal ---

type fileInfo struct {
	Key   string
	Path  string
	Size  int64
	MTime int64
}

func (c *Client) sendBatch(files []fileInfo) {
	buf := new(bytes.Buffer)
	buf.WriteByte(protocol.CmdCheckFiles)
	binary.Write(buf, binary.LittleEndian, int32(len(files)))

	for _, f := range files {
		binary.Write(buf, binary.LittleEndian, int32(len(f.Key)))
		buf.WriteString(f.Key)
		binary.Write(buf, binary.LittleEndian, int32(len(f.Path)))
		buf.WriteString(f.Path)
		binary.Write(buf, binary.LittleEndian, f.Size)
		binary.Write(buf, binary.LittleEndian, f.MTime)
	}

	if err := c.SafeSend(buf.Bytes()); err != nil {
		slog.Error("[Client] Failed to send batch", "size", len(files), "err", err)
	}
}

func (c *Client) SyncMultipleFiles(changes map[string][]string) {
	if c.session == nil {
		return
	}

	var batch []fileInfo
	suppressed := 0

	for key, paths := range changes {
		if isTargetShare(key, c.Config.TargetShares) {
			continue
		}

		baseDir, ok := c.Config.Shares[key]
		if !ok {
			slog.Warn("[Client] SyncMultipleFiles: unknown share key", "key", key)
			continue
		}

		for _, absPath := range paths {
			if strings.Contains(absPath, string(os.PathSeparator)+constants.TempDirName+string(os.PathSeparator)) ||
				strings.HasSuffix(absPath, string(os.PathSeparator)+constants.TempDirName) {
				continue
			}
			if tempDir, ok := c.Config.TempDirs[key]; ok {
				if strings.HasPrefix(absPath, tempDir+string(os.PathSeparator)) || absPath == tempDir {
					continue
				}
			}
			if strings.HasSuffix(filepath.Base(absPath), constants.TempExt) {
				continue
			}

			rel, err := filepath.Rel(baseDir, absPath)
			if err != nil {
				slog.Warn("[Client] Could not compute relative path",
					"base", baseDir, "abs", absPath, "err", err)
				continue
			}

			info, err := os.Stat(absPath)
			if err != nil {
				c.session.CancelActiveDownload(absPath)

				batch = append(batch, fileInfo{
					Key:   key,
					Path:  filepath.ToSlash(rel),
					Size:  -1,
					MTime: 0,
				})
			} else if info.IsDir() {
				slog.Debug("[Client] Directory event, scanning contents", "path", absPath)
				walkTempDir := ""
				if td, ok := c.Config.TempDirs[key]; ok {
					walkTempDir = td
				}
				filepath.WalkDir(absPath, func(p string, d fs.DirEntry, err error) error {
					if err != nil {
						slog.Debug("[Client] WalkDir error in SyncMultipleFiles", "path", p, "err", err)
						return nil
					}
					if d.IsDir() && d.Name() == constants.TempDirName {
						return filepath.SkipDir
					}
					if d.IsDir() && walkTempDir != "" && p == walkTempDir {
						return filepath.SkipDir
					}
					if d.IsDir() {
						return nil
					}
					if strings.HasSuffix(d.Name(), constants.TempExt) {
						return nil
					}
					inf, err := d.Info()
					if err != nil {
						slog.Debug("[Client] Could not stat file in directory scan", "path", p, "err", err)
						return nil
					}
					fsMtimeMs := inf.ModTime().UnixNano() / 1e6
						if c.session.IsRecentDownload(p, fsMtimeMs) {
						suppressed++
						return nil
					}
					r, _ := filepath.Rel(baseDir, p)
					batch = append(batch, fileInfo{
						Key:   key,
						Path:  filepath.ToSlash(r),
						Size:  inf.Size(),
						MTime: c.session.ResolveReportMtime(p, fsMtimeMs),
					})
					if len(batch) >= c.Config.ScanBatchSize {
						c.sendBatch(batch)
						batch = nil
					}
					return nil
				})
			} else {
				fsMtimeMs := info.ModTime().UnixNano() / 1e6
				if c.session.IsRecentDownload(absPath, fsMtimeMs) {
					suppressed++
					continue
				}
				batch = append(batch, fileInfo{
					Key:   key,
					Path:  filepath.ToSlash(rel),
					Size:  info.Size(),
					MTime: c.session.ResolveReportMtime(absPath, fsMtimeMs),
				})
			}

			if len(batch) >= c.Config.ScanBatchSize {
				c.sendBatch(batch)
				batch = nil
			}
		}
	}

	if suppressed > 0 {
		slog.Debug("[Client] SyncMultipleFiles: suppressed recently downloaded files (anti-echo)",
			"suppressed", suppressed)
	}

	if len(batch) > 0 {
		c.sendBatch(batch)
	}
}

func isTargetShare(key string, targets []string) bool {
	for _, t := range targets {
		if t == key {
			return true
		}
	}
	return false
}
