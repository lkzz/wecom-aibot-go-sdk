package aibot

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ============================================================================
// 常量定义
// ============================================================================

const (
	// 默认 WebSocket 连接地址
	defaultWSURL = "wss://openws.work.weixin.qq.com"

	// 重连参数
	defaultReconnectBaseDelay = 1000  // 基础延迟 (ms)
	reconnectMaxDelay         = 30000 // 最大延迟 (ms)
	maxMissedPong             = 2     // 最大丢失 pong 次数

	// 队列参数
	replyAckTimeout            = 5000 // 回执超时 (ms)
	defaultMaxReplyQueueSize   = 500  // 单个 req_id 的默认最大队列长度
	defaultMaxAuthFailAttempts = 5    // 默认认证失败最大重试次数
)

// ============================================================================
// 回调函数类型
// ============================================================================

// OnConnectedFunc 连接建立回调
type OnConnectedFunc func()

// OnAuthenticatedFunc 认证成功回调
type OnAuthenticatedFunc func()

// OnDisconnectedFunc 连接断开回调
type OnDisconnectedFunc func(reason string)

// OnMessageFunc 收到消息回调
type OnMessageFunc func(frame *WsFrame)

// OnReconnectingFunc 重连回调
type OnReconnectingFunc func(attempt int)

// OnErrorFunc 错误回调
type OnErrorFunc func(err error)

// ============================================================================
// 回复队列项
// ============================================================================

type replyQueueItem struct {
	frame   WsFrame
	resolve func(*WsFrame)
	reject  func(error)
}

// ============================================================================
// WsConnectionManager WebSocket 长连接管理器
// ============================================================================

// WsConnectionManager 负责维护与企业微信的 WebSocket 长连接
type WsConnectionManager struct {
	logger Logger
	wsURL  string
	dialer *websocket.Dialer

	heartbeatInterval      int
	reconnectBaseDelay     int
	maxReconnectAttempts   int
	maxAuthFailureAttempts int
	maxReplyQueueSize      int

	// ws 由多个 goroutine 访问：读循环、心跳定时器、每个 reqID 的发送
	// goroutine，以及重连定时器。必须通过 conn/setConn/closeConn 访问，
	// 否则 "if m.ws != nil" 之后字段可能已被置空，直接 nil 解引用 panic。
	ws     *websocket.Conn
	connMu sync.RWMutex

	// isManualClose 写自调用方的 Connect/Disconnect，读自读循环、handleClose
	// 和重连定时器，因此不能是裸 bool。
	isManualClose atomic.Bool

	// writeMu 串行化对 ws 的写操作。gorilla/websocket 只允许一个并发写者，
	// 否则直接 panic("concurrent write to websocket connection")。本管理器有
	// 多个独立写入方：每个 reqID 各起一个 processReplyQueue goroutine，以及
	// 心跳定时器，因此必须加锁。锁只覆盖写调用本身，不覆盖回执等待。
	writeMu sync.Mutex

	// 认证凭证
	botID           string
	botSecret       string
	extraAuthParams map[string]interface{}

	// 重连状态
	reconnectAttempts       int
	authFailureAttempts     int
	lastCloseWasAuthFailure bool
	missedPongCount         int

	// 定时器。两者都由多方访问：调用方的 Connect/Disconnect、读循环（经
	// handleClose → scheduleReconnect）、以及定时器自己的回调，因此统一由
	// timerMu 保护，只能通过下面的 setter/clear 方法访问。
	timerMu        sync.Mutex
	heartbeatTimer *time.Timer
	reconnectTimer *time.Timer

	// 回复队列
	replyQueues   map[string][]replyQueueItem
	replyQueuesMu sync.Mutex
	pendingAcks   map[string]*pendingAck
	pendingAcksMu sync.Mutex
	pendingAckSeq uint64

	// 回调函数
	OnConnected        OnConnectedFunc
	OnAuthenticated    OnAuthenticatedFunc
	OnDisconnected     OnDisconnectedFunc
	OnServerDisconnect OnDisconnectedFunc // 服务端主动断开回调（新连接建立导致旧连接被断开）
	OnMessage          OnMessageFunc
	OnReconnecting     OnReconnectingFunc
	OnError            OnErrorFunc

	// 上下文和取消
	ctx    context.Context
	cancel context.CancelFunc
}

// pendingAck 待回执项
type pendingAck struct {
	resolve func(*WsFrame)
	reject  func(error)
	timer   *time.Timer
	seq     uint64
}

// NewWsConnectionManager 创建 WebSocket 连接管理器
func NewWsConnectionManager(
	logger Logger,
	heartbeatInterval int,
	reconnectBaseDelay int,
	maxReconnectAttempts int,
	wsURL string,
	dialer *websocket.Dialer,
	maxReplyQueueSize int,
	maxAuthFailureAttempts int,
) *WsConnectionManager {
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30000
	}
	if reconnectBaseDelay <= 0 {
		reconnectBaseDelay = defaultReconnectBaseDelay
	}
	if maxReconnectAttempts == 0 {
		maxReconnectAttempts = 10
	}
	if wsURL == "" {
		wsURL = defaultWSURL
	}
	if maxReplyQueueSize <= 0 {
		maxReplyQueueSize = defaultMaxReplyQueueSize
	}
	if maxAuthFailureAttempts == 0 {
		maxAuthFailureAttempts = defaultMaxAuthFailAttempts
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WsConnectionManager{
		logger:                 logger,
		wsURL:                  wsURL,
		dialer:                 dialer,
		heartbeatInterval:      heartbeatInterval,
		reconnectBaseDelay:     reconnectBaseDelay,
		maxReconnectAttempts:   maxReconnectAttempts,
		maxAuthFailureAttempts: maxAuthFailureAttempts,
		maxReplyQueueSize:      maxReplyQueueSize,

		replyQueues: make(map[string][]replyQueueItem),
		pendingAcks: make(map[string]*pendingAck),

		ctx:    ctx,
		cancel: cancel,
	}
}

// conn 返回当前连接，可能为 nil
func (m *WsConnectionManager) conn() *websocket.Conn {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	return m.ws
}

// setConn 设置当前连接
func (m *WsConnectionManager) setConn(ws *websocket.Conn) {
	m.connMu.Lock()
	m.ws = ws
	m.connMu.Unlock()
}

// closeConn 关闭并清空当前连接
func (m *WsConnectionManager) closeConn() {
	m.connMu.Lock()
	ws := m.ws
	m.ws = nil
	m.connMu.Unlock()
	if ws != nil {
		_ = ws.Close()
	}
}

// clearConnIf 仅当 ws 仍是当前连接时清空它，返回是否清空成功。
// 用于让陈旧的读循环（其连接已被重连替换）不影响新连接的状态。
func (m *WsConnectionManager) clearConnIf(ws *websocket.Conn) bool {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	if m.ws != ws {
		return false
	}
	m.ws = nil
	return true
}

// setReconnectTimer 替换挂起的重连定时器。
func (m *WsConnectionManager) setReconnectTimer(t *time.Timer) {
	m.timerMu.Lock()
	old := m.reconnectTimer
	m.reconnectTimer = t
	m.timerMu.Unlock()
	if old != nil {
		old.Stop()
	}
}

// stopReconnectTimer 取消并清空挂起的重连定时器。
func (m *WsConnectionManager) stopReconnectTimer() {
	m.timerMu.Lock()
	t := m.reconnectTimer
	m.reconnectTimer = nil
	m.timerMu.Unlock()
	if t != nil {
		t.Stop()
	}
}

// clearReconnectTimer 仅清空引用，不 Stop：由定时器回调自己调用，此时它已触发。
func (m *WsConnectionManager) clearReconnectTimer() {
	m.timerMu.Lock()
	m.reconnectTimer = nil
	m.timerMu.Unlock()
}

// SetCredentials 设置认证凭证
func (m *WsConnectionManager) SetCredentials(botID, botSecret string, extraAuthParams map[string]interface{}) {
	m.botID = botID
	m.botSecret = botSecret
	m.extraAuthParams = extraAuthParams
}

// Connect 建立 WebSocket 连接
func (m *WsConnectionManager) Connect() {
	m.isManualClose.Store(false)

	// 取消挂起的重连定时器，防止与当前 connect 竞态
	m.stopReconnectTimer()

	// 清理可能未完全关闭的旧连接
	m.closeConn()

	m.logger.Info("Connecting to WebSocket: " + m.wsURL + "...")

	go m.connect()
}

func (m *WsConnectionManager) connect() {
	dialer := m.dialer
	if dialer == nil {
		dialer = &websocket.Dialer{}
	}

	ws, _, err := dialer.Dial(m.wsURL, nil)
	if err != nil {
		m.logger.Error("Failed to create WebSocket connection: " + err.Error())
		if m.OnError != nil {
			m.OnError(err)
		}
		m.scheduleReconnect()
		return
	}

	m.setConn(ws)
	m.missedPongCount = 0
	m.lastCloseWasAuthFailure = false
	m.setupEventHandlers(ws)

	// 连接建立后立即发送认证帧
	m.sendAuth()

	if m.OnConnected != nil {
		m.OnConnected()
	}
}

// setupEventHandlers 启动 ws 的读循环。
//
// 读循环持有自己的 conn 引用，不读取 m.ws：处理消息的过程本身可能清空 m.ws
// （disconnected_event、读错误后的 handleClose），重连也会把 m.ws 换成新连接。
// 任何一种情况下再从字段取连接都会拿到 nil 或别人的连接。
func (m *WsConnectionManager) setupEventHandlers(ws *websocket.Conn) {
	if ws == nil {
		return
	}

	// 读取消息
	go func() {
		for {
			select {
			case <-m.ctx.Done():
				return
			default:
			}

			_, data, err := ws.ReadMessage()
			if err != nil {
				if m.isManualClose.Load() {
					return
				}

				m.logger.Error("WebSocket read error: " + err.Error())
				m.handleClose(ws, err.Error())
				return
			}

			// 连接已被替换或关闭，本读循环已陈旧，停止处理
			if m.conn() != ws {
				return
			}

			m.handleMessage(data)
		}
	}()
}

// handleMessage 处理收到的消息
func (m *WsConnectionManager) handleMessage(data []byte) {
	var frame WsFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		m.logger.Error("Failed to parse WebSocket message: " + err.Error())
		return
	}

	cmd := frame.Cmd
	reqID := frame.Headers.ReqID

	// 消息推送回调
	if cmd == WsCmd.CALLBACK {
		m.logger.Debug(fmt.Sprintf("[server -> plugin] cmd=%s, reqId=%s", cmd, reqID))
		if m.OnMessage != nil {
			m.OnMessage(&frame)
		}
		return
	}

	// 事件推送回调
	if cmd == WsCmd.EVENT_CALLBACK {
		m.logger.Debug(fmt.Sprintf("[server -> plugin] cmd=%s, reqId=%s", cmd, reqID))

		// 检测 disconnected_event：有新连接建立，服务端通知旧连接即将被断开
		if m.isDisconnectedEvent(&frame) {
			m.logger.Warn("Received disconnected_event: a new connection has been established, this connection will be closed by server")
			// 先分发事件给上层（让用户可以监听 disconnected_event）
			if m.OnMessage != nil {
				m.OnMessage(&frame)
			}
			// 停止心跳、清理待处理消息
			m.stopHeartbeat()
			m.clearPendingMessages("Server disconnected due to new connection")
			// 标记为非手动断开但阻止自动重连（服务端正常行为，重连也会被再次断开）
			m.isManualClose.Store(true)
			// 通知上层服务端主动断开
			if m.OnServerDisconnect != nil {
				m.OnServerDisconnect("New connection established, server disconnected this connection")
			}
			// 主动关闭 socket
			m.closeConn()
			return
		}

		if m.OnMessage != nil {
			m.OnMessage(&frame)
		}
		return
	}

	// 无 cmd 的帧：认证响应、心跳响应或回复消息回执

	// 认证响应（优先于 pendingAcks 检查，避免误判）
	if strings.HasPrefix(reqID, WsCmd.SUBSCRIBE) {
		m.handleAuthResponse(&frame)
		return
	}

	// 心跳响应（优先于 pendingAcks 检查，避免误判）
	if strings.HasPrefix(reqID, WsCmd.HEARTBEAT) {
		m.handleHeartbeatResponse(&frame)
		return
	}

	// 检查是否是回复消息的回执
	if m.hasPendingAck(reqID) {
		m.handleReplyAck(reqID, &frame)
		return
	}

	// 未知帧类型 — 只记录警告，不传给 OnMessage
	m.logger.Warn("Received unknown frame (ignored): " + string(data))
}

// isDisconnectedEvent 检测是否为 disconnected_event
func (m *WsConnectionManager) isDisconnectedEvent(frame *WsFrame) bool {
	if frame.Body == nil {
		return false
	}
	var bodyMap map[string]interface{}
	if err := json.Unmarshal(frame.Body, &bodyMap); err != nil {
		return false
	}
	eventRaw, ok := bodyMap["event"]
	if !ok {
		return false
	}
	eventMap, ok := eventRaw.(map[string]interface{})
	if !ok {
		return false
	}
	eventType, _ := eventMap["eventtype"].(string)
	return eventType == string(EventTypeDisconnected)
}

// handleAuthResponse 处理认证响应
func (m *WsConnectionManager) handleAuthResponse(frame *WsFrame) {
	if frame.ErrCode != 0 {
		m.logger.Error(fmt.Sprintf("Authentication failed: errcode=%d, errmsg=%s", frame.ErrCode, frame.ErrMsg))
		if m.OnError != nil {
			m.OnError(fmt.Errorf("authentication failed: %s (code: %d)", frame.ErrMsg, frame.ErrCode))
		}
		// 标记为认证失败，close 事件中 scheduleReconnect 会据此使用 authFailureAttempts 计数器
		m.lastCloseWasAuthFailure = true
		// 认证失败，主动关闭连接，触发读错误 → handleClose → scheduleReconnect
		if ws := m.conn(); ws != nil {
			_ = ws.Close()
		}
		return
	}

	m.logger.Info("Authentication successful")
	// 认证成功：重置所有重连计数器
	m.reconnectAttempts = 0
	m.authFailureAttempts = 0
	m.startHeartbeat()
	if m.OnAuthenticated != nil {
		m.OnAuthenticated()
	}
}

// handleHeartbeatResponse 处理心跳响应
func (m *WsConnectionManager) handleHeartbeatResponse(frame *WsFrame) {
	if frame.ErrCode != 0 {
		m.logger.Warn(fmt.Sprintf("Heartbeat ack error: errcode=%d, errmsg=%s", frame.ErrCode, frame.ErrMsg))
		return
	}

	m.missedPongCount = 0
}

// handleClose 处理连接关闭。ws 为发生关闭的那个连接，若它已不是当前连接，
// 说明重连已经完成，本次关闭是陈旧事件，不应清理新连接或再次触发重连。
func (m *WsConnectionManager) handleClose(ws *websocket.Conn, reason string) {
	if !m.clearConnIf(ws) {
		return
	}
	// 读循环已退出，关闭底层连接释放资源
	_ = ws.Close()

	m.stopHeartbeat()
	m.clearPendingMessages("WebSocket connection closed (" + reason + ")")

	if m.OnDisconnected != nil {
		m.OnDisconnected(reason)
	}

	if !m.isManualClose.Load() {
		m.scheduleReconnect()
	}
}

// sendAuth 发送认证帧
func (m *WsConnectionManager) sendAuth() {
	if m.conn() == nil {
		return
	}

	// 构建认证 body
	authBody := map[string]interface{}{
		"bot_id": m.botID,
		"secret": m.botSecret,
	}
	// 合入额外的认证参数（如 scene、plug_version 等）
	for k, v := range m.extraAuthParams {
		authBody[k] = v
	}

	bodyJSON, err := json.Marshal(authBody)
	if err != nil {
		m.logger.Error("Failed to marshal auth body: " + err.Error())
		return
	}

	frame := WsFrame{
		Cmd: WsCmd.SUBSCRIBE,
		Headers: WsFrameHeaders{
			ReqID: generateReqId(WsCmd.SUBSCRIBE),
		},
		Body: bodyJSON,
	}

	m.sendFrame(frame)
	m.logger.Info("Auth frame sent")
}

// sendHeartbeat 发送心跳
func (m *WsConnectionManager) sendHeartbeat() {
	// 检查丢失 pong 次数
	if m.missedPongCount >= maxMissedPong {
		m.logger.Warn(fmt.Sprintf("No heartbeat ack received for %d consecutive pings, connection considered dead", m.missedPongCount))
		m.stopHeartbeat()
		if ws := m.conn(); ws != nil {
			_ = ws.Close()
		}
		return
	}

	m.missedPongCount++

	frame := WsFrame{
		Cmd: WsCmd.HEARTBEAT,
		Headers: WsFrameHeaders{
			ReqID: generateReqId(WsCmd.HEARTBEAT),
		},
	}

	m.sendFrame(frame)
}

// startHeartbeat 启动心跳
func (m *WsConnectionManager) startHeartbeat() {
	m.stopHeartbeat()

	m.timerMu.Lock()
	m.heartbeatTimer = time.AfterFunc(
		time.Duration(m.heartbeatInterval)*time.Millisecond,
		m.sendHeartbeat,
	)
	m.timerMu.Unlock()

	m.logger.Debug(fmt.Sprintf("Heartbeat timer started, interval: %dms", m.heartbeatInterval))
}

// stopHeartbeat 停止心跳
func (m *WsConnectionManager) stopHeartbeat() {
	m.timerMu.Lock()
	t := m.heartbeatTimer
	m.heartbeatTimer = nil
	m.timerMu.Unlock()
	if t != nil {
		t.Stop()
	}
}

// scheduleReconnect 安排重连
//
// 区分两种重连场景，使用独立的计数器和最大重试次数：
// - 认证失败（lastCloseWasAuthFailure=true）：使用 authFailureAttempts / maxAuthFailureAttempts
// - 连接断开（lastCloseWasAuthFailure=false）：使用 reconnectAttempts / maxReconnectAttempts
//
// disconnected_event（被踢下线）不会触发重连，因为 isManualClose 已被设为 true。
func (m *WsConnectionManager) scheduleReconnect() {
	if m.lastCloseWasAuthFailure {
		// 认证失败场景
		if m.maxAuthFailureAttempts > 0 && m.authFailureAttempts >= m.maxAuthFailureAttempts {
			m.logger.Error(fmt.Sprintf("Max auth failure attempts reached (%d), giving up", m.maxAuthFailureAttempts))
			if m.OnError != nil {
				m.OnError(&WSAuthFailureError{MaxAttempts: m.maxAuthFailureAttempts})
			}
			return
		}
		m.authFailureAttempts++

		delay := m.reconnectBaseDelay * (1 << (m.authFailureAttempts - 1))
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}

		m.logger.Info(fmt.Sprintf("Auth failed, reconnecting in %dms (auth attempt %d/%d)...", delay, m.authFailureAttempts, m.maxAuthFailureAttempts))
		if m.OnReconnecting != nil {
			m.OnReconnecting(m.authFailureAttempts)
		}

		m.setReconnectTimer(time.AfterFunc(
			time.Duration(delay)*time.Millisecond,
			func() {
				m.clearReconnectTimer()
				if m.isManualClose.Load() {
					return
				}
				m.Connect()
			},
		))
	} else {
		// 连接断开场景（网络异常、心跳超时等）
		if m.maxReconnectAttempts > 0 && m.reconnectAttempts >= m.maxReconnectAttempts {
			m.logger.Error(fmt.Sprintf("Max reconnect attempts reached (%d), giving up", m.maxReconnectAttempts))
			if m.OnError != nil {
				m.OnError(&WSReconnectExhaustedError{MaxAttempts: m.maxReconnectAttempts})
			}
			return
		}
		m.reconnectAttempts++

		delay := m.reconnectBaseDelay * (1 << (m.reconnectAttempts - 1))
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}

		m.logger.Info(fmt.Sprintf("Connection lost, reconnecting in %dms (attempt %d/%d)...", delay, m.reconnectAttempts, m.maxReconnectAttempts))
		if m.OnReconnecting != nil {
			m.OnReconnecting(m.reconnectAttempts)
		}

		m.setReconnectTimer(time.AfterFunc(
			time.Duration(delay)*time.Millisecond,
			func() {
				m.clearReconnectTimer()
				if m.isManualClose.Load() {
					return
				}
				m.Connect()
			},
		))
	}
}

// sendFrame 发送帧
func (m *WsConnectionManager) sendFrame(frame WsFrame) {
	ws := m.conn()
	if ws == nil {
		return
	}

	data, err := json.Marshal(frame)
	if err != nil {
		m.logger.Error("Failed to marshal frame: " + err.Error())
		return
	}

	m.writeMu.Lock()
	err = ws.WriteMessage(websocket.TextMessage, data)
	m.writeMu.Unlock()
	if err != nil {
		m.logger.Error("Failed to send frame: " + err.Error())
	}
}

// Send 发送数据帧
func (m *WsConnectionManager) Send(frame WsFrame) error {
	ws := m.conn()
	if ws == nil {
		return fmt.Errorf("WebSocket not connected, unable to send data")
	}

	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return ws.WriteMessage(websocket.TextMessage, data)
}

// SendReply 通过 WebSocket 通道发送回复消息
func (m *WsConnectionManager) SendReply(reqID string, body interface{}, cmd string) (*WsFrame, error) {
	if cmd == "" {
		cmd = WsCmd.RESPONSE
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	frame := WsFrame{
		Cmd: cmd,
		Headers: WsFrameHeaders{
			ReqID: reqID,
		},
		Body: bodyJSON,
	}

	// 放入队列
	item := replyQueueItem{
		frame: frame,
	}

	// 使用 channel 来实现异步等待
	resultChan := make(chan *WsFrame, 1)
	errChan := make(chan error, 1)

	item.resolve = func(ack *WsFrame) {
		resultChan <- ack
	}
	item.reject = func(err error) {
		errChan <- err
	}

	m.enqueueReply(reqID, item)

	// 等待结果
	select {
	case ack := <-resultChan:
		return ack, nil
	case err := <-errChan:
		return nil, err
	}
}

// enqueueReply 将回复放入队列
func (m *WsConnectionManager) enqueueReply(reqID string, item replyQueueItem) {
	m.replyQueuesMu.Lock()
	defer m.replyQueuesMu.Unlock()

	if _, ok := m.replyQueues[reqID]; !ok {
		m.replyQueues[reqID] = []replyQueueItem{}
	}

	queue := m.replyQueues[reqID]

	// 防止队列无限增长
	if len(queue) >= m.maxReplyQueueSize {
		m.logger.Warn(fmt.Sprintf("Reply queue for reqId %s exceeds max size (%d), rejecting new message", reqID, m.maxReplyQueueSize))
		item.reject(fmt.Errorf("reply queue for reqId %s exceeds max size", reqID))
		return
	}

	queue = append(queue, item)
	m.replyQueues[reqID] = queue

	// 如果队列中只有这一条，立即处理
	if len(queue) == 1 {
		go m.processReplyQueue(reqID)
	}
}

// processReplyQueue 处理回复队列
func (m *WsConnectionManager) processReplyQueue(reqID string) {
	m.replyQueuesMu.Lock()
	queue, ok := m.replyQueues[reqID]
	if !ok || len(queue) == 0 {
		m.replyQueuesMu.Unlock()
		return
	}

	item := queue[0]
	m.replyQueuesMu.Unlock()

	// 发送帧
	if err := m.Send(item.frame); err != nil {
		m.logger.Error(fmt.Sprintf("Failed to send reply for reqId %s: %s", reqID, err.Error()))

		m.replyQueuesMu.Lock()
		if q, exists := m.replyQueues[reqID]; exists && len(q) > 0 {
			m.replyQueues[reqID] = q[1:]
		}
		m.replyQueuesMu.Unlock()

		item.reject(err)
		// 异步继续处理下一条，避免同步递归栈溢出
		go m.processReplyQueue(reqID)
		return
	}

	m.logger.Debug(fmt.Sprintf("Reply message sent via WebSocket, reqId: %s", reqID))

	// 设置回执超时
	m.addPendingAck(reqID, item.resolve, item.reject)
}

// addPendingAck 添加待回执项
func (m *WsConnectionManager) addPendingAck(reqID string, resolve func(*WsFrame), reject func(error)) {
	m.pendingAcksMu.Lock()
	defer m.pendingAcksMu.Unlock()

	// 如果已存在，先清理
	if existing, ok := m.pendingAcks[reqID]; ok {
		existing.timer.Stop()
	}

	// 分配唯一序列号，用于超时回调中校验是否是当前 pending
	seq := atomic.AddUint64(&m.pendingAckSeq, 1)

	timer := time.AfterFunc(
		time.Duration(replyAckTimeout)*time.Millisecond,
		func() {
			m.handleReplyAckTimeout(reqID, seq)
		},
	)

	m.pendingAcks[reqID] = &pendingAck{
		resolve: resolve,
		reject:  reject,
		timer:   timer,
		seq:     seq,
	}
}

// hasPendingAck 检查是否有待回执项
func (m *WsConnectionManager) hasPendingAck(reqID string) bool {
	m.pendingAcksMu.Lock()
	defer m.pendingAcksMu.Unlock()
	_, ok := m.pendingAcks[reqID]
	return ok
}

// handleReplyAck 处理回复回执
func (m *WsConnectionManager) handleReplyAck(reqID string, frame *WsFrame) {
	m.pendingAcksMu.Lock()
	pending, ok := m.pendingAcks[reqID]
	if !ok {
		m.pendingAcksMu.Unlock()
		return
	}

	// 清除超时定时器
	pending.timer.Stop()
	delete(m.pendingAcks, reqID)
	m.pendingAcksMu.Unlock()

	// 从队列中移除
	m.replyQueuesMu.Lock()
	if q, exists := m.replyQueues[reqID]; exists && len(q) > 0 {
		m.replyQueues[reqID] = q[1:]
		if len(m.replyQueues[reqID]) == 0 {
			delete(m.replyQueues, reqID)
		}
	}
	m.replyQueuesMu.Unlock()

	if frame.ErrCode != 0 {
		m.logger.Warn(fmt.Sprintf("Reply ack error: reqId=%s, errcode=%d, errmsg=%s", reqID, frame.ErrCode, frame.ErrMsg))
		pending.reject(fmt.Errorf("reply ack error: %s (code: %d)", frame.ErrMsg, frame.ErrCode))
	} else {
		m.logger.Debug(fmt.Sprintf("Reply ack received for reqId: %s", reqID))
		pending.resolve(frame)
	}

	// 继续处理队列中的下一条
	m.processReplyQueue(reqID)
}

// handleReplyAckTimeout 处理回复超时
func (m *WsConnectionManager) handleReplyAckTimeout(reqID string, seq uint64) {
	m.pendingAcksMu.Lock()
	pending, ok := m.pendingAcks[reqID]
	if !ok {
		m.pendingAcksMu.Unlock()
		return
	}

	// 校验 seq：如果不匹配，说明这是过期的超时回调，直接忽略
	if pending.seq != seq {
		m.pendingAcksMu.Unlock()
		return
	}

	delete(m.pendingAcks, reqID)
	m.pendingAcksMu.Unlock()

	m.logger.Warn(fmt.Sprintf("Reply ack timeout (%dms) for reqId: %s", replyAckTimeout, reqID))

	// 从队列中移除
	m.replyQueuesMu.Lock()
	if q, exists := m.replyQueues[reqID]; exists && len(q) > 0 {
		m.replyQueues[reqID] = q[1:]
		if len(m.replyQueues[reqID]) == 0 {
			delete(m.replyQueues, reqID)
		}
	}
	m.replyQueuesMu.Unlock()

	pending.reject(fmt.Errorf("reply ack timeout (%dms) for reqId: %s", replyAckTimeout, reqID))

	// 继续处理队列中的下一条
	m.processReplyQueue(reqID)
}

// clearPendingMessages 清理所有待处理的消息
func (m *WsConnectionManager) clearPendingMessages(reason string) {
	// 收集所有已在 pendingAcks 中的 reject 函数引用，用于后续去重
	pendingRejects := make(map[*func(error)]struct{})

	m.pendingAcksMu.Lock()
	for reqID, pending := range m.pendingAcks {
		pending.timer.Stop()
		pendingRejects[&pending.reject] = struct{}{}
		pending.reject(fmt.Errorf("%s, reply for reqId: %s cancelled", reason, reqID))
	}
	m.pendingAcks = make(map[string]*pendingAck)
	m.pendingAcksMu.Unlock()

	m.replyQueuesMu.Lock()
	for reqID, queue := range m.replyQueues {
		for _, item := range queue {
			// 跳过已经在 pendingAcks 中被 reject 过的队首 item，避免双重 reject
			if _, alreadyRejected := pendingRejects[&item.reject]; alreadyRejected {
				continue
			}
			item.reject(fmt.Errorf("%s, reply for reqId: %s cancelled", reason, reqID))
		}
	}
	m.replyQueues = make(map[string][]replyQueueItem)
	m.replyQueuesMu.Unlock()
}

// Disconnect 断开连接
func (m *WsConnectionManager) Disconnect() {
	m.isManualClose.Store(true)
	m.cancel()

	m.stopHeartbeat()
	m.clearPendingMessages("Connection manually closed")

	// 取消挂起的重连定时器
	m.stopReconnectTimer()

	m.closeConn()

	m.logger.Info("WebSocket connection manually closed")
}

// IsConnected 获取当前连接状态
func (m *WsConnectionManager) IsConnected() bool {
	return m.conn() != nil
}
