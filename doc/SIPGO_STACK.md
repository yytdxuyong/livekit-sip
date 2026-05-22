# sipgo SIP 协议栈设计文档

本文档详细分析 **sipgo**（livekit fork：`github.com/livekit/sipgo`）SIP 协议栈的实现架构。sipgo 是 LiveKit SIP Bridge 使用的核心 SIP 协议栈，采用纯 Go 语言实现，完全遵循 RFC 3261 规范。

> 基础库来源：[github.com/emiago/sipgo](https://github.com/emiago/sipgo) v1.2.1
> LiveKit Fork：[github.com/livekit/sipgo](https://github.com/livekit/sipgo) v0.13.2

---

## 目录

1. [架构概述](#1-架构概述)
2. [SIP 消息层](#2-sip-消息层)
3. [SIP 解析器](#3-sip-解析器)
4. [传输层](#4-传输层)
5. [事务层](#5-事务层)
6. [UA / Server / Client](#6-ua--server--client)
7. [RFC 3261 定时器实现](#7-rfc-3261-定时器实现)
8. [sipgo 与 livekit-sip 的集成](#8-sipgo-与-livekit-sip-的集成)
9. [与 livekit-sip 原始 fork 的差异](#9-与-livekit-sip-原始-fork-的差异)
10. [关键设计决策](#10-关键设计决策)

---

## 1. 架构概述

### 1.1 分层架构

sipgo 采用标准的 SIP 分层架构（RFC 3261），从底到顶分为四层：

```
┌─────────────────────────────────────────────────────────────┐
│                   应用层 (User Application)                    │
│              livekit-sip (Server/Service/Client)              │
├─────────────────────────────────────────────────────────────┤
│                    UA 层 (sipgo)                               │
│              UserAgent / Server / Client                      │
├─────────────────────────────────────────────────────────────┤
│                事务层 (transaction.Layer)                      │
│         ServerTransaction / ClientTransaction                 │
│         INVITE/Non-INVITE FSM - RFC 3261 17章                │
├─────────────────────────────────────────────────────────────┤
│                传输层 (transport.Layer)                        │
│    UDP / TCP / TLS / WS / WSS  Transport                     │
│    连接池 / DNS 解析 / 消息收发                                │
├─────────────────────────────────────────────────────────────┤
│                SIP 消息层 (sip package)                        │
│    Request / Response / Header / URI / Parser                │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 包依赖关系

```
sipgo                    (facade: 整合 UA/Server/Client)
  ├── sip                (核心类型: Message, Request, Response, Header, URI, Parser)
  ├── transaction        (事务层: ServerTx, ClientTx, FSM)
  └── transport          (传输层: UDP, TCP, TLS, WS, WSS)

livekit-sip (调用方)
  └── sipgo              (作为整体 SIP 协议栈使用)
```

### 1.3 代码结构

```
github.com/livekit/sipgo/
├── ua.go                # UserAgent - 顶层管理器
├── server.go            # Server - 服务端封装
├── client.go            # Client - 客户端封装
├── sip/
│   ├── sip.go           # 常量、通用工具（GenerateBranch、默认端口）
│   ├── message.go       # Message 接口定义
│   ├── request.go       # Request 结构体（SIP 请求）
│   ├── response.go      # Response 结构体（SIP 响应）
│   ├── headers.go       # 各种 Header 类型（To, From, Via, Contact, CSeq 等）
│   ├── header_params.go # Header 参数解析
│   ├── uri.go           # URI 结构体
│   ├── transport.go     # 传输抽象、地址定义
│   ├── dialog.go        # Dialog 管理
│   ├── parser.go        # SIP 消息解析器
│   ├── parse_uri.go     # URI 解析（FSM）
│   ├── parse_header.go  # Header 解析
│   ├── parse_address.go # 地址解析
│   ├── parse_via.go     # Via 头解析
│   └── utils.go         # 工具函数
├── transaction/
│   ├── layer.go         # 事务层（消息分发）
│   ├── transaction.go   # 事务基类、Key 生成
│   ├── server_tx.go     # 服务端事务
│   ├── server_tx_fsm.go # 服务端事务 FSM
│   ├── client_tx.go     # 客户端事务
│   └── client_tx_fsm.go # 客户端事务 FSM
└── transport/
    ├── layer.go         # 传输层（消息分发）
    ├── conn.go          # 连接抽象
    ├── connection_pool.go # 连接池
    ├── udp.go           # UDP 传输
    ├── tcp.go           # TCP 传输
    ├── tls.go           # TLS 传输
    └── ws.go / wss.go   # WebSocket 传输
```

---

## 2. SIP 消息层

### 2.1 消息类型

```go
// Message 是所有 SIP 消息的接口
type Message interface {
    SipVersion() string
    SetSipVersion(v string)
    
    // Header 操作
    GetHeaders(name string) []Header     // 获取所有同名头
    GetHeader(name string) Header        // 获取第一个同名头
    AppendHeader(h Header)               // 添加头
    RemoveHeader(name string)            // 删除所有同名头
    ReplaceHeader(h Header)              // 替换第一个同名头
    Headers() []Header                   // 获取所有头
    
    // 特定 Header 快捷方法
    Via() *ViaHeader
    From() *FromHeader
    To() *ToHeader
    CallID() *CallIDHeader
    CSeq() *CSeqHeader
    Contact() *ContactHeader
    
    // Body 操作
    Body() []byte
    SetBody(body []byte)
    
    // 传输信息
    Transport() string
    SetTransport(transport string)
    Source() string
    SetSource(addr string)
    Destination() string
    SetDestination(addr string)
}
```

### 2.2 Request 结构体

```go
type Request struct {
    MessageData     // 嵌入的消息数据（Headers + Body）
    Method    RequestMethod  // 方法名: INVITE, BYE, ACK, CANCEL, OPTIONS 等
    Recipient Uri            // 请求 URI（sip:user@host）
    
    Laddr Addr               // 本地地址
    raddr Addr               // 远程地址（Via 解析后）
}
```

**请求行格式**（RFC 3261 §7.1）：
```
INVITE sip:user@host SIP/2.0
```

### 2.3 Response 结构体

```go
type Response struct {
    MessageData
    Reason     string   // 原因短语: "OK", "Not Found", "Unauthorized"
    StatusCode int      // 状态码: 200, 404, 401 等
    
    raddr Addr          // 远程地址
}
```

**状态行格式**（RFC 3261 §7.2）：
```
SIP/2.0 200 OK
```

### 2.4 URI 结构体

```go
type Uri struct {
    Scheme string        // "sip" 或 "sips"
    User   string        // 用户部分
    Password string      // 密码
    Host   string        // 主机名或 IP
    Port   int           // 端口
    
    UriParams    Params  // URI 参数（transport=tcp 等）
    Headers      Params  // URI 头部
    FuzzyHost    bool    // 模糊主机
    Wildcard     bool    // 通配符 (*)
}
```

**URI 解析 FSM**（`parse_uri.go`）：

```
输入字符串: "sip:user:pass@host:5060;transport=tcp?header=value"
               │
               ▼
    uriStateScheme → 识别 scheme ("sip")
               │
               ▼
    uriStateSlashes → 跳过 "//" (可选)
               │
               ▼
    uriStateUser → 解析 user:pass (分离 ':')
               │
               ▼
    uriStateHost → 解析 host (支持 IPv6)
               │
               ▼
    uriStatePort → 解析 :port
               │
               ▼
    uriStateParams → 解析 URI 参数 (key=value)
               │
               ▼
    uriStateHeaders → 解析 URI 头部
```

### 2.5 Header 体系

```go
type Header struct {
    Name  string   // 头部名称（不区分大小写）
    Value string   // 头部值（原始字符串）
}
```

**预定义 Header 类型**：

| 结构体 | 对应 SIP Header | RFC 参考 |
|--------|----------------|---------|
| ToHeader | To | RFC 3261 §8.1.1.2 |
| FromHeader | From | RFC 3261 §8.1.1.3 |
| ViaHeader | Via | RFC 3261 §18.2.1 |
| ContactHeader | Contact | RFC 3261 §8.1.1.8 |
| CallIDHeader | Call-ID | RFC 3261 §8.1.1.4 |
| CSeqHeader | CSeq | RFC 3261 §8.1.1.5 |
| MaxForwardsHeader | Max-Forwards | RFC 3261 §8.1.1.6 |
| ContentLengthHeader | Content-Length | RFC 3261 §20.14 |
| ContentTypeHeader | Content-Type | RFC 3261 §20.15 |
| RouteHeader | Route | RFC 3261 §20.34 |
| RecordRouteHeader | Record-Route | RFC 3261 §20.30 |
| ExpiresHeader | Expires | RFC 3261 §20.19 |

**Header 参数**（`header_params.go`）：
每个 Header 可以带参数，如 `From: <sip:user@host>;tag=abc123`。通过 `Params` 结构体管理参数键值对。

**消息序列化**：

```
Request.String() 输出示例:
INVITE sip:user@host SIP/2.0\r\n
Via: SIP/2.0/UDP 192.168.1.1:5060;branch=z9hG4bK...\r\n
From: <sip:caller@host>;tag=abc\r\n
To: <sip:callee@host>\r\n
Call-ID: uuid@host\r\n
CSeq: 1 INVITE\r\n
Max-Forwards: 70\r\n
Content-Type: application/sdp\r\n
Content-Length: 100\r\n
\r\n
v=0\r\n
... (SDP body)
```

---

## 3. SIP 解析器

### 3.1 Parser 接口

```go
type Parser struct {
    headersParsers   HeadersParser   // header 解析器映射
    MaxMessageLength int             // 最大消息长度（默认 65535）
}
```

### 3.2 解析流程

每次接收 SIP 消息时，按照以下逻辑解析：

```go
func (p *Parser) ParseSIP(data []byte) (Message, error) {
    // 1. 检查消息长度
    if len(data) > p.MaxMessageLength {
        return nil, ErrMessageTooLarge
    }
    
    // 2. 解析起始行（区分 Request/Response）
    //    如果是请求: 解析 Method + Recipient + SIPVersion
    //    如果是响应: 解析 SIPVersion + StatusCode + Reason
    msg, total, err := p.parseStartLine(data, stream)
    
    // 3. 解析所有 Header
    //    使用 header parsers map 逐个解析
    n, err := p.parseHeadersOnly(msg, data)
    
    // 4. 解析 Body
    //    根据 Content-Length 读取消息体
    p.parseBody(msg, data, contentLength)
    
    return msg, nil
}
```

### 3.3 起始行解析

```go
func (p *Parser) parseStartLine(data []byte, stream bool) (Message, int, error) {
    // SIP 请求以方法名开头: INVITE, BYE, ACK, CANCEL...
    // SIP 响应以 "SIP/2.0" 开头
    
    // 判断逻辑:
    // 如果第一个单词以 "SIP/" 开头 → Response
    // 否则 → Request
    
    line, n, err := readLine(data)
    
    // 对 Request: 分割成 Method + Recipient + Version
    // 对 Response: 分割成 Version + StatusCode + Reason
}
```

### 3.4 Header 解析

```go
// DefaultHeadersParser() 返回默认的 header 解析器映射
// key = header 名称（小写），value = HeaderParser 函数

func DefaultHeadersParser() map[string]HeaderParser {
    return map[string]HeaderParser{
        "via":              parseViaHeader,
        "from":             parseFromHeader,
        "to":               parseToHeader,
        "call-id":          parseCallIdHeader,
        "cseq":             parseCSeqHeader,
        "contact":          parseContactHeader,
        "route":            parseRouteHeader,
        "record-route":     parseRecordRouteHeader,
        "max-forwards":     parseMaxForwardsHeader,
        "content-length":   parseContentLengthHeader,
        "content-type":     parseContentTypeHeader,
        "expires":          parseExpiresHeader,
    }
}
```

**Header 解析优化**：
- 使用 `headersParsers map[string]HeaderParser` 快速查找
- 未注册的 Header 作为通用 Header（raw name/value）保存
- 可通过 `WithHeadersParsers()` 自定义解析器列表提升性能

### 3.5 Via 头解析

Via 头是 SIP 中最复杂的 Header，包含协议、地址、分支参数等。`parse_via.go` 实现专用的 Via 解析器：

```go
type ViaHeader struct {
    Protocol  string    // "SIP/2.0"
    Transport string    // "UDP", "TCP", "TLS"
    Host      string    // 发送方主机
    Port      int       // 发送方端口
    Params    Params    // branch, rport, received 等参数
}
```

### 3.6 性能优化

| 优化技术 | 实现方式 |
|---------|---------|
| 预定义解析器 | 常用 Header 使用专用 parser，未注册的使用通用 parser |
| 解析器可配置 | 可自定义 headerParsers map，减少不必要解析 |
| 消息长度限制 | 默认 65535 bytes 防止内存攻击 |
| 流式解析 | 支持分段解析（stream=true） |
| 零拷贝 | Header 值以原始字符串保存，避免不必要的字符串转换 |

---

## 4. 传输层

### 4.1 Layer 架构

```go
type Layer struct {
    log       *slog.Logger
    handlers  []sip.MessageHandler   // 消息回调链
    
    udp *UDPTransport    // UDP 传输
    tcp *TCPTransport    // TCP 传输
    tls *TLSTransport    // TLS 传输
    ws  *WSTransport     // WebSocket 传输
    wss *WSSTransport    // Secure WebSocket 传输
    
    transports  map[string]Transport  // 快速访问映射
    listenPorts map[string][]int      // 监听端口记录
    
    dnsResolver   *net.Resolver       // DNS 解析器
    ConnectionReuse bool              // 连接复用（默认 true）
}
```

### 4.2 传输协议支持

| 协议 | 实现文件 | 传输方式 | 默认端口 | 可靠性 |
|------|---------|---------|---------|--------|
| UDP | `transport/udp.go` | 无连接 | 5060 | 不可靠 |
| TCP | `transport/tcp.go` | 面向连接 | 5060 | 可靠 |
| TLS | `transport/tls.go` | 面向连接+加密 | 5061 | 可靠 |
| WS | `transport/ws.go` | WebSocket | 80 | 可靠 |
| WSS | `transport/wss.go` | WebSocket+TLS | 443 | 可靠 |

### 4.3 消息流

```
网络
  │
  ▼
┌─────────────────────────────────────┐
│   UDP/TCP/TLS 监听器                 │
│   接收原始字节 ([]byte)               │
└──────────┬──────────────────────────┘
           │ raw bytes
           ▼
┌─────────────────────────────────────┐
│   传输层 (Layer.handleMessage)       │
│   通知所有注册的 handler              │
│   (handler 列表，支持链式调用)         │
└──────────┬──────────────────────────┘
           │ parsed Message (Request/Response)
           ▼
┌─────────────────────────────────────┐
│   事务层 (transaction.Layer)         │
│   分发到对应的 ServerTx / ClientTx   │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   应用层 (Server onRequest)          │
│   调用回调: onInvite, onBye 等       │
└─────────────────────────────────────┘
```

### 4.4 连接管理

#### UDP 连接
```go
type UDPTransport struct {
    conn     net.PacketConn    // UDP socket
    parser   *sipgo.Parser     // SIP 解析器
    pool     *connectionPool   // 连接池（地址映射）
    handlers []MessageHandler  // 消息回调链
}
```

**UDP 接收协程**：
```
goroutine:
  循环:
    buf = make([]byte, TransportBufferReadSize)  // 32768
    n, addr, err = conn.ReadFrom(buf)
    msg, err = parser.ParseSIP(buf[:n])
    msg.SetSource(addr)
    遍历 handlers: handler(msg)
```

#### TCP 连接
```go
type TCPTransport struct {
    listeners  []net.Listener         // 监听器
    parser     *sipgo.Parser
    pool       *connectionPool        // 连接池
    tcpConfig  *TCPConfig             // TCP 配置
    handlers   []MessageHandler
}
```

**TCP 难点**：SIP over TCP 需要处理消息边界（RFC 3261 §18.3）：
- 通过 Content-Length Header 确定消息结束位置
- 使用 `parser_stream.go` 的流式解析器处理粘包

```go
type StreamParser struct {
    parser  *Parser
    buffer  bytes.Buffer
}
// parseHeaders(data) 返回已消费的字节数 n
// 剩余字节是下一条消息的开始
```

#### 连接池 (`connection_pool.go`)

```go
type connectionPool struct {
    mu     sync.Mutex
    conns  map[string]Connection  // 地址 → 连接
}
```

连接池管理：
- 通过 `GetConnection(addr)` 复用连接
- 通过 `CreateConnection(laddr, raddr)` 新建连接
- 连接生命周期由 `TransportIdleConnection` 控制
  - `-1`：单次请求/响应后关闭
  - `0`：事务结束后立即关闭
  - `1`（默认）：事务结束后保持连接

### 4.5 DNS 解析

传输层使用 Go 标准库的 `net.Resolver` 进行 DNS 解析：
- SRV 记录：查询 SIP 服务的 SRV 记录（`_sip._udp.example.com`）
- A/AAAA 记录：回退到普通地址解析

---

## 5. 事务层

### 5.1 Layer 职责

```go
type Layer struct {
    log                 *slog.Logger
    tpl                 *transport.Layer
    
    reqHandler          RequestHandler           // 请求处理器
    unRespHandler       UnhandledResponseHandler // 未匹配响应处理器
    
    clientTransactions  *transactionStore        // 客户端事务存储
    serverTransactions  *transactionStore        // 服务端事务存储
}
```

**消息分发流程**：

```go
func (txl *Layer) handleMessage(msg sip.Message) {
    switch msg := msg.(type) {
    case *sip.Request:
        // 1. 生成 ServerTxKey
        // 2. 尝试匹配已有 ServerTx
        // 3. 如果是重传（匹配），交由已有 ServerTx 处理
        // 4. 如果是新请求，创建新 ServerTx
        // 5. 对于 CANCEL，创建后立即终止
        // 6. 调用 reqHandler 回调
        txl.handleRequest(req)
    
    case *sip.Response:
        // 1. 生成 ClientTxKey
        // 2. 尝试匹配已有 ClientTx
        // 3. 如果匹配，交由 ClientTx 处理
        // 4. 如果不匹配，调用 unRespHandler
        txl.handleResponse(res)
    }
}
```

### 5.2 服务端事务 (ServerTx) — 状态机

服务端事务实现 RFC 3261 §17.2 定义的三种 FSM：

#### INVITE 服务端事务 FSM

```
                   ┌──────────┐
                   │  Trying   │ ◄── 收到 INVITE
                   └────┬─────┘
                        │ 1xx 响应
                        ▼
              ┌───────────────────┐
              │   Proceeding      │ ◄── 1xx/重传 INVITE
              └───┬───────┬───────┘
                  │       │
          2xx 响应│       │ 300+ 响应
                  ▼       ▼
         ┌──────────┐  ┌────────────┐
         │ Accepted │  │ Completed  │ ◄── Timer G (重传响应)
         │(RFC 6026)│  └──────┬─────┘
         └────┬─────┘        │ ACK 收到
              │              ▼
              │       ┌────────────┐
              │       │ Confirmed  │ ◄── Timer I (等待 ACK)
              │       └──────┬─────┘
              │              │ Timer I 超时
              │              ▼
              │       ┌────────────┐
              └──────→│ Terminated │
                      └────────────┘
```

**关键定时器**：

| 定时器 | 触发条件 | RFC 参考 |
|-------|---------|---------|
| Timer G | 重传 300+ 响应（递增间隔，上限 T2） | RFC 3261 §17.2.1 |
| Timer H | 等待 ACK 超时（64\*T1） | RFC 3261 §17.2.1 |
| Timer I | Confirmed 状态超时（T4），等待 ACK 重传结束 | RFC 3261 §17.2.1 |
| Timer L | Accepted 状态超时（64\*T1），RFC 6026 | RFC 6026 §7.1 |

#### 非 INVITE 服务端事务 FSM

```
    ┌──────────┐
    │  Trying  │ ◄── 收到请求
    └────┬─────┘
         │ 1xx/2xx/300+
         ▼
    ┌───────────┐
    │ Completed │ ◄── Timer J (64*T1)
    └─────┬─────┘
          │ Timer J 超时
          ▼
    ┌───────────┐
    │ Terminated│
    └───────────┘
```

### 5.3 客户端事务 (ClientTx) — 状态机

#### INVITE 客户端事务 FSM

```
    ┌──────────┐
    │  Calling  │ ◄── 发送 INVITE
    └────┬─────┘
         │
    ┌────┴────┐
    │ 1xx recv│ ← Timer A (重传 INVITE)
    │ 继续等待│ ← Timer B (超时)
    └────┬─────┘
         │ 2xx 响应
         ▼
    ┌──────────┐
    │ Completed │ ← 收到 200 OK，发送 ACK
    └────┬─────┘
         │ Timer D (等待重传的 200)
         ▼
    ┌───────────┐
    │ Terminated│
    └───────────┘
```

**关键定时器**：

| 定时器 | 触发条件 | RFC 参考 |
|-------|---------|---------|
| Timer A | UDP 下重传 INVITE（每次翻倍，上限 T2） | RFC 3261 §17.1.1.2 |
| Timer B | 等待响应超时（64\*T1） | RFC 3261 §17.1.1.2 |
| Timer D | Completed 等待重传的 200 OK 超时 | RFC 3261 §17.1.1.2 |
| Timer M | 2xx 响应等待 ACK 重传超时（64\*T1） | RFC 6026 |

#### 非 INVITE 客户端事务 FSM

```
    ┌──────────┐
    │  Trying  │ ◄── 发送请求
    └────┬─────┘
         │
    ┌────┴────┐
    │ 1xx recv│ ← Timer E (重传请求)
    │ 继续等待│ ← Timer F (超时)
    └────┬─────┘
         │ 2xx/300+
         ▼
    ┌───────────┐
    │ Completed │
    └─────┬─────┘
          │ Timer K (T4)
          ▼
    ┌───────────┐
    │ Terminated│
    └───────────┘
```

### 5.4 事务 Key 生成

sipgo 使用 RFC 3261 定义的规则生成事务 Key：

```go
// 服务端事务 Key（RFC 3261 §17.2.3）
// RFC 3261: branch + host + port + method
// RFC 2543 兼容: fromTag + callId + method + cseq + via
func MakeServerTxKey(msg) string {
    via := msg.Via()
    branch := via.Params.Get("branch")
    
    if branch 是 RFC 3261 格式 {
        key = branch + "__" + via.Host + "__" + port + "__" + method
    } else {
        key = fromTag + "__" + callId + "__" + method + "__" + cseq + "__" + via
    }
}

// 客户端事务 Key（RFC 3261 §17.1.3）
func MakeClientTxKey(msg) string {
    key = branch + "__" + method
}
```

### 5.5 FSM 实现模式

sipgo 使用 **函数指针** 实现有限状态机（而非 switch-case）：

```go
// FSMfunc 是状态处理函数
type FSMfunc func() FSMfunc

// ServerTx 的状态字段
func (tx *ServerTx) inviteStateProceeding(s FsmInput) FsmInput { ... }
func (tx *ServerTx) inviteStateCompleted(s FsmInput) FsmInput   { ... }
func (tx *ServerTx) inviteStateConfirmed(s FsmInput) FsmInput   { ... }

// 状态切换（spinFsm）
func (tx *ServerTx) spinFsm(in fsmInput) {
    // 根据当前状态和处理结果切换到下一状态
    tx.fsmState( in ) → 返回新的 fsmState
}
```

---

## 6. UA / Server / Client

### 6.1 UserAgent

```go
type UserAgent struct {
    log         *slog.Logger
    name        string           // UA 名称 ("LiveKit")
    ip          net.IP           // 本地 IP
    dnsResolver *net.Resolver    // DNS 解析器
    tcpConfig   *TCPConfig       // TCP 配置
    tlsConfig   *tls.Config      // TLS 配置
    tp          *transport.Layer // 传输层
    tx          *transaction.Layer // 事务层
}
```

**创建流程**：
```go
func NewUA(options ...UserAgentOption) (*UserAgent, error) {
    // 1. 创建 UA 对象，设置默认值
    ua = &UserAgent{
        name: "sipgo",
        dnsResolver: net.DefaultResolver,
    }
    
    // 2. 应用选项
    for _, o := range options { o(ua) }
    
    // 3. 如果没有指定 IP，自动检测
    if ua.ip == nil { ua.ip = sip.ResolveSelfIP() }
    
    // 4. 创建传输层（含 UDP/TCP/TLS/WS/WSS）
    ua.tp = transport.NewLayer(..., sipgo.NewParser(), ...)
    
    // 5. 创建事务层（与传输层关联）
    ua.tx = transaction.NewLayer(ua.log, ua.tp)
    
    return ua, nil
}
```

### 6.2 Server

```go
type Server struct {
    *UserAgent
    requestHandlers map[sip.RequestMethod]RequestHandler
    // 如: INVITE → onInvite, BYE → onBye, OPTIONS → onOptions
    
    noRouteHandler   RequestHandler    // 无匹配方法时的处理
    log              *slog.Logger
}
```

**请求注册方式**：
```go
srv.OnInvite(s.onInvite)    // 注册 INVITE 处理
srv.OnBye(s.onBye)          // 注册 BYE 处理
srv.OnAck(s.onAck)          // 注册 ACK 处理
srv.OnNotify(s.onNotify)    // 注册 NOTIFY 处理
srv.OnNoRoute(s.onNoRoute)  // 注册无路由处理
```

**消息分发**：
```go
// 事务层收到请求 → 调用 Server.onRequest
func (s *Server) onRequest(log *slog.Logger, req *sip.Request, tx sip.ServerTransaction) {
    if handler, ok := s.requestHandlers[req.Method]; ok {
        handler(log, req, tx)  // 调用对应的处理器
    } else {
        s.noRouteHandler(log, req, tx)  // 无匹配处理器
    }
}
```

### 6.3 Client

```go
type Client struct {
    *UserAgent
    host string   // 默认路由主机
    port int      // 默认路由端口
    log  *slog.Logger
}
```

**核心方法**：

```go
// 发送请求并获取事务（用于非 ACK 请求）
func (c *Client) TransactionRequest(req, options) (ClientTransaction, error) {
    // 1. 自动补充缺失的 Header（To, From, CSeq, Call-ID, Max-Forwards, Via）
    if len(options) == 0 {
        clientRequestBuildReq(c, req)
    }
    // 2. 通过事务层发送
    return c.tx.Request(req)
}

// 直接发送请求（用于 ACK）
func (c *Client) WriteRequest(req, options) error {
    // 直接通过传输层发送，不经过事务层
}
```

**自动 Header 补充**（`clientRequestBuildReq`）：
- To：如果未设置，使用 Recipient
- From：如果未设置，使用 sip:host:port
- CSeq：自动分配序号
- Call-ID：生成 UUID
- Max-Forwards：默认 70
- Via：自动添加（含 branch 参数）

---

## 7. RFC 3261 定时器实现

sipgo 严格遵循 RFC 3261 定义的 SIP 定时器：

### 7.1 默认定时器值

```
T1 = 500ms     (RTT 估计，可配置)
T2 = 4s        (最大重传间隔)
T4 = 5s        (消息在网络中的最长存活时间)
```

### 7.2 导出定时器

| 定时器 | 默认值 | 触发条件 |
|--------|-------|---------|
| Timer_A | T1 (500ms) | 客户端 INVITE 重传初始间隔 |
| Timer_B | 64\*T1 (32s) | 客户端 INVITE 超时 |
| Timer_D | 32s | 客户端 Completed 状态超时 |
| Timer_E | T1 (500ms) | 客户端非 INVITE 重传初始间隔 |
| Timer_F | 64\*T1 (32s) | 客户端非 INVITE 超时 |
| Timer_G | T1 (500ms) | 服务端 300+ 响应重传初始间隔 |
| Timer_H | 64\*T1 (32s) | 服务端 INVITE 等待 ACK 超时 |
| Timer_I | T4 (5s) | 服务端 Confirmed 状态超时 |
| Timer_J | 64\*T1 (32s) | 服务端非 INVITE Completed 超时 |
| Timer_K | T4 (5s) | 客户端非 INVITE Completed 超时 |
| Timer_L | 64\*T1 (32s) | 服务端 Accepted 状态超时（RFC 6026） |
| Timer_M | 64\*T1 (32s) | 客户端 INVITE 2xx 重传超时（RFC 6026） |

### 7.3 定时器使用

```go
// 定时器通过 time.AfterFunc 实现
// 示例：客户端 INVITE 重传
func (tx *ClientTx) Init() {
    if unreliable {
        tx.timer_a = time.AfterFunc(tx.timer_a_time, func() {
            tx.spinFsm(client_input_timer_a)  // 触发重传
        })
    }
    tx.timer_b = time.AfterFunc(Timer_B, func() {
        tx.spinFsm(client_input_timer_b)      // 触发超时
    })
}
```

---

## 8. sipgo 与 livekit-sip 的集成

### 8.1 集成架构图

```
livekit-sip (pkg/sip)
  │
  │ 创建 UA (共享)
  │
  ├── Server (pkg/sip/server.go)
  │   ├── 调用 sipgo.NewServer(ua)
  │   ├── 注册回调:
  │   │   ├── OnInvite  → s.onInvite  → processInvite
  │   │   ├── OnBye     → s.onBye     → inboundCall.Bye
  │   │   ├── OnAck     → s.onAck     → sipInbound.AcceptAck
  │   │   ├── OnNotify  → s.onNotify  → sipInbound.handleNotify
  │   │   └── OnOptions → s.onOptions → 200 OK
  │   ├── 启动监听:
  │   │   ├── startUDP(netip.AddrPort)
  │   │   ├── startTCP(netip.AddrPort)
  │   │   └── startTLS(netip.AddrPort, tlsConfig)
  │   └── 使用 sipgo.Server.ListenOnNetwork
  │
  ├── Client (pkg/sip/client.go)
  │   ├── 调用 sipgo.NewClient(ua)
  │   ├── 主动发起 INVITE
  │   │   └── c.sipCli.TransactionRequest(invite) → ClientTx
  │   ├── 发送 ACK
  │   │   └── c.sipCli.WriteRequest(ack)
  │   └── 处理 BYE 等
  │       └── OnRequest → s.cli.OnRequest
  │
  └── 共享 UA (同一端口)
      └── ua = sipgo.NewUA(options)
```

### 8.2 共享 UA 的原因

livekit-sip 的 Server 和 Client **共享同一个 UserAgent**。为什么：

```go
// 两种设计的对比:
//
// 分离 UA (两个 UA, 两个端口) ❌
// ┌──────────────┐   UDP :5060
// │ UA-Server    │─────────────
// └──────────────┘
// ┌──────────────┐   UDP :5061 (随机端口)
// │ UA-Client    │─────────────
// └──────────────┘
// 问题: 路由器通常只转发固定端口，Client 的随机端口可能穿透失败
//
// 共享 UA (一个 UA, 一个端口) ✅
// ┌──────────────────────┐   UDP :5060
// │ UA (Server + Client) │─────────────
// └──────────────────────┘
// 优点: 单端口，防火墙配置简单，路由协议兼容
//
// 共享 UA 的 #TECHNICAL_CHALLENGE:
// 服务端收到的 BYE 等请求必须由 Client 响应
// 解决方案: Server.OnNoRoute → Client.OnRequest 链式调用
```

### 8.3 Header 到 Attribute 映射集成

sipgo 的 Header 解析能力被 livekit-sip 充分利用，通过 `RemoteHeaders()` 接口获取 SIP 请求中的自定义 Header：

```go
// livekit-sip 中
func (cc *sipInbound) RemoteHeaders() Headers {
    // 从 sipgo.Request 获取所有 Header
    return cc.Request.Headers()
}

// 将第三方提供商 Header 映射到 LiveKit 参与者属性
// 如: X-Twilio-AccountSid → lk.sip.twilio.accountSid
func HeadersToAttrs(...) map[string]string { ... }

// 将参与者属性映射回 SIP Header（呼出时）
// 如: lk.sip.twilio.callSid → X-Twilio-CallSid
func AttrsToHeaders(...) map[string]string { ... }
```

### 8.4 外呼流程中的 sipgo 调用

```go
// livekit-sip 呼出关键调用链:
func (c *outboundCall) sipSignal(ctx, tid) error {
    // 1. 创建 SDP Offer
    sdpOffer, err := c.media.NewOffer(codecs, encryption)
    sdpOfferData, _ := sdpOffer.SDP.Marshal()
    
    // 2. 构建 INVITE 请求
    toUri := CreateURIFromUserAndAddress(c.sipConf.to, c.sipConf.address, transport)
    
    // 3. 调用 sipgo.Client.TransactionRequest (实际是 c.c.sipCli)
    //    通过事务层发送 INVITE，等待响应
    resp, err := c.cc.Invite(ctx, toUri, user, pass, headers, sdpOfferData, callback)
    
    // 4. sipgo 内部:
    //    a. 构建 INVITE Request（自动补充 Via/From/Call-ID/CSeq/To/Max-Forwards）
    //    b. ClientTx.Init() → 发送请求 → 启动定时器 A/B
    //    c. 等待响应 → 事务层 FSM 状态切换
    //    d. 返回最终 Response（200 OK 或其他）
    
    // 5. 处理 200 OK（含 SDP Answer）
    mc, localSDP, err := c.media.SetAnswer(sdpOffer, resp, codecs, encryption)
    
    // 6. 发送 ACK
    c.cc.Ack(resp)  // 直接通过 sipgo.Client.WriteRequest
}
```

### 8.5 呼入流程中的 sipgo 调用

```go
// livekit-sip 呼入关键调用链:
func (s *Server) processInvite(req, tx) error {
    // 1. sipgo.Server 收到 INVITE → OnInvite 回调
    //    此时 req 和 tx 已由事务层管理
    
    // 2. 鉴权、路由...
    
    // 3. 发送 200 OK（含 SDP）
    cc.Respond200(contact, sdpAnswer)
    //    → 调用 sipgo.ServerTransaction.Respond(res)
    //    → 事务层 FSM: Trying → Proceeding → Completed/Accepted
    
    // 4. 等待 ACK
    cc.WaitAck(ctx)
    //    → 从事务层的 Ack() channel 等待
    //    → 事务层 FSM: Completed → Confirmed → Terminated
    
    // 5. 媒体桥接...
}
```

---

## 9. 与 livekit-sip 原始 fork 的差异

LiveKit 维护了一个 Fork（`livekit/sipgo`），与原始 `emiago/sipgo` 相比的差异：

| 维度 | emiago/sipgo v1.2.1 | livekit/sipgo v0.13.2 |
|------|-------------------|---------------------|
| **事务层独立** | `sip/transaction.go` | 独立 `transaction/` 包 |
| **传输层独立** | `sip/transport_*.go` | 独立 `transport/` 包 |
| **Server/Client** | 内嵌在 sipgo 包中 | 独立的 `server.go`/`client.go` |
| **FSM 实现** | 代码混合在 sip 包 | 清晰分离 `server_tx_fsm.go`/`client_tx_fsm.go` |
| **RFC 6026 支持** | 基础 | 完整 Accepted 状态实现 |
| **消息解析器** | 在 sip 包内 | 在 sip 包内（略有优化） |
| **定时器** | 全局变量 | 全局常量（复用原始设置） |

LiveKit Fork 的主要变化：
1. **包结构重构** — 将 transport 和 transaction 从 `sip/` 子包提升为独立包，便于维护和测试
2. **完整的 RFC 6026 实现** — 增加 `inviteStateAccepted` 状态和 Timer_L/Timer_M
3. **Server/Client 独立** — 不再从 `sipgo.NewServer/NewClient` 构造，而是显式创建
4. **性能优化** — 移除了一些不必要的 goroutine 创建

---

## 10. 关键设计决策

1. **纯 Go 实现** — 无需 cgo 依赖，交叉编译方便，部署简单（单二进制）

2. **函数指针 FSM** — SIP 状态机使用函数指针而非 switch-case，状态更清晰，扩展更灵活

3. **共享 UA** — Server 和 Client 共享同一个 UserAgent、传输层和端口，解决 NAT/防火墙穿透问题

4. **解析器可配置** — 通过 `WithHeadersParsers` 允许调用方优化 Header 解析性能，按需减少解析的 Header 类型

5. **连接池** — TCP/TLS 连接池复用，支持 `TransportIdleConnection` 控制连接生命周期，平衡资源使用和延迟

6. **Content-Length 边界** — TCP 传输依赖 Content-Length 确定消息边界，使用 stream parser 处理粘包

7. **零拷贝策略** — Header 值尽可能以原始字符串保存，避免不必要的内存分配和拷贝

8. **UDP 重传在事务层** — 在事务层（而非传输层）处理 UDP 重传，与传输协议解耦，符合 RFC 3261 设计

9. **分支标识符** — 通过 Via 头的 branch 参数（RFC 3261 Magic Cookie）创建事务 Key，确保唯一性和消息匹配

10. **异步定时器** — 所有 SIP 定时器使用 `time.AfterFunc` 实现，避免阻塞 goroutine，支持大量并发事务