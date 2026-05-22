# LiveKit SIP Bridge 设计文档

本文档详细介绍 LiveKit SIP Bridge 服务的架构设计、核心模块、数据流程和关键实现细节。SIP Bridge 是一个独立的服务，负责将 SIP 协议（Session Initiation Protocol）与 LiveKit 实时音视频房间桥接。

---

## 目录

1. [概述](#1-概述)
2. [整体架构](#2-整体架构)
3. [运行入口与服务生命周期](#3-运行入口与服务生命周期)
4. [核心模块](#4-核心模块)
5. [SIP 呼入流程](#5-sip-呼入流程)
6. [SIP 呼出流程](#6-sip-呼出流程)
7. [媒体处理](#7-媒体处理)
8. [音频编解码](#8-音频编解码)
9. [Room 连接管理](#9-room-连接管理)
10. [统计与监控](#10-统计与监控)
11. [SIP 协议特性](#11-sip-协议特性)
12. [关键设计决策](#12-关键设计决策)

---

## 1. 概述

LiveKit SIP Bridge 是一个独立的服务，负责在 SIP 协议和 LiveKit WebRTC 之间桥接音视频通信。它实现了以下核心能力：

- **呼入（Dialing In）**：接受来自 PSTN/SIP 运营商的 INVITE 请求
- **呼出（Dialing Out）**：向外部电话网络发起 INVITE 请求
- **摘要认证（Digest Authentication）**：支持 SIP 层面的认证挑战
- **DTMF 按键**：支持发送和接收按键信号
- **呼叫转移**：支持通过 SIP REFER 进行通话转移
- **TLS 加密**：支持安全 SIP（SIPS）

### 技术栈

| 类别 | 技术选型 |
|------|----------|
| 编程语言 | Go 1.22+ |
| SIP 协议栈 | [livekit/sipgo](https://github.com/livekit/sipgo)（自定义 SIP 栈） |
| 媒体处理 | [livekit/media-sdk](https://github.com/livekit/media-sdk) |
| RPC 通信 | [livekit/psrpc](https://github.com/livekit/psrpc)（基于 Redis Pub/Sub） |
| 配置存储 | Redis |
| LiveKit SDK | [server-sdk-go](https://github.com/livekit/server-sdk-go) |
| 音频编解码 | libopus（OPUS ↔ PCM16） |
| 统计 | Prometheus、pprof |

### 代码仓库

```
https://github.com/livekit/sip
```

---

## 2. 整体架构

### 2.1 系统上下文

```
                    PSTN/电话网络
                          │
                   SIP INVITE (UDP/TCP/TLS)
                          ▼
                  ┌─────────────────┐
                  │   SIP Bridge    │
                  │ (livekit-sip)   │
                  └────────┬────────┘
                           │ psrpc RPC (通过 Redis)
                           ▼
               ┌─────────────────────────────────────────┐
               │         LiveKit Server                  │
               │  ┌───────────────────────────────┐     │
               │  │ IOInfoService (鉴权/路由匹配)   │     │
               │  └───────────────────────────────┘     │
               │  ┌───────────────────────────────┐     │
               │  │ SIPService (管理 API)        │     │
               │  └───────────────────────────────┘     │
               └──────────────────┬──────────────────────┘
                                  │ WebSocket
                                  ▼
                         ┌─────────────────┐
                         │  LiveKit Room   │
                         │                 │
                         │  ┌───────────┐ │
                         │  │ AI Agent  │ │
                         │  │  + SIP 用户│ │
                         │  └───────────┘ │
                         └─────────────────┘
```

### 2.2 SIP Bridge 内部架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        SIP Bridge (livekit-sip)                  │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │                    Service Layer (pkg/service)                ││
│  │  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐  ││
│  │  │ psrpc 服务端│ │ 健康检查/统计 │ │  优雅关闭/Call管理   │  ││
│  │  └──────┬──────┘ └──────────────┘ └──────────────────────┘  ││
│  └─────────┼────────────────────────────────────────────────────┘│
│            │                                                     │
│  ┌─────────▼────────────────────────────────────────────────────┐│
│  │                    SIP 核心层 (pkg/sip)                       ││
│  │                                                               ││
│  │  ┌──────────────────┐       ┌──────────────────┐             ││
│  │  │    Server        │       │    Client        │             ││
│  │  │  (呼入处理)      │       │  (呼出处理)      │             ││
│  │  │                  │       │                  │             ││
│  │  │ ┌─ onInvite ───┐ │       │ ┌─ Dial ──────┐ │             ││
│  │  │ └──────────────┘ │       │ └─────────────┘ │             ││
│  │  │ ┌─ onBye ──────┐ │       │ └─ Transfer ──┐ │             ││
│  │  │ └──────────────┘ │       │ └─────────────┘ │             ││
│  │  └────────┬─────────┘       └────────┬─────────┘             ││
│  │           │                         │                        ││
│  │  ┌────────▼─────────────────────────▼────────┐               ││
│  │  │          MediaPort (媒体处理核心)           │              ││
│  │  │  ┌──────────┐  ┌──────────┐  ┌─────────┐  │              ││
│  │  │  │ RTP 收发 │  │ SRTP加密 │  │ DTMF   │  │              ││
│  │  │  │ SDP交换  │  │ 音视频   │  │ 检测    │  │              ││
│  │  │  └──────────┘  └──────────┘  └─────────┘  │              ││
│  │  └─────────────────────────────────────────────┘              ││
│  │                                                               ││
│  │  ┌────────────┐ ┌────────────┐ ┌──────────────────┐          ││
│  │  │ RoomManager │ │   Stats   │ │  CallState (状态)│          ││
│  │  └────────────┘ └────────────┘ └──────────────────┘          ││
│  └───────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 关键配置项

SIP Bridge 使用 YAML 配置文件，定义在 [config.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/config/config.go)：

```yaml
# 必需配置
api_key: <livekit_api_key>        # env: LIVEKIT_API_KEY
api_secret: <livekit_api_secret>  # env: LIVEKIT_API_SECRET
ws_url: <livekit_ws_url>          # env: LIVEKIT_WS_URL
redis:
  address: <redis_addr>           # 必须与 LiveKit Server 使用同一 Redis

# SIP 端口配置
sip_port: 5060                    # SIP 监听端口（默认 5060）
sip_port_listen: 5060             # 实际监听端口（可与宣布端口不同）
rtp_port:
  start: 10000                    # RTP 端口范围起始
  end: 20000                      # RTP 端口范围结束
tls:
  port: 5061                      # TLS SIP 端口（默认 5061）
  certs:                          # TLS 证书列表
    - cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem

# 其他配置
log_level: debug
health_port: 8080                 # 健康检查端口
prometheus_port: 9090             # Prometheus 指标端口
sip_hostname: sip.example.com     # SIP 主机名（TLS 必需）
max_active_calls: 100             # 最大并发通话数
hide_inbound_port: false          # 隐藏入站端口（防扫描）
```

---

## 3. 运行入口与服务生命周期

### 3.1 启动流程

入口：[main.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/cmd/livekit-sip/main.go)

```
main()
  │
  ├─ 1. 加载配置（文件或环境变量）
  │     config.NewConfig(configBody)
  │     conf.Init() → 设置默认值
  │
  ├─ 2. 初始化 Redis 客户端
  │     redis.GetRedisClient(conf.Redis)
  │
  ├─ 3. 创建 psrpc 消息总线和客户端
  │     bus = psrpc.NewRedisMessageBus(rc)
  │     psrpcClient = rpc.NewIOInfoClient(bus)
  │     └── 用于与 LiveKit Server 的 IOInfoService 通信
  │
  ├─ 4. 初始化监控
  │     mon = stats.NewMonitor(conf)
  │
  ├─ 5. 创建 SIP 服务核心
  │     sipsrv = sip.NewService(region, conf, mon, log, getIOClient)
  │     └── 内部创建 Server 和 Client
  │
  ├─ 6. 创建服务层（psrpc 服务注册）
  │     svc = service.NewService(...)
  │     sipsrv.SetHandler(svc)
  │     └── Handler 接口：GetAuthCredentials / DispatchCall / 等
  │
  ├─ 7. 启动 SIP 服务
  │     sipsrv.Start()
  │     ├── 启动 UA（User Agent）
  │     ├── 启动 Client（呼出处理）
  │     ├── 启动 Server（呼入处理）
  │     │   ├── UDP 监听 (sipPortListen:5060)
  │     │   ├── TCP 监听 (sipPortListen:5060)
  │     │   └── TLS 监听 (tls.port:5061, 可选)
  │     └── 注册 SIP 事件回调
  │         ├── onInvite → 处理 INVITE
  │         ├── onBye   → 处理 BYE
  │         ├── onAck   → 处理 ACK
  │         └── onNotify → 处理 NOTIFY
  │
  └─ 8. 运行主循环
        svc.Run()
        ├── 启动 Prometheus/pprof/Health HTTP 服务
        ├── 注册 psrpc SIP 内部服务
        │   └── CreateSIPParticipant RPC
        ├── 等待关闭信号
        └── 优雅关闭（等待活动通话结束）
```

### 3.2 关闭流程

服务支持两种关闭模式：

- **SIGTERM/SIGQUIT**：优雅关闭，等待所有通话自然结束
- **SIGINT/Ctrl+C**：强制关闭，立即终止所有通话

```go
select {
case sig := <-stopChan:
    svc.Stop(false) // 优雅关闭
case sig := <-killChan:
    svc.Stop(true) // 强制关闭
}
```

---

## 4. 核心模块

### 4.1 Service 层 ([pkg/service](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/service/service.go))

负责 psrpc 通信和顶层服务管理。

**关键职责**：
- 注册 `SIPInternalServer` psrpc 服务
- 实现 `Handler` 接口（鉴权、路由、媒体处理）
- 管理 Prometheus/pprof/Health HTTP 服务
- 实现优雅关闭逻辑

**Handler 接口实现**：

| 方法 | 功能 | 备注 |
|------|------|------|
| `GetAuthCredentials` | 通过 psrpc 调用 Server 的 GetSIPTrunkAuthentication | 鉴权 |
| `DispatchCall` | 通过 psrpc 调用 Server 的 EvaluateSIPDispatchRules | 路由 |
| `GetMediaProcessor` | 获取媒体处理器 | 当前返回 nil |
| `OnInboundInfo` | 呼入信息回调 | 空实现 |
| `OnSessionEnd` | 通话结束回调 | 记录日志 |

#### psrpc 通信 ([psrpc.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/service/psrpc.go))

```
SIP Bridge → GetAuthCredentials → LiveKit Server.GetSIPTrunkAuthentication()
SIP Bridge → DispatchCall       → LiveKit Server.EvaluateSIPDispatchRules()

LiveKit Server → CreateSIPParticipant → SIP Bridge (通过 SIPInternalServer)
LiveKit Server → TransferSIPParticipant → SIP Bridge (通过 SIPInternalServer)
```

### 4.2 SIP 核心层 ([pkg/sip](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip))

#### 4.2.1 Service ([service.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/service.go))

SIP 服务的顶层协调器，管理 Server、Client 和配置。

**关键字段**：

| 字段 | 说明 |
|------|------|
| `srv *Server` | 呼入处理服务 |
| `cli *Client` | 呼出处理客户端 |
| `sconf *ServiceConfig` | 服务配置（含 IP 地址解析） |
| `pendingTransfers` | 等待中的呼叫转移请求 |

**核心方法**：

| 方法 | 说明 |
|------|------|
| `Start()` | 初始化 UserAgent，启动 Client 和 Server |
| `Stop()` | 停止所有服务 |
| `ActiveCalls()` | 获取当前活动通话统计 |
| `CreateSIPParticipant()` | 处理外呼请求 |
| `TransferSIPParticipant()` | 处理呼叫转移请求 |
| `processParticipantTransfer()` | 执行实际转移（查找 inbound/outbound call） |

**IP 地址解析** (`GetServiceConfig`)：
- 根据 `use_external_ip`、`nat_1_to_1_ip`、`local_net` 等配置
- 自动选择最佳的外部 IP 和本地 IP
- 分别用于信令（SignalingIP）和媒体（MediaIP）

#### 4.2.2 Server ([server.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/server.go))

负责处理所有呼入的 SIP 请求。

**事件处理注册**：

```go
s.sipSrv.OnOptions(s.onOptions)  // OPTIONS 请求（保持连接）
s.sipSrv.OnInvite(s.onInvite)    // INVITE 请求（核心）
s.sipSrv.OnAck(s.onAck)          // ACK 确认
s.sipSrv.OnBye(s.onBye)          // BYE 挂断
s.sipSrv.OnNotify(s.onNotify)    // NOTIFY 通知
s.sipSrv.OnNoRoute(s.OnNoRoute)  // 无路由响应
```

**关键内部状态**：

| 状态 | 说明 |
|------|------|
| `byLocalTag map[LocalTag]*inboundCall` | 活跃的呼入通话 |
| `provisionalInvites *LRU` | 临时的 INVITE（等待鉴权） |
| `inProgressInvites []*inProgressInvite` | 正在鉴权中的请求 |
| `infos.byLocalTag *LRU` | 通话鉴权计数器 |

#### 4.2.3 Client ([client.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/client.go))

负责处理所有呼出的 SIP 请求。

**关键内部状态**：

| 字段 | 说明 |
|------|------|
| `activeCalls map[LocalTag]*outboundCall` | 活跃的呼出通话 |
| `sipCli SIPClient` | sipgo 客户端实例 |

**核心方法**：

| 方法 | 说明 |
|------|------|
| `CreateSIPParticipant()` | 发起外呼 |
| `SetHandler()` | 设置 Handler |
| `Start()` | 启动客户端 |
| `OnRequest()` | 处理未由 Server 处理的请求（如 BYE） |

#### 4.2.4 呼入通话 ([inbound.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/inbound.go))

处理呼入通话的完整生命周期。

**inboundCall 结构体关键字段**：

| 字段 | 说明 |
|------|------|
| `cc *sipInbound` | SIP 协议连接 |
| `media *MediaPort` | 媒体端口（RTP/SRTP） |
| `lkRoom RoomInterface` | LiveKit 房间连接 |
| `state *CallState` | 通话状态（通过 psrpc 同步到 Server） |
| `mon *stats.CallMonitor` | 通话级监控 |
| `stats Stats` | 通话统计 |

**关键方法**：

| 方法 | 说明 |
|------|------|
| `handleInvite()` | 处理鉴权通过后的 INVITE（核心逻辑） |
| `processInvite()` | 完整 INVITE 处理入口 |
| `joinLiveKitRoom()` | 加入 LiveKit 房间 |
| `Bye()` | 处理 BYE 挂断 |
| `handleDTMF()` | DTMF 按键处理 |
| `transferCall()` | 呼叫转移 |
| `Shutdown()` | 关闭通话 |

**SIP 鉴权流程**（`handleInviteAuth`）：
1. 如果 Trunk 没有配置认证凭据 → 直接通过
2. 如果请求中没有认证头 → 发送 407 挑战
3. 解析认证头 → 摘要算法验证
4. 验证失败 → 401 Unauthorized
5. 验证成功 → 继续处理

#### 4.2.5 呼出通话 ([outbound.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/outbound.go))

处理呼出通话的完整生命周期。

**outboundCall 结构体关键字段**：

| 字段 | 说明 |
|------|------|
| `cc *sipOutbound` | SIP 协议连接 |
| `media *MediaPort` | 媒体端口 |
| `lkRoom RoomInterface` | LiveKit 房间连接 |
| `lkRoomIn msdk.PCM16Writer` | 房间音频输出 |
| `sipConf sipOutboundConfig` | SIP 呼出配置 |

**核心流程**：

| 方法 | 说明 |
|------|------|
| `NewCall()` | 创建呼出通话，初始化媒体和房间连接 |
| `Dial()` | 发起 SIP 呼叫 |
| `DialAsync()` | 异步发起呼叫 |
| `connectSIP()` | SIP 信令连接 |
| `connectToRoom()` | 连接 LiveKit 房间 |
| `dialSIP()` | 执行 SIP 拨号（可选播放拨号音） |
| `connectMedia()` | 连接媒体流 |

### 4.3 媒体处理 ([media_port.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/media_port.go) / [media.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/media.go))

负责 RTP/SRTP 媒体流的收发和处理。

**MediaPort 结构体关键职责**：

| 功能 | 说明 |
|------|------|
| RTP 收发 | 通过 UDP 端口接收/发送 RTP 包 |
| SRTP 加密 | 支持 SRTP 加密/解密 |
| DTMF 检测 | 检测 RFC2833 DTMF 事件 |
| SDP 交换 | SDP Offer/Answer 编解码协商 |
| 音频桥接 | 将 RTP 音频 → PCM16 → 房间 / 房间 → PCM16 → RTP |
| 超时检测 | RTP 超时检测（初始 30s，后续 15s） |

**MediaOptions 配置**：

| 选项 | 默认值 | 说明 |
|------|--------|------|
| IP | 自动检测 | 媒体 IP |
| Ports | 10000-20000 | RTP 端口范围 |
| MediaTimeoutInitial | 30s | 初始媒体超时 |
| MediaTimeout | 15s | 后续媒体超时 |
| SymmetricRTP | false | 对称 RTP |
| EnableJitterBuffer | false | 启用抖动缓冲 |

### 4.4 Room 连接管理 ([room.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/room.go))

管理 SIP Bridge 与 LiveKit 房间的连接。

**RoomInterface 接口**：

| 方法 | 说明 |
|------|------|
| `Connect()` | 连接到 LiveKit 房间 |
| `Room()` | 获取房间对象 |
| `CloseOutput()` | 关闭输出流 |
| `CloseWithReason()` | 关闭连接并指定原因 |
| `Closed()` | 获取关闭信号通道 |
| `ClosedReason()` | 获取关闭原因 |
| `Participant()` | 获取参与者信息 |
| `NewParticipantTrack()` | 创建音频发布轨道 |
| `SetDTMFOutput()` | 设置 DTMF 输出 |
| `Subscribe()` | 订阅房间音频轨道 |
| `SwapOutput()` | 交换音频输出 |

**音频流处理**：
- 默认采样率：48000 Hz（RoomSampleRate）
- 单声道（channels = 1）
- 支持抖动缓冲（JitterBuffer）
- Opus 编解码：OPUS 48k ↔ PCM16 48k

### 4.5 音频编解码 ([media/opus](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/media/opus/opus.go))

基于 libopus 的音频编解码实现。

特例：如果 SIP 端使用 8000 Hz 音频，需要通过 Opus 重采样器转换到 48000 Hz。

### 4.6 CallState 管理

通话状态通过 psrpc 同步到 LiveKit Server。

**SIPCallStatus 状态枚举**：

| 状态 | 说明 |
|------|------|
| SCS_CALL_INCOMING | 呼入中 |
| SCS_ACTIVE | 通话中 |
| SCS_DISCONNECTED | 已断开 |
| SCS_ERROR | 错误 |

**SIPCallInfo 记录字段**：

| 字段 | 说明 |
|------|------|
| CallId | 通话 ID |
| CallDirection | 方向（入站/出站） |
| CallStatus | 当前状态 |
| FromUri / ToUri | SIP 主/被叫 URI |
| TrunkId | 匹配的 Trunk |
| RoomId | LiveKit 房间 ID |
| SipCallId | SIP Call-ID |
| ParticipantIdentity / Attributes | 参与者信息 |
| CreatedAtNs / StartedAtNs / EndedAtNs | 时间戳 |
| Error | 错误信息 |
| DisconnectReason | 断开原因 |

---

## 5. SIP 呼入流程

完整呼入流程（从运营商 INVITE 到 Agent 对话）：

### 5.1 流程总览

```
PSTN/SIP 运营商                                  SIP Bridge                   LiveKit Server
      │                                              │                              │
      │  ──── SIP INVITE (To:+1234) ───────────────→│                              │
      │                                              │                              │
      │                                              │─ ① GetAuthCredentials() ───→│
      │                                              │←── AuthInfo (Trunk/密码) ──│
      │                                              │                              │
      │  ←── 407 Proxy Authentication ──────────────│                              │
      │                                              │                              │
      │  ──── SIP INVITE (with Auth) ───────────────→│                              │
      │                                              │                              │
      │                                              │─ ② handleInviteAuth() ──── │
      │                                              │   (摘要认证验证通过)          │
      │                                              │                              │
      │                                              │─ ③ DispatchCall() ─────────→│
      │                                              │←── CallDispatch (ACCEPT) ──│
      │                                              │   (返回 RoomName/Token)     │
      │                                              │                              │
      │                                              │─ ④ 加入 LiveKit Room        │
      │                                              │   (作为 SIP Participant)     │
      │                                              │                              │
      │  ←── 200 OK (with SDP) ─────────────────────│                              │
      │                                              │                              │
      │  ──── ACK ──────────────────────────────────→│                              │
      │                                              │                              │
      │                                              │─ ⑤ 开始媒体桥接              │
      │══════════ RTP Audio (双向) ════════════│                              │
      │                                              │                              │
      │                                              │←── Agent 加入房间 ──────────│
      │══════════ 双向对话 ═══════════════════│                              │
```

### 5.2 详细步骤

#### 步骤 1：接收 INVITE

```go
func (s *Server) processInvite(req *sip.Request, tx sip.ServerTransaction) (retErr error) {
    // 1. 解析源 IP
    src, err := netip.ParseAddrPort(req.Source())
    
    // 2. 创建 SIP 呼入连接
    cc, err := s.newInbound(req, tx, src)
    
    // 3. 构建 CallInfo
    callInfo := &rpc.SIPCall{
        LkCallId:  string(cc.ID()),
        SipCallId: cc.SIPCallID(),
        SourceIp:  src.Addr().String(),
        From:      ToSIPUri("", from),
        To:        ToSIPUri("", to),
    }
    
    // 4. 调用 Handler 获取鉴权
    r, err := s.handler.GetAuthCredentials(ctx, callInfo)
}
```

#### 步骤 2：鉴权处理

根据 Trunk 配置返回不同的鉴权结果：

| AuthResult | 处理方式 |
|-----------|---------|
| `AuthAccept` | 无需鉴权，直接通过 |
| `AuthPassword` | 发送 407 挑战，等待带认证的 INVITE |
| `AuthDrop` | 静默丢弃（HideInboundPort 模式） |
| `AuthNotFound` | 返回 404 Not Found |
| `AuthQuotaExceeded` | 返回 503 Service Unavailable |

#### 步骤 3：路由分发

鉴权通过后，调用 DispatchCall 查询路由规则：

```go
dispatch := s.handler.DispatchCall(ctx, &CallInfo{
    TrunkID: trunkID,
    Call:    callInfo,
})
```

| DispatchResult | 处理方式 |
|---------------|---------|
| `DispatchAccept` | 接受通话，加入房间 |
| `DispatchRequestPin` | 要求输入 PIN 码 |
| `DispatchNoRuleReject` | 拒绝通话 |
| `DispatchNoRuleDrop` | 静默丢弃 |

#### 步骤 4：加入 LiveKit 房间

```go
func (c *inboundCall) joinLiveKitRoom(ctx context.Context, roomConf RoomConfig) error {
    // 使用返回的 Token 连接 LiveKit 房间
    r := getRoom(...)
    r.Connect(ctx, conf, roomConf)
    
    // 创建音频轨道
    track, err := r.NewParticipantTrack(RoomSampleRate)
    c.lkRoom = r
}
```

#### 步骤 5：SDP 协商

加入房间后，完成 SIP 200 OK 响应（含 SDP），建立 RTP 媒体连接。

```go
// inboundCall.handleInvite() 中：
// 发送 200 OK（含 SDP）
sdpAnswer := c.media.negotiateSDP(sdpOffer)
cc.RespondOK(sdpAnswer)
// 等待 ACK
cc.WaitAck()
```

#### 步骤 6：媒体桥接

```go
// RTP → LiveKit Room
media.GetAudioWriter() → mixer → OPUS编码 → Room.Track

// LiveKit Room → RTP
Room.Track → OPUS解码 → media.WriteAudioTo(roomIn)
```

---

## 6. SIP 呼出流程

### 6.1 流程总览

```
LiveKit Server                              SIP Bridge                 PSTN/SIP 运营商
      │                                          │                          │
      │  psrpc CreateSIPParticipant              │                          │
      │─────────────────────────────────────────→│                          │
      │                                          │                          │
      │                                          │─ ① 解析配置              │
      │                                          │─ ② 连接 LiveKit 房间     │
      │                                          │   (发布音频轨道)          │
      │                                          │                          │
      │                                          │ 可选: 播放拨号音         │
      │                                          │                          │
      │                                          │─ ③ SIP INVITE ─────────→│
      │                                          │                          │
      │                                          │←── 100 Trying ──────────│
      │                                          │←── 180 Ringing ─────────│
      │                                          │←── 200 OK (with SDP) ──│
      │                                          │                          │
      │                                          │─ ④ ACK ────────────────→│
      │                                          │                          │
      │                                          │─ ⑤ 连接媒体流            │
      │══════════ RTP Audio (双向) ══════════│
```

### 6.2 详细步骤

#### 步骤 1：创建呼叫请求

```go
func (s *Service) CreateSIPParticipant(ctx context.Context, req *rpc.InternalCreateSIPParticipantRequest) {
    // 1. 解析呼出配置（号码、传输、认证等）
    sipConf := parseOutboundConfig(req)
    
    // 2. 查询 Room 配置
    room := RoomConfig{...}
    
    // 3. 创建通话对象
    call, err := s.cli.newCall(ctx, tid, s.conf, log, id, room, sipConf, state, projectID)
    
    // 4. 异步拨号
    call.DialAsync(ctx)
}
```

#### 步骤 2：连接 LiveKit 房间

```go
func (c *outboundCall) connectToRoom(ctx context.Context, roomConf RoomConfig) {
    // 1. 设置参与者属性（含 SIP CallID）
    attrs[livekit.AttrSIPCallID] = sipCallID
    
    // 2. 连接房间
    r.Connect(ctx, c.conf, roomConf)
    
    // 3. 创建音频轨道（先创建，用于播放拨号音）
    local, err := r.NewParticipantTrack(RoomSampleRate)
    c.lkRoom = r
    c.lkRoomIn = local
}
```

#### 步骤 3：SIP 信令

```go
func (c *outboundCall) sipSignal(ctx context.Context, tid traceid.ID) error {
    // 1. 创建 SDP Offer
    sdpOffer, err := c.media.NewOffer(mconf.Codecs, mconf.Encryption)
    
    // 2. 发送 INVITE
    sdpResp, err := c.cc.Invite(ctx, toUri, user, pass, headers, sdpOfferData, callback)
    
    // 3. 处理 SDP Answer
    mc, localSDP, err := c.media.SetAnswer(sdpOffer, sdpResp, mconf.Codecs, mconf.Encryption)
    
    // 4. 配置媒体
    c.media.SetConfig(mc)
    
    // 5. 收到 ACK（由 INVITE 响应隐含）
}
```

#### 步骤 4：连接媒体流

```go
func (c *outboundCall) connectMedia() {
    // 交换音频输出（拨号音 → 真正的媒体流）
    c.lkRoom.SwapOutput(c.media.GetAudioWriter())
    
    // 设置 DTMF 输出
    c.lkRoom.SetDTMFOutput(c.media)
    
    // 连接音频流
    c.media.WriteAudioTo(c.lkRoomIn)
    c.media.HandleDTMF(c.handleDTMF)
    
    // 订阅房间音频
    c.lkRoom.Subscribe()
}
```

---

## 7. 媒体处理

### 7.1 整体架构

```
SIP 端 (PSTN)              SIP Bridge                  LiveKit 房间
     │                          │                           │
     │    RTP (OPUS/PCMU/...)   │                           │
     ├─────────────────────────→│                           │
     │                          │    OPUS 解码              │
     │                          │    ┌──────────┐          │
     │                          │    │  Opus →  │          │
     │                          │    │  PCM16   │          │
     │                          │    └────┬─────┘          │
     │                          │         │                 │
     │                          │    ┌────▼─────┐          │
     │                          │    │  Mixer   │          │
     │                          │    └────┬─────┘          │
     │                          │         │ PCM16 48k       │
     │                          │    ┌────▼─────┐          │
     │                          │    │  Room    │───────→  │
     │                          │    │  Track   │          │
     │                          │    └──────────┘          │
     │                          │                           │
     │    RTP (OPUS/PCMU/...)   │                           │
     │←─────────────────────────│                           │
     │                          │    OPUS 编码              │
     │                          │    ┌──────────┐          │
     │                          │    │  Opus →  │          │
     │                          │    │  PCM16   │          │
     │                          │    └────▲─────┘          │
     │                          │         │                 │
     │                          │    ┌────┴─────┐          │
     │                          │    │  Room ←  │←──────── │
     │                          │    │  Track   │          │
     │                          │    └──────────┘          │
```

### 7.2 RTP 端口管理 ([media_port.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/media_port.go))

MediaPort 管理 RTP 端口的完整生命周期：

| 方法 | 说明 |
|------|------|
| `NewMediaPort()` | 创建媒体端口，绑定 UDP 端口 |
| `NewOffer()` | 创建 SDP Offer |
| `SetAnswer()` | 处理 SDP Answer |
| `SetConfig()` | 配置媒体参数 |
| `GetAudioWriter()` | 获取音频写入器（RTP 输出到 SIP） |
| `WriteAudioTo()` | 设置音频输入源（从 Room 到 SIP） |
| `HandleDTMF()` | 设置 DTMF 回调 |
| `WriteDTMF()` | 发送 DTMF 按键 |
| `EnableTimeout()` | 启用/禁用超时检测 |
| `Timeout()` | 获取超时信号通道 |
| `Close()` | 关闭媒体端口 |

### 7.3 SDP 交换与编解码协商

SIP Bridge 在 `MediaPort.NewOffer()` 中生成 SDP Offer，支持的编解码包括：

- **OPUS/48000/2** (动态 PT)
- **PCMU/8000** (PT 0)
- **PCMA/8000** (PT 8)
- **telephone-event/8000** (DTMF, PT 101)

SDP 特性：
- 支持 DIRECTION（sendrecv/sendonly/recvonly）
- 支持加密（SRTP）
- 通过 `sdp` 包处理媒体线路

### 7.4 DTMF 处理

两种 DTMF 模式：

1. **RFC 2833 (Telephone Event)**：通过 RTP 包中的 telephone-event 载荷传输
   - 由 `media-sdk/dtmf` 包解析
   - 支持检测（从 SIP 到 Room）和生成（从 Room 到 SIP）

2. **音频 DTMF (In-band)**：通过音频信号传输
   - 由配置 `AudioDTMF` 控制
   - 将 DTMF 按键转为音频音调

### 7.5 抖动缓冲

`MediaPort` 支持抖动缓冲（JitterBuffer），配置项：
- `EnableJitterBuffer`：全局开关
- `EnableJitterBufferProb`：基于概率启用

### 7.6 音频静音填充

[`silence_filler.go`](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/silence_filler.go) - 在 RTP 流间隙填充静音包，防止音频中断。

### 7.7 音频延迟桥接

[`media_latency.go`](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/media_latency.go)

`audioBridgeMaxDelay = 1s`：在首次收到 RTP 包之前，延迟发送音频最多 1 秒，防止开头音频被截断（在真实环境中观察到的问题）。

---

## 8. 音频编解码

### 8.1 采样率转换

| 输入 | 处理 | 输出 |
|------|------|------|
| RTP OPUS 48k | Opus 解码 → PCM16 48k | LiveKit Room |
| LiveKit Room | PCM16 48k | Opus 编码 → RTP OPUS 48k |
| RTP PCMU 8k | 重采样 → PCM16 48k | LiveKit Room |

### 8.2 音频混音器 (Mixer)

Room 模块中的 Mixer 负责将多个音频轨道的 PCM16 数据混合：
- 输入：来自房间内多个参与者的 OPUS 音频
- 处理：OPUS 解码 → PCM16 → 混音
- 输出：PCM16 → RTP 发送到 SIP 端

### 8.3 端到端加密 (SRTP)

[`srtp_conn.go`](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/srtp_conn.go)

支持通过 SDES（Session Description Protocol Security Descriptions）进行 SRTP 加密协商。

---

## 9. Room 连接管理

### 9.1 连接参数

SIP Bridge 作为 `PARTICIPANT_KIND_SIP` 类型的参与者加入房间，携带以下属性：

| 属性键 | 说明 |
|--------|------|
| `livekit.AttrSIPCallID` | SIP Call-ID |
| `livekit.AttrSIPCallStatus` | 通话状态（dialing/ringing/active） |
| 提供商特定属性 | Twilio/Telnyx/Amazon 等 |

### 9.2 参与者标识

SIP 参与者通过以下方式标识：

- `participant_identity`：调用方指定
- `participant_name`：可选显示名称
- `participant_metadata`：可选元数据
- `attributes`：包含 SIP 相关属性

### 9.3 属性映射

```
SIP Headers → 参与者属性: HeadersToAttrs
参与者属性 → SIP Headers: AttrsToHeaders
```

支持以下提供商自动映射：

| 提供商 | SIP Header | 参与者属性 |
|--------|-----------|-----------|
| Twilio | X-Twilio-AccountSid | lk.sip.twilio.accountSid |
| Twilio | X-Twilio-CallSid | lk.sip.twilio.callSid |
| Telnyx | X-call_leg_id | lk.sip.telnyx.callLegID |
| Telnyx | X-call_session_id | lk.sip.telnyx.callSessionID |
| Amazon Connect | X-Amzn-ConnectContactId | lk.sip.amazon.contactId |
| Amazon Connect | X-Amzn-ConnectInitialContactId | lk.sip.amazon.initialContactId |

### 9.4 关闭原因映射

Room 关闭原因与 SIP 断开原因的映射（[participant.go](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/participant.go)）：

| LiveKit DisconnectReason | Termination 分类 |
|-------------------------|-----------------|
| CLIENT_INITIATED / ROOM_CLOSED / ROOM_DELETED | Success("removed") |
| SERVER_SHUTDOWN | ServerError("server-shutdown") |
| CONNECTION_TIMEOUT | ServerError("connection-timeout") |
| SIP_TRUNK_FAILURE | ServerError("sip-trunk-failure") |
| MEDIA_FAILURE | ServerError("media-failure") |
| USER_UNAVAILABLE | ClientError("user-unavailable") |
| USER_REJECTED | ClientError("user-rejected") |

---

## 10. 统计与监控

### 10.1 数据采集点

[`stats/monitor.go`](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/stats/monitor.go)

| 指标 | 类型 | 说明 |
|------|------|------|
| InviteReq | Counter | INVITE 请求数 |
| InviteError | Counter | INVITE 错误数 |
| CallStart | Counter | 通话开始数 |
| CallEnd | Counter | 通话结束数 |
| SessionDur | Histogram | 通话时长 |
| JoinDur | Histogram | 加入房间耗时 |
| RTPPacketRecv | Counter | RTP 包接收数 |
| TransferStarted | Counter | 转移开始数 |
| TransferSucceeded | Counter | 转移成功数 |
| TransferFailed | Counter | 转移失败数 |

### 10.2 健康状态

| 状态 | 说明 |
|------|------|
| HealthOK | 正常运行 |
| HealthUnderLoad | 负载过高 |
| HealthUnavailable | 不可用 |

### 10.3 调用统计

每个通话结束时输出统计日志：

```json
{
    "sip_rx_ppm": 0.0,       // SIP 接收速率偏差 (ppm)
    "sip_tx_ppm": 0.0,       // SIP 发送速率偏差 (ppm)
    "lk_publish_ppm": 0.0,   // LiveKit 发布速率偏差 (ppm)
    "durMin": 5,             // 通话时长 (分钟)
    "stats": {
        "port": { "audio_rx": 48000.0, "audio_tx": 48000.0 },
        "room": { "publish_tx": 48000.0 }
    }
}
```

---

## 11. SIP 协议特性

### 11.1 传输协议支持

| 协议 | 支持 | 端口 |
|------|------|------|
| UDP | 是 | 5060（默认） |
| TCP | 是 | 5060（默认） |
| TLS | 是 | 5061（默认） |

### 11.2 摘要认证（Digest Auth）

- 支持 407 Proxy Authentication
- 使用 MD5 算法（标准摘要）
- 每通通话生成唯一 Nonce
- 支持用户名/密码验证
- Inbound 模式支持隐藏端口（`hide_inbound_port`）

### 11.3 呼叫转移（Transfer）

通过 SIP REFER 方法实现：

```go
// 流程:
// 1. 发送 REFER 请求（含 Refer-To 头）
// 2. 接收 202 Accepted 或 200 OK
// 3. 等待 NOTIFY 通知结果
// 4. 收到 BYE（原通话结束）
```

### 11.4 header 到 attribute 映射

SIP Bridge 支持 SIP 请求中的自定义 Header 映射到 LiveKit 参与者属性：

```go
var headerToAttr = map[string]string{
    "X-Twilio-AccountSid":            livekit.AttrSIPPrefix + "twilio.accountSid",
    "X-Twilio-CallSid":               livekit.AttrSIPPrefix + "twilio.callSid",
    "X-call_leg_id":                  livekit.AttrSIPPrefix + "telnyx.callLegID",
    "X-Amzn-ConnectContactId":        livekit.AttrSIPPrefix + "amazon.contactId",
}
```

### 11.5 TLS 配置

[`tls.go`](file:///d:/xuyong/source/AITest/livekit/livekit-sip/pkg/sip/tls.go)

支持：
- 多证书加载
- TLS 版本配置（1.0 - 1.3）
- 密码套件配置
- ALPN 协议配置（默认 "sip"）
- KeyLog 调试

### 11.6 呼叫路由

支持通过 `OutboundRouteHeaders` 配置路由头，预置到所有出站请求中。

---

## 12. 关键设计决策

1. **两层架构** — SIP Bridge 与 LiveKit Server 分离部署，各司其职。Bridge 处理 SIP 协议，Server 处理业务路由

2. **psrpc 通信** — 基于 Redis Pub/Sub 的异步 RPC，支持多节点部署和负载均衡

3. **独立 UserAgent** — Server 和 Client 共享同一个 sipgo UserAgent，避免端口冲突和防火墙配置问题

4. **统一采样率** — LiveKit 房间使用固定 48kHz 采样率，SIP 端音频通过 Opus 编解码或重采样器转换

5. **预创建音频轨道** — 呼出时先创建音频轨道（用于播放拨号音），呼入时在 SDP 交换前创建

6. **音频延迟桥接** — 延迟 1 秒发送音频，等待首个 RTP 包到达，防止开头音频截断

7. **两次 Trunk 匹配** — 鉴权阶段和路由阶段各执行一次，路由阶段可利用 trunkId 加速

8. **HideInboundPort** — 对于未认证的请求静默丢弃，隐藏 SIP 端点防止端口扫描

9. **SIP 鉴权超时** — 30 秒超时，超时后标记为错误状态，防止僵尸通话

10. **属性映射** — 通过 SIP Header ↔ Participant Attribute 的映射实现与第三方提供商（Twilio、Telnyx、Amazon Connect）的深度集成

11. **十亿分之一精度统计** — 音频速率偏差以 ppm（parts per million）量化，用于质量监控

12. **向前兼容** — 支持旧版 Trunk 格式的自动转换和加载，确保平滑迁移