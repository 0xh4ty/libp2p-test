package app

import (
    "bufio"
    "context"
    "crypto/rand"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/0xh4ty/libp2p-test/internal/network"
    dht "github.com/libp2p/go-libp2p-kad-dht"
    "github.com/libp2p/go-libp2p/core/connmgr"
    "github.com/libp2p/go-libp2p/core/control"
    "github.com/libp2p/go-libp2p/core/crypto"
    "github.com/libp2p/go-libp2p/core/host"
    net "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    pingp2p "github.com/libp2p/go-libp2p/p2p/protocol/ping"
    ma "github.com/multiformats/go-multiaddr"
    manet "github.com/multiformats/go-multiaddr/net"
)

const protocolID = "/libp2p-test/1.0.0"

func RunNode(enableRelay bool) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    homeDir, err := os.UserHomeDir()
    if err != nil {
        panic(err)
    }

    nodeDataDir := filepath.Join(homeDir, ".libp2p-test")
    if err := os.MkdirAll(nodeDataDir, 0700); err != nil {
        panic(err)
    }

    privKey, err := loadOrCreateIdentity(filepath.Join(nodeDataDir, "identity.key"))
    if err != nil {
        panic(err)
    }

    bootstrapPath := filepath.Join(nodeDataDir, "bootstrap.config")
    bootstrapNodes, err := readBootstrapConfig(bootstrapPath)
    if err != nil {
        log.Printf("bootstrap config: %v", err)
    }

    var staticRelays []peer.AddrInfo
    if !enableRelay {
        staticRelays = parseBootstrapPeers(bootstrapNodes)
    }

    var gater connmgr.ConnectionGater
    if !enableRelay {
        gater = lanGater{}
    }

    node, err := network.NewHost(privKey, enableRelay, staticRelays, gater)
    if err != nil {
        panic(err)
    }
    defer node.Close()

    fmt.Println("PeerID:", node.ID())
    log.Println("This node's multiaddresses:")
    for _, la := range node.Addrs() {
        log.Printf(" - %s/p2p/%s", la, node.ID())
    }

    node.SetStreamHandler(protocolID, handleStream)

    if enableRelay {
        log.Println("Relay service enabled")
    } else {
        log.Println("Relay service disabled (client: LAN dials blocked)")
    }

    var kad *dht.IpfsDHT
    if len(bootstrapNodes) == 0 {
        kad, err = startPeer(ctx, node, enableRelay)
    } else {
        kad, err = startPeerWithBootstrapNodes(ctx, node, bootstrapNodes, enableRelay)
    }
    if err != nil {
        panic(err)
    }
    defer kad.Close()

    if !enableRelay {
        go maintainRelayedPeers(ctx, node, kad, staticRelays)
        for _, r := range staticRelays {
            go keepAlive(ctx, node, r.ID)
        }
    }

    go watchRoutingTable(ctx, node, kad)
    select {}
}

func loadOrCreateIdentity(path string) (crypto.PrivKey, error) {
    if raw, err := os.ReadFile(path); err == nil && len(raw) > 0 {
        return crypto.UnmarshalPrivateKey(raw)
    }

    priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, -1, rand.Reader)
    if err != nil {
        return nil, err
    }
    raw, err := crypto.MarshalPrivateKey(priv)
    if err != nil {
        return nil, err
    }
    if err := os.WriteFile(path, raw, 0600); err != nil {
        return nil, err
    }
    return priv, nil
}

func readBootstrapConfig(path string) ([]string, error) {
    f, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, fmt.Errorf("%s not found (create it with one multiaddr per line)", path)
        }
        return nil, err
    }
    defer f.Close()

    var out []string
    sc := bufio.NewScanner(f)
    for sc.Scan() {
        line := strings.TrimSpace(sc.Text())
        if line == "" || strings.HasPrefix(line, "#") {
            continue
        }
        out = append(out, line)
    }
    return out, sc.Err()
}

func parseBootstrapPeers(addrs []string) []peer.AddrInfo {
    var out []peer.AddrInfo
    for _, address := range addrs {
        info, err := parseAddrInfo(address)
        if err != nil {
            log.Printf("skip bootstrap %q: %v", address, err)
            continue
        }
        out = append(out, *info)
    }
    return out
}

func parseAddrInfo(address string) (*peer.AddrInfo, error) {
    info, err := peer.AddrInfoFromString(address)
    if err != nil {
        return nil, err
    }
    return info, nil
}

func startPeer(ctx context.Context, node host.Host, enableRelay bool) (*dht.IpfsDHT, error) {
    log.Println("Starting DHT...")
    mode := dht.ModeClient
    if enableRelay {
        mode = dht.ModeServer
    }
    kad, err := dht.New(node, dht.Mode(mode), dht.RoutingTableRefreshQueryTimeout(30*time.Second))
    if err != nil {
        return nil, err
    }
    log.Println("DHT started (no bootstrap peers)")
    return kad, nil
}

func startPeerWithBootstrapNodes(
    ctx context.Context,
    node host.Host,
    bootstrapNodes []string,
    enableRelay bool,
) (*dht.IpfsDHT, error) {
    log.Println("Starting DHT...")
    mode := dht.ModeClient
    if enableRelay {
        mode = dht.ModeServer
    }
    kad, err := dht.New(node, dht.Mode(mode), dht.RoutingTableRefreshQueryTimeout(30*time.Second))
    if err != nil {
        return nil, err
    }
    log.Println("DHT started")

    var lastErr error
    connectedAny := false
    for _, address := range bootstrapNodes {
        log.Printf("Connecting to bootstrap node: %s", address)
        info, err := parseAddrInfo(address)
        if err != nil {
            log.Printf("Invalid bootstrap address %q: %v", address, err)
            lastErr = err
            continue
        }
        connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
        err = node.Connect(connectCtx, *info)
        cancel()
        if err != nil {
            log.Printf("Failed to connect to bootstrap node %s: %v", address, err)
            lastErr = err
            continue
        }
        log.Printf("Connected to bootstrap peer: %s", info.ID)
        _, _ = kad.RoutingTable().TryAddPeer(info.ID, true, false)
        connectedAny = true
    }

    if !connectedAny {
        if lastErr == nil {
            lastErr = fmt.Errorf("no valid bootstrap nodes")
        }
        _ = kad.Close()
        return nil, lastErr
    }

    if err := kad.Bootstrap(ctx); err != nil {
        _ = kad.Close()
        return nil, err
    }
    log.Println("DHT bootstrap completed")
    return kad, nil
}

func keepAlive(ctx context.Context, node host.Host, target peer.ID) {
    _ = pingp2p.NewPingService(node)

    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if len(node.Network().ConnsToPeer(target)) == 0 {
                continue
            }
            pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
            res := <-pingp2p.Ping(pctx, node, target)
            cancel()
            if res.Error != nil {
                log.Printf("keepalive ping to %s failed: %v", target, res.Error)
            } else {
                log.Printf("keepalive ping to %s rtt=%s", target, res.RTT)
            }
        }
    }
}

func maintainRelayedPeers(
    ctx context.Context,
    node host.Host,
    kad *dht.IpfsDHT,
    relays []peer.AddrInfo,
) {
    if len(relays) == 0 {
        return
    }

    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            seen := make(map[peer.ID]struct{})
            for _, p := range node.Peerstore().PeersWithAddrs() {
                if p == node.ID() || isRelay(p, relays) {
                    continue
                }
                seen[p] = struct{}{}
                ensureCircuitConn(ctx, node, kad, relays, p)
            }
            for _, p := range kad.RoutingTable().ListPeers() {
                if p == node.ID() || isRelay(p, relays) {
                    continue
                }
                if _, ok := seen[p]; ok {
                    continue
                }
                ensureCircuitConn(ctx, node, kad, relays, p)
            }
        }
    }
}

func isRelay(id peer.ID, relays []peer.AddrInfo) bool {
    for _, r := range relays {
        if r.ID == id {
            return true
        }
    }
    return false
}

func ensureCircuitConn(
    ctx context.Context,
    node host.Host,
    kad *dht.IpfsDHT,
    relays []peer.AddrInfo,
    target peer.ID,
) {
    if len(node.Network().ConnsToPeer(target)) == 0 {
        var addrs []ma.Multiaddr
        for _, r := range relays {
            for _, ra := range usableRelayAddrs(r) {
                circ, err := circuitAddr(ra, r.ID, target)
                if err != nil {
                    continue
                }
                addrs = append(addrs, circ)
            }
        }
        if len(addrs) == 0 {
            return
        }
        cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
        err := node.Connect(cctx, peer.AddrInfo{ID: target, Addrs: addrs})
        cancel()
        if err != nil {
            log.Printf("circuit connect to %s failed: %v", target, err)
            return
        }
        log.Printf("circuit connected to %s", target)
    }

    conns := node.Network().ConnsToPeer(target)
    if len(conns) == 0 {
        return
    }

    var circ net.Conn
    for _, c := range conns {
        if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err == nil {
            circ = c
            break
        }
    }
    if circ == nil {
        circ = conns[0]
    }

    _, _ = kad.RoutingTable().TryAddPeer(target, true, false)

    sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    s, err := circ.NewStream(sctx)
    if err != nil {
        log.Printf("raw stream to %s failed: %v", target, err)
        return
    }
    if err := s.SetProtocol(protocolID); err != nil {
        _ = s.Reset()
        log.Printf("set protocol failed: %v", err)
        return
    }
    log.Printf("stream ok to %s via %v", target, circ.RemoteMultiaddr())
    _ = s.Close()
}

func usableRelayAddrs(r peer.AddrInfo) []ma.Multiaddr {
    var out []ma.Multiaddr
    for _, a := range r.Addrs {
        if manet.IsPrivateAddr(a) || manet.IsIPLoopback(a) {
            continue
        }
        if _, err := a.ValueForProtocol(ma.P_CIRCUIT); err == nil {
            continue
        }
        out = append(out, a)
    }
    return out
}

func circuitAddr(relayAddr ma.Multiaddr, relayID, target peer.ID) (ma.Multiaddr, error) {
    base := relayAddr
    if _, err := base.ValueForProtocol(ma.P_P2P); err != nil {
        p, err := ma.NewMultiaddr("/p2p/" + relayID.String())
        if err != nil {
            return nil, err
        }
        base = base.Encapsulate(p)
    }
    circ, err := ma.NewMultiaddr("/p2p-circuit/p2p/" + target.String())
    if err != nil {
        return nil, err
    }
    return base.Encapsulate(circ), nil
}

func handleStream(s net.Stream) {
    defer s.Close()
    log.Printf("got %s from %s", protocolID, s.Conn().RemotePeer())
}

func watchRoutingTable(ctx context.Context, node host.Host, kad *dht.IpfsDHT) {
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            tablePeers := kad.RoutingTable().ListPeers()
            log.Printf("Routing table size: %d", len(tablePeers))
            for _, p := range tablePeers {
                log.Printf("  - %s", p)
            }

            allKnown := node.Peerstore().PeersWithAddrs()
            log.Printf("Peerstore knows %d peer(s) with addresses", len(allKnown))
            for _, p := range allKnown {
                if p == node.ID() {
                    continue
                }
                log.Printf("  peer %s addrs: %v", p, node.Peerstore().Addrs(p))
                conns := node.Network().ConnsToPeer(p)
                if len(conns) == 0 {
                    log.Printf("    no active connection")
                    continue
                }
                for _, c := range conns {
                    log.Printf("    active conn via: %v", c.RemoteMultiaddr())
                }
            }

            refreshErrCh := kad.RefreshRoutingTable()
            go func() {
                if err := <-refreshErrCh; err != nil {
                    log.Printf("Routing table refresh error: %v", err)
                }
            }()
        }
    }
}

type lanGater struct{}

func (lanGater) InterceptPeerDial(peer.ID) bool { return true }

func (lanGater) InterceptAddrDial(_ peer.ID, addr ma.Multiaddr) bool {
    if _, err := addr.ValueForProtocol(ma.P_CIRCUIT); err == nil {
        return true
    }
    if manet.IsIPLoopback(addr) || manet.IsPrivateAddr(addr) {
        return false
    }
    return true
}

func (lanGater) InterceptAccept(net.ConnMultiaddrs) bool { return true }

func (lanGater) InterceptSecured(net.Direction, peer.ID, net.ConnMultiaddrs) bool {
    return true
}

func (lanGater) InterceptUpgraded(net.Conn) (bool, control.DisconnectReason) {
    return true, 0
}
