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
    "sync"
    "time"

    "github.com/0xh4ty/libp2p-test/internal/network"
    "github.com/ipfs/go-cid"
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
    mh "github.com/multiformats/go-multihash"
)

const protocolID = "/libp2p-test/1.0.0"

func rendezvousCID() cid.Cid {
    h, _ := mh.Sum([]byte("libp2p-test-rendezvous"), mh.SHA2_256, -1)
    return cid.NewCidV1(cid.Raw, h)
}

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
        log.Println("Relay service disabled (client: private + AutoRelay)")
    }

    kad, err := newDHT(node, enableRelay)
    if err != nil {
        panic(err)
    }
    defer kad.Close()

    node.Network().Notify(&net.NotifyBundle{
        ConnectedF: func(_ net.Network, c net.Conn) {
            pid := c.RemotePeer()
            if pid == node.ID() {
                return
            }
            _, _ = kad.RoutingTable().TryAddPeer(pid, true, false)
            log.Printf("connected %s via %s", pid, c.RemoteMultiaddr())
        },
        DisconnectedF: func(_ net.Network, c net.Conn) {
            log.Printf("disconnected %s via %s", c.RemotePeer(), c.RemoteMultiaddr())
        },
    })

    if err := connectBootstrap(ctx, node, kad, bootstrapNodes); err != nil {
        if enableRelay {
            log.Printf("relay has no bootstrap peers (ok): %v", err)
        } else {
            panic(err)
        }
    }

    if !enableRelay {
        seen := make(map[peer.ID]struct{})
        for _, r := range staticRelays {
            if _, ok := seen[r.ID]; ok {
                continue
            }
            seen[r.ID] = struct{}{}
            go keepAlive(ctx, node, r.ID)
        }
    }

    go watchAndDial(ctx, node, kad, staticRelays, enableRelay)
    go announceAndDiscover(ctx, node, kad, staticRelays, enableRelay)

    select {}
}

func newDHT(node host.Host, enableRelay bool) (*dht.IpfsDHT, error) {
    log.Println("Starting DHT...")
    mode := dht.ModeClient
    if enableRelay {
        mode = dht.ModeServer
    }
    kad, err := dht.New(
        node,
        dht.Mode(mode),
        dht.RoutingTableRefreshQueryTimeout(30*time.Second),
    )
    if err != nil {
        return nil, err
    }
    log.Println("DHT started")
    return kad, nil
}

func connectBootstrap(ctx context.Context, node host.Host, kad *dht.IpfsDHT, bootstrapNodes []string) error {
    if len(bootstrapNodes) == 0 {
        return fmt.Errorf("no bootstrap peers")
    }

    var lastErr error
    connectedAny := false
    seen := make(map[peer.ID]struct{})

    for _, address := range bootstrapNodes {
        log.Printf("Connecting to bootstrap node: %s", address)
        info, err := parseAddrInfo(address)
        if err != nil {
            log.Printf("Invalid bootstrap address %q: %v", address, err)
            lastErr = err
            continue
        }
        if _, ok := seen[info.ID]; ok {
            continue
        }
        seen[info.ID] = struct{}{}

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
        return lastErr
    }

    if err := kad.Bootstrap(ctx); err != nil {
        return err
    }
    log.Println("DHT bootstrap completed")
    return nil
}

func announceAndDiscover(
    ctx context.Context,
    node host.Host,
    kad *dht.IpfsDHT,
    relays []peer.AddrInfo,
    enableRelay bool,
) {
    key := rendezvousCID()
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()

    announce := func() {
        if enableRelay && len(kad.RoutingTable().ListPeers()) == 0 {
            return
        }
        pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
        defer cancel()
        if err := kad.Provide(pctx, key, true); err != nil {
            log.Printf("provide rendezvous: %v", err)
        }
    }

    discover := func() {
        pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
        defer cancel()
        providers, err := kad.FindProviders(pctx, key)
        if err != nil {
            log.Printf("find providers: %v", err)
            return
        }
        for _, info := range providers {
            if info.ID == node.ID() || isRelay(info.ID, relays) {
                continue
            }
            go connectToPeer(ctx, node, kad, relays, info.ID)
        }
    }

    time.Sleep(3 * time.Second)
    announce()
    discover()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            announce()
            discover()
        }
    }
}

func watchAndDial(
    ctx context.Context,
    node host.Host,
    kad *dht.IpfsDHT,
    relays []peer.AddrInfo,
    enableRelay bool,
) {
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

            conns := node.Network().Conns()
            log.Printf("Live connections: %d", len(conns))
            seenConn := make(map[peer.ID]struct{})
            for _, c := range conns {
                p := c.RemotePeer()
                if _, ok := seenConn[p]; !ok {
                    log.Printf("  live %s via %v", p, c.RemoteMultiaddr())
                    seenConn[p] = struct{}{}
                }
            }

            allKnown := node.Peerstore().PeersWithAddrs()
            log.Printf("Peerstore knows %d peer(s) with addresses", len(allKnown))
            for _, p := range allKnown {
                if p == node.ID() {
                    continue
                }
                log.Printf("  peer %s addrs: %v", p, node.Peerstore().Addrs(p))
                pcs := node.Network().ConnsToPeer(p)
                if len(pcs) == 0 {
                    log.Printf("    no active connection")
                    if !isRelay(p, relays) {
                        go connectToPeer(ctx, node, kad, relays, p)
                    }
                    continue
                }
                for _, c := range pcs {
                    log.Printf("    active conn via: %v", c.RemoteMultiaddr())
                }
            }

            if enableRelay && len(tablePeers) == 0 {
                continue
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

var dialing sync.Map

func connectToPeer(
    ctx context.Context,
    node host.Host,
    kad *dht.IpfsDHT,
    relays []peer.AddrInfo,
    target peer.ID,
) {
    if target == node.ID() || isRelay(target, relays) {
        return
    }
    if _, loaded := dialing.LoadOrStore(target, struct{}{}); loaded {
        return
    }
    defer dialing.Delete(target)

    if len(node.Network().ConnsToPeer(target)) == 0 {
        fctx, cancel := context.WithTimeout(ctx, 20*time.Second)
        info, err := kad.FindPeer(fctx, target)
        cancel()
        if err != nil {
            log.Printf("find peer %s: %v", target, err)
        } else if len(info.Addrs) > 0 {
            cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
            err = node.Connect(cctx, info)
            cancel()
            if err != nil {
                log.Printf("FindPeer connect to %s failed: %v", target, err)
            } else {
                log.Printf("connected to %s via FindPeer addrs %v", target, info.Addrs)
            }
        }

        if len(node.Network().ConnsToPeer(target)) == 0 {
            var addrs []ma.Multiaddr
            addrs = append(addrs, node.Peerstore().Addrs(target)...)
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
            cctx, cancel := context.WithTimeout(ctx, 25*time.Second)
            err := node.Connect(cctx, peer.AddrInfo{ID: target, Addrs: addrs})
            cancel()
            if err != nil {
                log.Printf("circuit/addr connect to %s failed: %v", target, err)
                return
            }
            log.Printf("connected to %s", target)
        }
    }

    if len(node.Network().ConnsToPeer(target)) == 0 {
        return
    }

    _, _ = kad.RoutingTable().TryAddPeer(target, true, false)
    openTestStream(ctx, node, target)
}

func openTestStream(ctx context.Context, node host.Host, target peer.ID) {
    sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
    defer cancel()
    s, err := node.NewStream(sctx, target, protocolID)
    if err != nil {
        log.Printf("stream to %s failed: %v", target, err)
        return
    }
    defer s.Close()
    log.Printf("stream ok to %s via %v", target, s.Conn().RemoteMultiaddr())
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

func isRelay(id peer.ID, relays []peer.AddrInfo) bool {
    for _, r := range relays {
        if r.ID == id {
            return true
        }
    }
    return false
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
    log.Printf("got %s from %s via %v", protocolID, s.Conn().RemotePeer(), s.Conn().RemoteMultiaddr())
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
