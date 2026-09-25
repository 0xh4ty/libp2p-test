package app

import (
    "bufio"
    "context"
    "crypto/rand"
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "time"

    "github.com/0xh4ty/libp2p-test/internal/network"
    dht "github.com/libp2p/go-libp2p-kad-dht"
    "github.com/libp2p/go-libp2p/core/crypto"
    "github.com/libp2p/go-libp2p/core/host"
    net "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    ma "github.com/multiformats/go-multiaddr"
)

const protocolID = "/quailfs/1.0.0"

func RunNode(enableRelay bool) {
    var privKey crypto.PrivKey
    var filePrivKey crypto.PrivKey

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    homeDir, err := os.UserHomeDir()
    if err != nil {
        panic(err)
    }

    nodeDataDir := filepath.Join(homeDir, ".libp2p-test")

    err = os.MkdirAll(nodeDataDir, 0700)
    if err != nil {
        panic(err)
    }

    incomingDir := filepath.Join(nodeDataDir, "incoming")
    err = os.MkdirAll(incomingDir, 0700)
    if err != nil {
        panic(err)
    }

    outgoingPath := filepath.Join(nodeDataDir, "outgoing.txt")

    keyPath := filepath.Join(nodeDataDir, "identity.key")
    keyBytes, err := os.ReadFile(keyPath)
    if err != nil && !os.IsNotExist(err) {
        panic(err)
    }

    if len(keyBytes) == 0 {
        randomness := rand.Reader

        privKey, _, err = crypto.GenerateKeyPairWithReader(
            crypto.RSA,
            2048,
            randomness,
        )
        if err != nil {
            panic(err)
        }

        keyBytes, err = crypto.MarshalPrivateKey(privKey)
        if err != nil {
            panic(err)
        }

        err = os.WriteFile(keyPath, keyBytes, 0600)
        if err != nil {
            panic(err)
        }
    } else {
        filePrivKey, err = crypto.UnmarshalPrivateKey(keyBytes)
        if err != nil {
            panic(err)
        }

        privKey = filePrivKey
    }

    bootstrapConfigPath := filepath.Join(nodeDataDir, "bootstrap.config")
    bootstrapNodes, err := readBootstrapConfig(bootstrapConfigPath)
    if err != nil {
        log.Printf("%v\n", err)
    }

    var staticRelays []peer.AddrInfo
    if !enableRelay {
        for _, address := range bootstrapNodes {
            maddr, err := ma.NewMultiaddr(address)
            if err != nil {
                log.Printf(
                    "Invalid bootstrap multiaddress %q, skipping for relay use: %v\n",
                    address,
                    err,
                )
                continue
            }

            addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
            if err != nil {
                log.Printf(
                    "Invalid bootstrap peer address %q, skipping for relay use: %v\n",
                    address,
                    err,
                )
                continue
            }

            staticRelays = append(staticRelays, *addrInfo)
        }
    }

    node, err := network.NewHost(privKey, enableRelay, staticRelays)
    if err != nil {
        panic(err)
    }
    defer node.Close()

    fmt.Println("PeerID:", node.ID())

    log.Println("This node's multiaddresses:")
    for _, la := range node.Addrs() {
        log.Printf(" - %v\n", la)
    }
    log.Println()

    time.Sleep(3 * time.Second)

    log.Println("protocols after delay:")
    for _, p := range node.Mux().Protocols() {
        log.Println("  -", p)
    }

    node.SetStreamHandler(protocolID, func(s net.Stream) {
        handleStream(s, incomingDir)
    })

    if enableRelay {
        log.Println("Relay service enabled")
    } else {
        log.Println("Relay service disabled")
    }

    var kad *dht.IpfsDHT

    if len(bootstrapNodes) == 0 {
        kad, err = startPeer(node)
    } else {
        kad, err = startPeerWithBootstrapNodes(
            ctx,
            node,
            bootstrapNodes,
        )
    }
    if err != nil {
        panic(err)
    }

    defer kad.Close()

    go watchRoutingTable(ctx, node, kad, staticRelays, outgoingPath)

    select {}
}

func startPeer(node host.Host) (*dht.IpfsDHT, error) {

    log.Println("Starting DHT...")

    kad, err := dht.New(
        node,
        dht.Mode(dht.ModeServer),
    )
    if err != nil {
        return nil, err
    }

    log.Println("DHT started")

    return kad, nil
}

func startPeerWithBootstrapNodes(
    ctx context.Context,
    node host.Host,
    bootstrapNodes []string,
) (*dht.IpfsDHT, error) {

    log.Println("Starting DHT...")

    kad, err := dht.New(
        node,
        dht.Mode(dht.ModeServer),
    )
    if err != nil {
        return nil, err
    }

    log.Println("DHT started")

    var lastErr error
    var connectedPeer peer.ID
    connectedAny := false

    for _, address := range bootstrapNodes {
        log.Printf("Connecting to bootstrap node: %s\n", address)

        maddr, err := ma.NewMultiaddr(address)
        if err != nil {
            log.Printf(
                "Invalid bootstrap multiaddress %q: %v\n",
                address,
                err,
            )
            lastErr = err
            continue
        }

        addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
        if err != nil {
            log.Printf(
                "Invalid bootstrap peer address %q: %v\n",
                address,
                err,
            )
            lastErr = err
            continue
        }

        connectCtx, cancel := context.WithTimeout(
            ctx,
            10*time.Second,
        )

        err = node.Connect(connectCtx, *addrInfo)

        cancel()

        if err != nil {
            log.Printf(
                "Failed to connect to bootstrap node %s: %v\n",
                address,
                err,
            )
            lastErr = err
            continue
        }

        log.Printf(
            "Connected to bootstrap peer: %s\n",
            addrInfo.ID,
        )

        kad.RoutingTable().TryAddPeer(addrInfo.ID, true, false)

        connectedPeer = addrInfo.ID
        connectedAny = true
    }

    if !connectedAny {
        if lastErr == nil {
            lastErr = fmt.Errorf("no valid bootstrap nodes")
        }
        kad.Close()
        return nil, lastErr
    }

    if err := kad.Bootstrap(ctx); err != nil {
        kad.Close()
        return nil, err
    }

    log.Println("DHT bootstrap completed")

    if err := verifyDHTDiscovery(
        ctx,
        kad,
        connectedPeer,
    ); err != nil {
        kad.Close()
        return nil, err
    }

    return kad, nil
}

func handleStream(s net.Stream, incomingDir string) {
    defer s.Close()

    log.Println("Got a new stream!")

    from := s.Conn().RemotePeer()
    path, n, err := receiveFile(s, incomingDir)
    if err != nil {
        log.Printf("receive from %s failed: %v\n", from, err)
        return
    }
    log.Printf("received %d bytes from %s -> %s\n", n, from, path)
}

func receiveFile(r io.Reader, incomingDir string) (string, int64, error) {
    var nameLen uint32
    if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
        return "", 0, err
    }
    if nameLen == 0 || nameLen > 4096 {
        return "", 0, fmt.Errorf("bad name length %d", nameLen)
    }

    nameBuf := make([]byte, nameLen)
    if _, err := io.ReadFull(r, nameBuf); err != nil {
        return "", 0, err
    }
    name := filepath.Base(string(nameBuf))

    var size uint64
    if err := binary.Read(r, binary.BigEndian, &size); err != nil {
        return "", 0, err
    }

    outPath := filepath.Join(incomingDir, fmt.Sprintf("%d-%s", time.Now().UnixNano(), name))
    f, err := os.Create(outPath)
    if err != nil {
        return "", 0, err
    }
    defer f.Close()

    n, err := io.CopyN(f, r, int64(size))
    if err != nil {
        return "", n, err
    }
    return outPath, n, nil
}

func sendFile(ctx context.Context, node host.Host, target peer.ID, path string) error {
    data, err := os.ReadFile(path)
    if err != nil {
        return err
    }

    sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    s, err := node.NewStream(sctx, target, protocolID)
    if err != nil {
        return err
    }
    defer s.Close()

    name := filepath.Base(path)
    if err := binary.Write(s, binary.BigEndian, uint32(len(name))); err != nil {
        return err
    }
    if _, err := s.Write([]byte(name)); err != nil {
        return err
    }
    if err := binary.Write(s, binary.BigEndian, uint64(len(data))); err != nil {
        return err
    }
    if _, err := s.Write(data); err != nil {
        return err
    }
    return s.Close()
}

func isRelay(id peer.ID, relays []peer.AddrInfo) bool {
    for _, r := range relays {
        if r.ID == id {
            return true
        }
    }
    return false
}

func verifyDHTDiscovery(
    ctx context.Context,
    kad *dht.IpfsDHT,
    target peer.ID,
) error {

    log.Printf(
        "Verifying DHT discovery of peer: %s\n",
        target,
    )

    lookupCtx, cancel := context.WithTimeout(
        ctx,
        15*time.Second,
    )
    defer cancel()

    peers, err := kad.GetClosestPeers(
        lookupCtx,
        string(target),
    )
    if err != nil {
        return fmt.Errorf(
            "DHT peer lookup failed: %w",
            err,
        )
    }

    for _, p := range peers {
        log.Printf(
            "DHT discovered peer: %s\n",
            p,
        )

        if p == target {
            log.Printf(
                "DHT discovery verified: %s\n",
                target,
            )
            return nil
        }
    }

    return fmt.Errorf(
        "DHT lookup completed but target peer %s was not found",
        target,
    )
}

func watchRoutingTable(
    ctx context.Context,
    node host.Host,
    kad *dht.IpfsDHT,
    relays []peer.AddrInfo,
    outgoingPath string,
) {
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()

    var sent sync.Map

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            peers := kad.RoutingTable().ListPeers()
            log.Printf("Routing table size: %d\n", len(peers))
            for _, p := range peers {
                log.Printf("  - %s\n", p)
            }

            for _, c := range node.Network().Conns() {
                p := c.RemotePeer()
                if p == node.ID() || isRelay(p, relays) {
                    continue
                }
                if _, ok := sent.Load(p); ok {
                    continue
                }
                if _, err := os.Stat(outgoingPath); err != nil {
                    log.Printf("outgoing file missing: %v\n", err)
                    continue
                }
                sent.Store(p, struct{}{})
                go func(target peer.ID) {
                    log.Printf("sending %s to %s\n", outgoingPath, target)
                    if err := sendFile(ctx, node, target, outgoingPath); err != nil {
                        sent.Delete(target)
                        log.Printf("send to %s failed: %v\n", target, err)
                        return
                    }
                    log.Printf("sent %s to %s via %v\n", outgoingPath, target, node.Network().ConnsToPeer(target))
                }(p)
            }

            refreshErrCh := kad.RefreshRoutingTable()
            go func() {
                if err := <-refreshErrCh; err != nil {
                    log.Printf("Routing table refresh error: %v\n", err)
                }
            }()
        }
    }
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
