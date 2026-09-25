package network

import (
    "time"

    "github.com/libp2p/go-libp2p"
    "github.com/libp2p/go-libp2p/core/connmgr"
    "github.com/libp2p/go-libp2p/core/crypto"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
    ma "github.com/multiformats/go-multiaddr"
    manet "github.com/multiformats/go-multiaddr/net"
)

func NewHost(
    privKey crypto.PrivKey,
    enableRelay bool,
    staticRelays []peer.AddrInfo,
    gater connmgr.ConnectionGater,
) (host.Host, error) {
    options := []libp2p.Option{
        libp2p.Identity(privKey),
        libp2p.ListenAddrStrings(
            "/ip4/0.0.0.0/tcp/4001",
            "/ip4/0.0.0.0/udp/4001/quic-v1",
        ),
        libp2p.EnableAutoNATv2(),
        libp2p.EnableRelay(),
        libp2p.EnableHolePunching(),
        libp2p.ResourceManager(&network.NullResourceManager{}),
    }

    if gater != nil {
        options = append(options, libp2p.ConnectionGater(gater))
    }

    if enableRelay {
        options = append(options,
            libp2p.ForceReachabilityPublic(),
            libp2p.EnableNATService(),
            libp2p.EnableRelayService(
                relayv2.WithResources(relayv2.Resources{
                    Limit: &relayv2.RelayLimit{
                        Duration: 30 * time.Minute,
                        Data:     32 << 20,
                    },
                    ReservationTTL:         time.Hour,
                    MaxReservations:        128,
                    MaxCircuits:            16,
                    BufferSize:             2048,
                    MaxReservationsPerPeer: 4,
                    MaxReservationsPerIP:   32,
                    MaxReservationsPerASN:  64,
                }),
            ),
        )
        return libp2p.New(options...)
    }

    options = append(options, libp2p.AddrsFactory(publicOrCircuitAddrs))

    if len(staticRelays) > 0 {
        options = append(options,
            libp2p.EnableAutoRelayWithStaticRelays(staticRelays),
        )
    }

    return libp2p.New(options...)
}

func publicOrCircuitAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
    out := make([]ma.Multiaddr, 0, len(addrs))
    for _, a := range addrs {
        if _, err := a.ValueForProtocol(ma.P_CIRCUIT); err == nil {
            out = append(out, a)
            continue
        }
        if manet.IsIPLoopback(a) || manet.IsPrivateAddr(a) {
            continue
        }
        out = append(out, a)
    }
    return out
}
