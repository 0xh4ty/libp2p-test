package network

import (
    "github.com/libp2p/go-libp2p"
    "github.com/libp2p/go-libp2p/core/crypto"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/peer"
)

func NewHost(
    privKey crypto.PrivKey,
    enableRelay bool,
    staticRelays []peer.AddrInfo,
) (host.Host, error) {

    if enableRelay {
        options := []libp2p.Option{
            libp2p.Identity(privKey),
            libp2p.ListenAddrStrings(
                "/ip4/0.0.0.0/tcp/4001",
                "/ip4/0.0.0.0/udp/4001/quic-v1",
            ),
            libp2p.EnableAutoNATv2(),
            libp2p.EnableNATService(),
            libp2p.EnableRelay(),
            libp2p.EnableHolePunching(),
            libp2p.ForceReachabilityPublic(),
            libp2p.EnableRelayService(),
        }
        return libp2p.New(options...)
    }

    if len(staticRelays) > 0 {
        options := []libp2p.Option{
            libp2p.Identity(privKey),
            libp2p.ListenAddrStrings(
                "/ip4/0.0.0.0/tcp/4001",
                "/ip4/0.0.0.0/udp/4001/quic-v1",
            ),
            libp2p.EnableAutoNATv2(),
            libp2p.EnableNATService(),
            libp2p.EnableRelay(),
            libp2p.EnableHolePunching(),
            libp2p.EnableAutoRelayWithStaticRelays(staticRelays),
        }
        return libp2p.New(options...)
    }

    options := []libp2p.Option{
        libp2p.Identity(privKey),
        libp2p.ListenAddrStrings(
            "/ip4/0.0.0.0/tcp/4001",
            "/ip4/0.0.0.0/udp/4001/quic-v1",
        ),
        libp2p.EnableAutoNATv2(),
        libp2p.EnableNATService(),
        libp2p.EnableRelay(),
        libp2p.EnableHolePunching(),
    }

    return libp2p.New(options...)
}
