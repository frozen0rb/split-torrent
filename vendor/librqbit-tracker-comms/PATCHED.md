# Vendored librqbit-tracker-comms 9.0.1

Unmodified crates.io 9.0.1 except `UdpTrackerClient::new` in
`src/tracker_comms_udp.rs`: if binding `[::]:0` fails (no IPv6 stack), it
binds `0.0.0.0:0` instead. Drop this patch once upstream librqbit handles it.
