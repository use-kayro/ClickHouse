#pragma once
#include <cstdint>
#include <optional>

namespace DB::Cas
{

/// The limits every pool request is issued under.
struct CasRequestBudget
{
    /// Maximum client wait budgeted for one HTTP attempt. The request contract reserves this before
    /// every attempt it starts; the actual socket-level wait is configured on the object storage's
    /// client (the object storage backend's single-attempt client), not by this struct.
    uint64_t attempt_timeout_ms = 5000;

    /// The cap the single-attempt client puts on one TCP connect and again on one TLS handshake,
    /// frozen when the pool opens as `min(disk connect_timeout_ms, attempt_timeout_ms)` (a configured
    /// zero means unbounded and is normalized to the attempt timeout) so a later client reload cannot
    /// widen the envelope. Empty when the storage has no S3 client: the envelope is the attempt alone.
    std::optional<uint64_t> connect_timeout_cap_ms = 1000;

    /// What ONE physical attempt is allowed to cost end to end: two connect caps (TCP, then TLS) plus
    /// the attempt timeout, saturating. The request contract reserves this, not the bare attempt timeout, before
    /// every attempt it starts.
    uint64_t attemptEnvelopeMs() const;
};

}
