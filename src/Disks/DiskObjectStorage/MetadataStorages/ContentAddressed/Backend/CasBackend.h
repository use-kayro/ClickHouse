#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasTransportAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/types.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace DB::Cas
{

/// Re-readable payload transport for one unconditional blob publication. `fresh_envelope` is the
/// complete CAS envelope to prepend, while `payload_size` is the exact number of bytes the source
/// must yield before the backend can make the destination visible.
struct StreamingBlobPublication
{
    uint64_t payload_size;
    String fresh_envelope;
    std::function<std::unique_ptr<ReadBuffer>()> open_payload;
};

/// Transport-only request for an unconditional blob rewrite. Publication does not inspect destination
/// state or freshness metadata and does not produce an incarnation token.
struct BlobPublishRequest
{
    String destination_key;
    StreamingBlobPublication publication;
};

namespace blob_publication_detail
{

struct BlobPayloadCopyResult
{
    uint64_t copied = 0;
    bool has_excess = false;

    bool exact(uint64_t expected) const
    {
        return copied == expected && !has_excess;
    }
};

/// Copy no more than the declared payload, then consume one byte solely to distinguish exact input
/// from a long source. The probe byte never reaches `to`, so callers can cancel without having sent
/// more than the promised body to their destination transport.
inline BlobPayloadCopyResult copyBlobPayloadBounded(ReadBuffer & from, WriteBuffer & to, uint64_t expected)
{
    uint64_t remaining = expected;
    while (remaining != 0 && !from.eof())
    {
        const size_t available = static_cast<size_t>(from.buffer().end() - from.position());
        const size_t count = static_cast<size_t>(std::min<uint64_t>(remaining, available));
        to.write(from.position(), count);
        from.position() += count;
        remaining -= count;
    }

    BlobPayloadCopyResult result{.copied = expected - remaining};
    if (remaining == 0)
    {
        char excess;
        result.has_excess = from.read(excess);
    }
    return result;
}

}

/// Etag-aware storage seam used by the content-addressed pool. INCARNATION SEMANTICS ARE THE
/// CONTRACT:
///   - every present key has exactly one current incarnation identified by an opaque backend value;
///   - `write` with an `expected_value` succeeds only against that exact current value (or expected
///     absence);
///   - `remove` removes ONLY the incarnation whose value matches — a mismatch MUST report
///     `RawRemoval::Mismatch` with the object untouched (backends that silently ignore the condition
///     must not serve a pool: the exact delete GC issues relies on it).
///
/// VALUE ⟹ CONTENT PRECONDITION (read-path caches depend on this): a value must uniquely identify the
/// byte-content of the incarnation it labels — i.e. `head(k)`'s value equalling a prior `read(k)`'s
/// value MUST imply the bytes are unchanged. The protocol's SAFETY only needs the contrapositive
/// (changed bytes ⟹ a new value, so a stale conditional write/delete is rejected). Holds for every
/// backend in use: S3 ETag is content-derived; the emulated/in-memory backends mint a strictly-monotonic
/// sequence that is never reused. A backend with a weak/recycled value must NOT be used as a Cas pool;
/// nothing verifies it at open, so it is a requirement of every backend implementation.
///
/// Most ops take/return whole `String` bodies — sufficient for `_pool_meta`. Large content blobs use the transport-only `publish` seam; reads stay String-based because
/// blob payload reads go through the wiring's read stack, not this seam.
class Backend
{
public:
    Backend();
    virtual ~Backend() = default;

    /// ---- The transport primitives ----
    ///
    /// These are the ONLY methods that reach the store. Each takes a `TransportAccess`, which nothing
    /// outside `CasRequests` can construct, so no caller can reach the store without the request
    /// contract's retry, deadline and fence rules. They deal in the store's own strings: an
    /// incarnation VALUE means no more here than "what the store answered", and every grammar,
    /// key-binding and dialect check on it belongs to `CasRequests`.

    /// An object's bytes together with the value naming the incarnation they were read from.
    struct Raw     { String bytes; String value; };
    /// One object's size and incarnation value, without its body.
    struct RawMeta { uint64_t size; String value; };
    /// One listed key; `value` is present only on a backend that surfaces per-key incarnations
    /// through LIST -- see `supportsListTokens`.
    struct RawListedKey { String key; uint64_t size; std::optional<String> value; };
    struct RawListPage  { std::vector<RawListedKey> keys; String next_cursor; };
    /// The store refused the write's precondition. Nothing was written; what the key holds now is
    /// whatever a read finds. An expected outcome, never an error.
    struct RawConflict {};
    /// `DeleteMarker` is a removal that did NOT reclaim: a versioned bucket archived a noncurrent
    /// version instead. Distinct from `Removed` because reclaiming the storage is the point.
    enum class RawRemoval : uint8_t { Removed, Gone, Mismatch, DeleteMarker };

    /// Reads the whole object, or nullopt when the key is absent.
    virtual std::optional<Raw>     read  (const String & key, TransportAccess &) = 0;
    /// One point-in-time observation of the current incarnation's size and value; nullopt when absent.
    virtual std::optional<RawMeta> head  (const String & key, TransportAccess &) = 0;
    /// One page of keys under `prefix`, resuming strictly after `cursor`; an empty `next_cursor`
    /// marks the end of the enumeration.
    virtual RawListPage            list  (const String & prefix, const String & cursor, size_t limit, TransportAccess &) = 0;
    /// Removes ONLY the incarnation whose value equals `expected_value`; a mismatch must leave the
    /// object untouched.
    virtual RawRemoval             remove(const String & key, const String & expected_value, TransportAccess &) = 0;
    /// Creates the key (`expected_value == nullopt`) or replaces exactly the incarnation named by
    /// `expected_value`. Returns the store's own value for what it just wrote, UNVALIDATED: a value
    /// that fails the dialect grammar means the write may still have landed, which the caller settles
    /// by reading the key back, and which this seam must never report as corruption.
    virtual std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                                     const std::optional<String> & expected_value, TransportAccess &) = 0;
    /// A forward-only read of a WRITE-ONCE object: nothing is materialized by the seam.
    /// MUTABLE objects (`_pool_meta`) MUST use `read` -- their bytes may change
    /// under an open stream. Null when the key is absent.
    virtual std::unique_ptr<ReadBuffer> stream(const String & key, TransportAccess &) = 0;
    /// Executes one unconditional blob publication and returns once the complete destination is
    /// visible. Transport only: it observes no destination state and produces no incarnation.
    virtual void publish(const BlobPublishRequest & request, TransportAccess &) = 0;

    /// The dialect this backend mints its incarnation values in.
    virtual Dialect dialect() const = 0;

    /// Identifies this backend INSTANCE, so an incarnation observed elsewhere can be refused rather
    /// than used as a precondition here. Assigned at construction and never reused in this process.
    uint64_t backendId() const { return backend_id; }

    /// The budget for one HTTP attempt in milliseconds; 0 when this backend has no such notion. The
    /// request contract reserves it before every attempt it starts.
    virtual uint64_t attemptTimeoutMs() const { return 0; }

    /// What one attempt may cost end to end, connect included; the contract reserves THIS. A backend
    /// with no connect notion answers its attempt timeout. A decorator that forwards `attemptTimeoutMs`
    /// to an inner backend must forward THIS too -- the default falls back to `attemptTimeoutMs`, which
    /// would silently drop the inner backend's connect contribution.
    virtual uint64_t attemptEnvelopeMs() const { return attemptTimeoutMs(); }

    /// Asks the storage to re-acquire credentials. TRUE when fresh ones were installed, so the
    /// caller's reissue can sign with them; FALSE when this backend has no refresh mechanism, which
    /// makes an expired-credential failure terminal for the caller's policy rather than retryable.
    virtual bool refreshCredentials() { return false; }

    /// Capability fact about the LIST seam: TRUE iff this backend can surface a per-key incarnation
    /// value through `list` (i.e. each `RawListedKey` carries a value that uniquely identifies the
    /// current incarnation of that key, matching what `head` would return).
    ///
    /// Why this matters: S3 ETags are content-derived and are returned in list responses; the
    /// in-memory backend mints a monotonic value it can also surface through `list`. A backend that
    /// cannot surface per-key values through `list` MUST return FALSE.
    ///
    /// FALSE ⇒ GC `discover` must read every root-shard body to learn the current incarnation (fail
    /// closed). TRUE  ⇒ `discover` may skip an unchanged root-shard body read when the listed value
    /// equals the persisted folded one, saving a GET per unchanged shard.
    virtual bool supportsListTokens() const = 0;

    /// Pool-level preconditions beyond per-op conditional semantics, checked by `Pool::open` on the
    /// writer before any request. Default: nothing to check. The S3 backend refuses here when a
    /// generation-dialect (GCS) bucket is verified to have object versioning enabled: a token-exact
    /// DELETE against a versioned bucket archives a noncurrent generation instead of reclaiming
    /// storage, so GC "reclaim" would silently stop reclaiming. A probe that cannot answer is not
    /// evidence of that, so it warns and the mount proceeds.
    virtual void checkPoolPreconditions() {}

    /// Fail-closed precondition: a Native-mode backend MUST have a
    /// working single-attempt conditional-write path before it coordinates a WRITABLE pool — silently
    /// running CAS conditional writes under the disk's default (~500-attempt) transparent retry policy
    /// is exactly the hazard this seam forbids. Asked by `Pool::open` on the writer, alongside
    /// `checkPoolPreconditions`, before any request. Default: nothing to check (EmulatedSingleProcess and non-S3 backends
    /// are not gated here — see ObjectStorageBackend's override for the one backend that is).
    virtual void checkConditionalWriteSingleAttemptSupport() {}

private:
    uint64_t backend_id;
};

inline Backend::Backend()
{
    /// Per instance, monotonic, never reused: an incarnation carries the id of the backend that
    /// observed it, so it can be refused anywhere else. Starts at 1, leaving 0 naming no backend.
    static std::atomic<uint64_t> next_backend_id{1};
    backend_id = next_backend_id.fetch_add(1, std::memory_order_relaxed);
}

using BackendPtr = std::shared_ptr<Backend>;

}
