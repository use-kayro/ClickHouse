#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>

#include <limits>

namespace DB::Cas
{

uint64_t CasRequestBudget::attemptEnvelopeMs() const
{
    /// The TCP connect and the TLS handshake each get one connect interval from Poco, so an HTTPS
    /// attempt may spend two caps before any request I/O. Scheme-agnostic on purpose: conservative for
    /// plain HTTP, exact for HTTPS.
    const uint64_t cap = connect_timeout_cap_ms.value_or(0);
    const uint64_t connects = cap > std::numeric_limits<uint64_t>::max() / 2 ? std::numeric_limits<uint64_t>::max() : 2 * cap;
    return attempt_timeout_ms > std::numeric_limits<uint64_t>::max() - connects
        ? std::numeric_limits<uint64_t>::max()
        : attempt_timeout_ms + connects;
}

}
