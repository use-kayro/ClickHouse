#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <base/types.h>

namespace DB::Cas
{

/// The object-storage keys of one content-addressed pool. Since TEC-851 step 3c a pool holds two kinds of
/// object only:
///   - blob bodies:    POOL/blobs/ALGO/S/HEX
///   - pool metadata:  POOL/_pool_meta
///
/// A blob key carries its `BlobHashAlgo` as a path segment (`blobHashAlgoName(ref.algo)`: `"ch128"`, `"xxh3"`,
/// `"sha256"`), with `S`/`HEX` taken from `ref.digest` at the algo's own width, so a pool may hold blobs under
/// several algos at once. `S` is the first two characters of `HEX`.
class Layout
{
public:
    explicit Layout(String prefix_) : prefix(std::move(prefix_)) {}

    String blobKey(const BlobRef & ref) const;
    String blobsPrefix() const { return prefix + "/blobs/"; }
    String poolMetaKey() const { return prefix + "/_pool_meta"; }

    /// The pool's root key prefix, i.e. the constructor's `prefix_` verbatim.
    const String & poolPrefix() const { return prefix; }

private:
    String prefix;
};

}
