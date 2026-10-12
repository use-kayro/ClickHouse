#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <base/types.h>
#include <optional>
#include <set>
#include <vector>

namespace DB::Cas
{

/// One writer build prefix under `cas/manifests/<ns>/`: the canonical hex `<epoch-hex>-<seq-hex>/`
/// directory encoded as canonical hexadecimal epoch and sequence components.
struct BuildPrefix
{
    uint64_t writer_epoch = 0;
    uint64_t build_sequence = 0;
};

/// Per-namespace pre-precommit orphan sweep. Deletes
/// manifest bodies written before `PrecommitAdd` and never named by any live owner, scoped to ONE
/// namespace + ONE build prefix. Rules:
///   - eligibility from the durable watermark fact only: the retired sentinel
///     (`min_active_build_sequence == UINT64_MAX`), or `min_active_build_sequence > build_sequence`, or a replaced incarnation —
///     NEVER a frozen-seq / judged-dead heuristic alone (a missing watermark => not eligible);
///   - the active `ManifestId` set comes from the namespace's committed + live-precommit owner view;
///   - delete only bodies whose `ManifestId` is ABSENT from the active set, by exact token;
///   - a 404 between listing and deletion is record-and-continue, never a throw;
///   - never GETs a condemned body to revive it — eligibility +
///     exact-token delete only.
/// Returns the number of bodies actually deleted (a `Removal::Removed` exact-token
/// delete only, never a spared `Gone`/`Mismatch`) — the decommission manifest-debris drain
/// (`Tools/CasDecommission.cpp`) sums this across every eligible build prefix into
/// `DecommissionReport::manifest_debris_removed`.
///
/// A per-key transient failure (a thrown backend exception on `head`/`remove`) is pushed onto
/// `warnings` and the sweep continues with the next key; a namespace whose protection view is
/// unavailable (missing `_ckpt`, corrupt snapshot) also pushes a "cannot confirm emptiness" warning
/// and is skipped. `Gone`/`Mismatch` delete outcomes are spared silently -- the normal "a fresh owner
/// reclaimed it" race, not a failure to warn about.
/// This direct decommission path relies on the caller's held server-root claim/fence: while that claim
/// is held, a same-server-root rebirth cannot become live between its catalog cut and exact-token delete.
/// It still rejects every catalog cut with an ambiguous current life id before making any deletion decision.
/// `listed_server_roots` is as for `prefixEligible`.
uint64_t sweepNamespace(Pool & store, const RootNamespace & ns, const BuildPrefix & prefix,
                        const std::set<String> & listed_server_roots, std::vector<String> & warnings);

/// The server-root ids with a `/mount` key under `gc/server-roots/` (one `listMounts` pass): the
/// `listed_server_roots` a caller takes once and passes to every `prefixEligible`/`sweepNamespace` call
/// it makes.
std::set<String> listServerRoots(Pool & store);

/// Whether `prefix` is sweep-eligible by the durable watermark fact alone. The floor is read from the
/// mount lease identified by the namespace's server-root prefix, not inferred from the manifest key or a
/// judged-dead heuristic. A missing lease provides no deletion authority, so the prefix is not eligible.
/// Only the server roots in `listed_server_roots` (the caller's LIST of `gc/server-roots/`) are tried: a
/// mount younger than that LIST is not found, and so retains, until the caller lists again.
bool prefixEligible(Pool & store, const RootNamespace & ns, const BuildPrefix & prefix,
                    const std::set<String> & listed_server_roots);

/// The pure half of `prefixEligible`: whether `prefix` is retired under one observation of the mount
/// floor. `nullopt` (no mount body under any prefix of the namespace) admits nothing. Retirement is
/// permanent -- the epoch and the acknowledgement floor only grow and the farewell is terminal -- so
/// an admission derived from any observation stays true afterwards.
bool prefixEligibleUnder(const std::optional<MountLease> & floor, const BuildPrefix & prefix);

}
