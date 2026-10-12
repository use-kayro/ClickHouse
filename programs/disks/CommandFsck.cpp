#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Common/Exception.h>
#include <ICommand.h>

#include <chrono>
#include <iostream>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandFsck final : public ICommand
{
public:
    CommandFsck() : ICommand("CommandFsck")
    {
        command_name = "cas-fsck";
        description = "Independently verify content-addressed pool reachability (read-only). "
                      "Exits nonzero if any reachable object is missing (dangling).";
        options_description.add_options()("detail", "list per-object rows (class, key, size, reachable_from)")(
            "timeout", po::value<UInt64>(), "abort the scan after N seconds with a clear error instead of hanging (default 600; 0 = unbounded)")(
            "namespace", po::value<String>(), "scope the scan to namespaces with this prefix (skips the pool-wide "
                                               "physical/pipeline classification; still reports the scoped namespaces' "
                                               "dangling refs and orphan-manifest debris as unreachable)")(
            "partial", "on --timeout, print the counts accumulated so far flagged partial=1 instead of aborting empty-handed");
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const bool detail = options.contains("detail");
        const UInt64 timeout_sec = getValueFromCommandLineOptionsWithDefault<UInt64>(options, "timeout", 600);
        const String namespace_prefix = options.contains("namespace") ? options["namespace"].as<String>() : "";
        const bool partial = options.contains("partial");
        auto disk = client.getCurrentDiskWithPath().getDisk();

        auto * dos = dynamic_cast<DiskObjectStorage *>(disk.get());
        if (!dos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-fsck: '{}' is not an object-storage disk", disk->getName());

        auto * ca = dynamic_cast<ContentAddressedMetadataStorage *>(dos->getMetadataStorage().get());
        if (!ca)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-fsck: disk '{}' is not content-addressed", disk->getName());

        if (!ca->isReadOnly())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "cas-fsck: open the CA disk read-only (<readonly>true</readonly>) so inspection never probes/schedules a live pool");

        /// Progress to stderr so a long scan is visibly working (the reachable=… summary stays on
        /// stdout, machine-parseable). The deadline bounds a slow-but-progressing scan with a clear
        /// error; for a single LIST page stuck in S3-client retries, lower the disk's S3 retry budget.
        Cas::FsckProgress on_progress = [](std::string_view phase, uint64_t objects, uint64_t pages)
        {
            std::cerr << "cas-fsck: " << phase << " — " << objects << " objects, " << pages << " pages\n";
        };
        std::optional<std::chrono::steady_clock::time_point> deadline;
        if (timeout_sec > 0)
            deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);

        const Cas::FsckReport report = Cas::runFsck(*ca->store(), detail, on_progress, deadline, partial, namespace_prefix);

        /// Built by `Cas::formatFsckSummary` rather than here, so the line is reachable from a unit test.
        std::cout << Cas::formatFsckSummary(report) << "\n";

        /// `unchecked` is not a finding and does not exit nonzero — it says the audit could not PROVE
        /// those namespaces either way, which is a statement about coverage. Saying so out loud is the
        /// whole point: a silent verdict of "no complaints" would read as a clean bill of health.
        if (report.unchecked > 0)
            std::cout << "note: " << report.unchecked
                      << " namespace(s) could NOT be proved either way (an unprovable epoch crossing, an "
                         "unreadable record, or a namespace the scan could not examine) — this run says "
                         "nothing about them; the per-namespace reason is listed as an `unchecked` row "
                         "under --detail\n";
        if (report.unaccounted > 0)
            std::cout << "note: " << report.unaccounted
                      << " present blob(s) no live ref references (listed as `unaccounted` rows under --detail)\n";
        /// Not a finding: a canonical namespace-life key whose life is absent from the catalog is debris
        /// of a removed namespace.
        if (report.namespace_janitor_pending > 0)
            std::cout << "note: " << report.namespace_janitor_pending
                      << " namespace-life object(s) (" << report.namespace_janitor_pending_bytes
                      << " byte(s) across " << report.namespace_janitor_pending_lives
                      << " life/lives) belong to a namespace whose catalog row is already gone — not "
                         "corruption (listed as `janitor-pending` rows under --detail)\n";

        if (detail)
        {
            for (const auto & o : report.objects)
            {
                const char * c = "unreachable"; // NOLINT(clang-analyzer-deadcode.DeadStores) - defensive fallback if the enum grows
                switch (o.cls)
                {
                    case Cas::FsckClass::Reachable:   c = "reachable"; break;
                    case Cas::FsckClass::Dangling:    c = "dangling"; break;
                    case Cas::FsckClass::Unreachable: c = "unreachable"; break;
                    case Cas::FsckClass::Unaccounted: c = "unaccounted"; break;
                    case Cas::FsckClass::ChainBroken: c = "chain-broken"; break;
                    case Cas::FsckClass::Unchecked:   c = "unchecked"; break;
                    case Cas::FsckClass::LifelessKey: c = "lifeless-key"; break;
                    case Cas::FsckClass::JanitorPending: c = "janitor-pending"; break;
                }
                std::cout << c << "\t" << o.key << "\t" << o.size;
                for (const auto & r : o.reachable_from)
                    std::cout << "\t" << r;
                std::cout << "\n";
            }
        }

        if (report.dangling > 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "cas-fsck: {} reachable object(s) MISSING (INV-NO-LOSS violation)", report.dangling);
        /// A hole in a ref stream is loss of a different kind: the records above it are unreachable, so
        /// the table's own history is truncated wherever recovery next reads it. Fatal in the summary AND
        /// in the exit code (spec §7) — a verdict only a `--detail` reader would notice is a verdict no
        /// automation acts on.
        if (report.chain_broken > 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "cas-fsck: {} namespace(s) have a HOLE in their ref-log stream — an id is absent below a "
                "durable id of the same epoch, which contiguity (INV-1) makes impossible without a lost "
                "record; every transaction above the hole is unreachable (positions are listed as "
                "`chain-broken` rows under --detail)", report.chain_broken);
        /// A key the `Layout` parsers refuse (no current writer can produce it), or a catalog
        /// incarnation that is ambiguous or unreadable, is corruption nothing clears on its own: the
        /// namespace enumeration now reports it instead of aborting, which is what makes an exit code
        /// the only signal automation can act on. A COMPLETE, canonical namespace-life key whose life is
        /// simply absent from the catalog is NOT counted here — see the `janitor-pending` note above.
        if (report.lifeless_keys > 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "cas-fsck: {} key(s) under this pool name are malformed or unresolvable — no current "
                "writer could have produced them, or their catalog incarnation is ambiguous/unreadable "
                "(the keys are listed as `lifeless-key` rows under --detail)", report.lifeless_keys);
    }
};

CommandPtr makeCommandFsck()
{
    return std::make_shared<DB::CommandFsck>();
}

}
