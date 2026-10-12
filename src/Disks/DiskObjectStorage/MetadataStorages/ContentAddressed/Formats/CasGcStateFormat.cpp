#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace GcStateWire
{
    constexpr WireKey round{"round"};
    constexpr WireKey gc_shards{"gc_shards"};
    constexpr WireKey snap_generation{"snap_generation"};
    constexpr WireKey snap_pruned_through{"snap_pruned_through"};
    constexpr WireKey snap_attempt{"snap_attempt"};
    constexpr WireKey manifest_sweep_cursor{"manifest_sweep_cursor"};
    constexpr WireKey lease_owner{"lease_owner"};
    constexpr WireKey lease_seq{"lease_seq"};
}

String encodeGcState(const GcState & state)
{
    if (state.gc_shards < 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "encodeGcState: gc_shards must be >= 1 -- refusing to persist an unreadable gc/state");
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::GcState);
    bool first = true;
    writeU64StringField(out, GcStateWire::round, state.round, first);
    writeNumberField(out, GcStateWire::gc_shards, state.gc_shards, first);
    writeU64StringField(out, GcStateWire::snap_generation, state.snap_generation, first);
    writeU64StringField(out, GcStateWire::snap_pruned_through, state.snap_pruned_through, first);
    writeU64StringField(out, GcStateWire::snap_attempt, state.snap_attempt, first);
    writeStringField(out, GcStateWire::manifest_sweep_cursor, state.manifest_sweep_cursor, first);
    writeHex128Field(out, GcStateWire::lease_owner, state.lease.owner, first);
    writeU64StringField(out, GcStateWire::lease_seq, state.lease.seq, first);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

GcState decodeGcState(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::GcState);
    const String body = readLine(in, traitsFor(FormatId::GcState).line_cap, "gc/state");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "gc/state");

    GcState state;
    bool saw_gcs = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == GcStateWire::round)
            state.round = r.readU64String();
        else if (key == GcStateWire::gc_shards)
        {
            state.gc_shards = r.readU64Number();
            saw_gcs = true;
        }
        else if (key == GcStateWire::snap_generation)
            state.snap_generation = r.readU64String();
        else if (key == GcStateWire::snap_pruned_through)
            state.snap_pruned_through = r.readU64String();
        else if (key == GcStateWire::snap_attempt)
            state.snap_attempt = r.readU64String();
        else if (key == GcStateWire::manifest_sweep_cursor)
            state.manifest_sweep_cursor = r.readString();
        else if (key == GcStateWire::lease_owner)
            state.lease.owner = r.readHex128();
        else if (key == GcStateWire::lease_seq)
            state.lease.seq = r.readU64String();
        else
            r.skipUnknown(key);
    }
    /// Fail closed on an absent gc_shards: the writer always emits it, so a missing key means a corrupt object.
    /// Do NOT silently keep the struct default (1) — that would hide corruption (no-fallback principle).
    if (!saw_gcs)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc/state: missing gc_shards");
    if (state.gc_shards == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc/state: gc_shards must be >= 1");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc/state: trailing bytes");
    return state;
}

}
