#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>

namespace ProfileEvents
{
/// The CA per-namespace and per-operation events declared in `ProfileEvents.cpp`.
extern const Event CASBlobPut;
extern const Event CASBlobPutDeduplicated;
extern const Event CASBlobOverwrite;
extern const Event CASBlobCompareSwap;
extern const Event CASBlobCompareSwapConflict;
extern const Event CASBlobHead;
extern const Event CASBlobHeadMiss;
extern const Event CASBlobGet;
extern const Event CASBlobGetStream;
extern const Event CASBlobDelete;
extern const Event CASBlobList;

extern const Event CASOtherPut;
extern const Event CASOtherPutDeduplicated;
extern const Event CASOtherOverwrite;
extern const Event CASOtherCompareSwap;
extern const Event CASOtherCompareSwapConflict;
extern const Event CASOtherHead;
extern const Event CASOtherHeadMiss;
extern const Event CASOtherGet;
extern const Event CASOtherGetStream;
extern const Event CASOtherDelete;
extern const Event CASOtherList;
}

namespace DB::Cas
{

/// Maps `(CasNs, CasOp)` to the corresponding `ProfileEvents::Event`. The table is row-major: the
/// outer index is the namespace and the inner index is the operation. Its rows and columns must stay
/// in lockstep with the `CasNs` and `CasOp` enum orderings.
///
/// `CasOp::Read` deliberately keeps the `CAS*Get` event names: renaming a user-visible ProfileEvent
/// is its own change, made when the events are retired, and nothing outside this table would have
/// been improved by doing it here.
static const ProfileEvents::Event cas_event_table[CAS_NS_COUNT][CAS_OP_COUNT] =
{
    /* Blob   */ {ProfileEvents::CASBlobPut, ProfileEvents::CASBlobPutDeduplicated, ProfileEvents::CASBlobOverwrite,
                  ProfileEvents::CASBlobCompareSwap, ProfileEvents::CASBlobCompareSwapConflict, ProfileEvents::CASBlobHead,
                  ProfileEvents::CASBlobHeadMiss, ProfileEvents::CASBlobGet, ProfileEvents::CASBlobGetStream,
                  ProfileEvents::CASBlobDelete, ProfileEvents::CASBlobList},
    /* Other  */ {ProfileEvents::CASOtherPut, ProfileEvents::CASOtherPutDeduplicated, ProfileEvents::CASOtherOverwrite,
                  ProfileEvents::CASOtherCompareSwap, ProfileEvents::CASOtherCompareSwapConflict, ProfileEvents::CASOtherHead,
                  ProfileEvents::CASOtherHeadMiss, ProfileEvents::CASOtherGet, ProfileEvents::CASOtherGetStream,
                  ProfileEvents::CASOtherDelete, ProfileEvents::CASOtherList},
};

CasNs classifyCasNs(const String & key)
{
    if (key.find("/blobs/") != String::npos)
        return CasNs::Blob;
    return CasNs::Other;
}

void incrementCasEvent(CasNs ns, CasOp op)
{
    ProfileEvents::increment(cas_event_table[static_cast<size_t>(ns)][static_cast<size_t>(op)]);
}

void InstrumentedBackend::publish(const BlobPublishRequest & request, TransportAccess & access)
{
    inner->publish(request, access);
    incrementCasEvent(classifyCasNs(request.destination_key), CasOp::Put);
}

}
