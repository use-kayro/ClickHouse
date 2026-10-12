#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>

namespace DB::Cas
{

String Layout::blobKey(const BlobRef & ref) const
{
    const String hex = blobHexOf(ref);
    return prefix + "/blobs/" + String(blobHashAlgoName(ref.algo)) + "/" + hex.substr(0, 2) + "/" + hex;
}

}
