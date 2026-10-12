#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <functional>
#include <memory>
#include <string>

namespace Poco { namespace Util { class AbstractConfiguration; } } // NOLINT(cppcoreguidelines-virtual-class-destructor)


namespace DB
{
struct ContentAddressedSettingsImpl;

/// Resolves `{macro}` placeholders in a config value, e.g. `server_root_id`. Kept as a
/// type-erased callback (rather than a `Context`/`Macros` reference) so this header stays free
/// of `Interpreters/Context` — settings loading has no business knowing about the query context.
using MacroExpander = std::function<std::string(const std::string &)>;

#define CONTENT_ADDRESSED_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64)

CONTENT_ADDRESSED_SETTINGS_SUPPORTED_TYPES(ContentAddressedSettings, DECLARE_SETTING_TRAIT)

/// Declarative settings for the `cas` disk metadata storage, mirroring the
/// `FileCacheSettings` pimpl/traits shape (`Core/BaseSettings.h`). Replaces the ~25 inline
/// `config.getX` calls that used to live in `MetadataStorageFactory.cpp`'s
/// `registerContentAddressedMetadataStorage` lambda; that lambda's key names, defaults, and
/// per-key rationale are the authoritative source for `LIST_OF_CONTENT_ADDRESSED_SETTINGS`.
struct ContentAddressedSettings
{
    ContentAddressedSettings();
    ContentAddressedSettings(const ContentAddressedSettings &);
    ~ContentAddressedSettings();

    CONTENT_ADDRESSED_SETTINGS_SUPPORTED_TYPES(ContentAddressedSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// Consumes only `cas_`-prefixed keys under `config_prefix`. Every other key belongs to another
    /// consumer of the shared disk block and is left untouched. The unprefixed spelling of a CAS
    /// setting is temporarily accepted during the migration window. A missing `scratch_path` defaults
    /// to `default_scratch_path`; a relative OVERRIDE is anchored to `scratch_path_anchor_if_relative`
    /// instead (never the process CWD, and never
    /// `default_scratch_path` -- that default is a per-disk subdirectory, e.g.
    /// `<server-data-path>/disks/<name>/cas_scratch/`, and anchoring a relative override to it would
    /// silently nest the override two levels deeper than the server data path the operator meant;
    /// mirrors `FileCacheSettings::loadFromConfig`'s `cache_path_prefix_if_relative` /
    /// `default_cache_path` split). `server_root_id` is passed through `expand_macros` before
    /// `validate` runs. Ends by calling `validate`.
    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const std::string & scratch_path_anchor_if_relative,
        const std::string & default_scratch_path,
        const MacroExpander & expand_macros);

    /// Fail-closed checks: `gc_interval_sec` must be >= 1; `server_root_id` must
    /// be present (an ABSENT key throws a typed `NO_ELEMENTS_IN_CONFIG`, distinct from a
    /// PRESENT-but-invalid value, which throws `BAD_ARGUMENTS`); and the
    /// enum-valued `blob_hash` must parse. The parsed value is cached for the typed accessor below.
    void validate();

    /// Typed accessor for the enum-valued string setting, parsed and cached by `validate`.
    Cas::BlobHashAlgo blobHashAlgo() const;

private:
    /// The parsed value lives inside `impl` (defined in the .cpp).
    std::unique_ptr<ContentAddressedSettingsImpl> impl;
};

}
