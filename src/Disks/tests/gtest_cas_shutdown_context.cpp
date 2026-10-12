#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <memory>

using namespace DB::Cas;

namespace DB::ContentAddressedSetting
{
extern const ContentAddressedSettingsBool gc_enabled;
}

namespace
{

/// Reuse the single `ContextSharedPart` owned by the global gtest environment, exactly as the
/// interpreter tests do. Each returned `Context` is independently owned, so a test can release or
/// reset its copy without disturbing the process-global context or the other tests.
DB::ContextMutablePtr makeTestContext()
{
    return DB::Context::createCopy(getContext().context);
}

std::shared_ptr<DB::ContentAddressedMetadataStorage> openTestStorage(
    const DB::ContextPtr & context = {}, bool startup = true)
{
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "cas_shutdown_context_scratch");
    /// Keeping the GC scheduler off avoids unrelated worker activity.
    settings[DB::ContentAddressedSetting::gc_enabled] = false;
    settings.validate();

    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", context, settings);
    if (startup)
        storage->startup();
    return storage;
}

}

/// The storage must no longer keep the context alive. This is the property `Server.cpp` relies on when
/// it destroys the context explicitly.
TEST(CASShutdownContext, StorageDoesNotExtendContextLifetime)
{
    auto context = makeTestContext();
    std::weak_ptr<const DB::Context> weak_context = context;
    auto storage = openTestStorage(context);

    context.reset();

    EXPECT_EQ(weak_context.use_count(), 0L);
}

/// An expired reference supplied at `startup` is an error, not the disabled path.
TEST(CASShutdownContext, ExpiredContextAtStartupFails)
{
    auto context = makeTestContext();
    auto storage = openTestStorage(context, /*startup=*/false);
    context.reset();

    EXPECT_ANY_THROW(storage->startup());
}
