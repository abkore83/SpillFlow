using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace SpillFlow.Tests;

public sealed class SpillExtensionsTests
{
    private static async IAsyncEnumerable<T> AsAsync<T>(IEnumerable<T> values,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var value in values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
            await Task.Yield();
        }
    }

    [Fact]
    public async Task DistinctSpill_matches_distinct_by_key_and_uses_disk()
    {
        await using var session = TinySession();
        var input = Enumerable.Range(0, 20_000).Select(i => new Row(i % 1_000, new string('x', 100))).ToArray();
        var result = await CollectAsync(AsAsync(input).DistinctSpill(session, x => x.Id));
        Assert.Equal(1_000, result.Count);
        Assert.Equal(1_000, result.Select(x => x.Id).Distinct().Count());
        Assert.True(session.Statistics.UsedDisk);
    }

    [Fact]
    public async Task ExceptBySpill_matches_linq_semantics()
    {
        await using var session = TinySession();
        var source = Enumerable.Range(0, 2_000).Select(i => new Row(i, "value"));
        var excluded = Enumerable.Range(0, 1_000).Where(i => i % 2 == 0);
        var result = await CollectAsync(AsAsync(source).ExceptBySpill(AsAsync(excluded), session, x => x.Id));
        Assert.Equal(1_500, result.Count);
        Assert.DoesNotContain(result, x => x.Id < 1_000 && x.Id % 2 == 0);
    }

    [Fact]
    public async Task ExceptBySpill_removes_duplicate_source_keys()
    {
        await using var session = TinySession();
        var source = new[] { new Row(1, "first"), new Row(1, "second"), new Row(2, "third") };
        var result = await CollectAsync(AsAsync(source).ExceptBySpill(AsAsync(Array.Empty<int>()), session, x => x.Id));
        Assert.Equal(new[] { 1, 2 }, result.Select(x => x.Id).Order());
    }

    [Fact]
    public async Task ExceptBySpill_uses_custom_comparer_for_partition_routing()
    {
        await using var session = TinySession();
        var result = await CollectAsync(AsAsync(new[] { "ACCOUNT-123", "ACCOUNT-456" }).ExceptBySpill(
            AsAsync(new[] { "account-123" }), session, value => value, StringComparer.OrdinalIgnoreCase));
        Assert.Equal(new[] { "ACCOUNT-456" }, result);
    }

    [Fact]
    public async Task GroupJoinSpill_uses_custom_comparer_for_partition_routing()
    {
        await using var session = TinySession();
        var result = await CollectAsync(AsAsync(new[] { "CUSTOMER-A" }).GroupJoinSpill(
            AsAsync(new[] { "customer-a" }), session, value => value, value => value,
            (key, matches) => (key, Count: matches.Count), StringComparer.OrdinalIgnoreCase));
        Assert.Equal(1, result.Single().Count);
    }

    [Fact]
    public async Task GroupJoinSpill_rejects_partition_that_cannot_fit_memory_budget()
    {
        await using var session = TinySession().RegisterCodec(new RowCodec());
        var left = new[] { new Row(7, "left") };
        var hotPartition = Enumerable.Range(0, 10_000).Select(_ => new Row(7, "right"));
        var operation = AsAsync(left).GroupJoinSpill(AsAsync(hotPartition), session,
            row => row.Id, row => row.Id, (row, matches) => matches.Count);
        var error = await Assert.ThrowsAsync<SpillResourceLimitException>(() => CollectAsync(operation));
        Assert.Contains("memory lease", error.Message);
    }

    [Fact]
    public async Task DistinctSpill_recursively_refines_oversized_partitions()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 64 * 1024,
            PartitionCount = 2,
            RecursivePartitionCount = 16,
            MaxPartitionDepth = 3
        });
        var input = Enumerable.Range(0, 20_000).ToArray();
        var result = await CollectAsync(AsAsync(input).DistinctSpill(session, value => value));
        Assert.Equal(input.Length, result.Count);
        Assert.Equal(input.Length, result.Distinct().Count());
        Assert.True(session.Statistics.SpillFiles > session.Options.PartitionCount);
        Assert.True(session.Statistics.PeakLeasedBytes <= session.Options.MemoryLimitBytes * 0.75);
    }

    [Fact]
    public async Task ExceptBySpill_recursively_refines_both_sides_together()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 64 * 1024,
            PartitionCount = 2,
            RecursivePartitionCount = 16,
            MaxPartitionDepth = 3
        });
        var source = Enumerable.Range(0, 20_000).ToArray();
        var excluded = Enumerable.Range(0, 20_000).Where(value => value % 3 == 0).ToArray();
        var result = await CollectAsync(AsAsync(source).ExceptBySpill(AsAsync(excluded), session, value => value));
        Assert.Equal(source.Where(value => value % 3 != 0), result.Order());
        Assert.True(session.Statistics.SpillFiles > session.Options.PartitionCount * 2);
        Assert.True(session.Statistics.PeakLeasedBytes <= session.Options.MemoryLimitBytes * 0.75);
    }

    [Fact]
    public async Task OrderBySpill_returns_globally_sorted_records()
    {
        await using var session = TinySession();
        var random = new Random(42);
        var input = Enumerable.Range(0, 10_000).Select(i => new Row(random.Next(), new string('y', 32))).ToArray();
        var result = await CollectAsync(AsAsync(input).OrderBySpill(session, x => x.Id));
        Assert.Equal(input.Select(x => x.Id).Order().ToArray(), result.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task OrderBySpill_uses_multi_pass_merge_when_fan_in_is_small()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 64 * 1024,
            MergeFanIn = 2,
            PartitionCount = 16
        });
        var input = Enumerable.Range(0, 20_000).Reverse()
            .Select(i => $"{i:D8}-{new string('x', 40)}").ToArray();
        var result = await CollectAsync(AsAsync(input).OrderBySpill(session, value => value));
        Assert.Equal(input.Order().ToArray(), result);
        Assert.True(session.Statistics.SpillFiles > 10);
        Assert.True(session.Statistics.PeakOpenReaders <= session.Options.MergeFanIn);
    }

    [Fact]
    public async Task GroupJoinSpill_returns_matches_and_empty_groups()
    {
        await using var session = TinySession();
        var left = new[] { new Row(1, "a"), new Row(2, "b"), new Row(3, "c") };
        var right = new[] { new Row(1, "x"), new Row(1, "y"), new Row(3, "z") };
        var result = await CollectAsync(AsAsync(left).GroupJoinSpill(AsAsync(right), session, x => x.Id, x => x.Id,
            (item, matches) => (item.Id, Count: matches.Count)));
        Assert.Equal(new[] { (1, 2), (2, 0), (3, 1) }, result.OrderBy(x => x.Id));
    }

    [Fact]
    public async Task Session_removes_spill_files_when_disposed()
    {
        var session = TinySession();
        var directory = session.WorkingDirectory;
        _ = await CollectAsync(AsAsync(Enumerable.Range(0, 10_000)).OrderBySpill(session, x => -x));
        await session.DisposeAsync();
        Assert.False(Directory.Exists(directory));
    }

    [Fact]
    public async Task Disk_quota_fails_before_unbounded_spill_growth()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 64 * 1024,
            MaxDiskUsageBytes = 16 * 1024,
            PartitionCount = 8
        });
        var operation = AsAsync(Enumerable.Range(0, 20_000)).OrderBySpill(session, value => -value);
        var error = await Assert.ThrowsAsync<SpillResourceLimitException>(() => CollectAsync(operation));
        Assert.Contains("disk quota exceeded", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(session.Statistics.PeakDiskBytes <= session.Options.MaxDiskUsageBytes);
    }

    [Fact]
    public async Task Session_startup_removes_abandoned_spill_directories()
    {
        var root = Path.Combine(Path.GetTempPath(), $"spillflow-cleanup-test-{Guid.NewGuid():N}");
        var stale = Path.Combine(root, "spillflow-00000000000000000000000000000000");
        Directory.CreateDirectory(stale);
        await File.WriteAllTextAsync(Path.Combine(stale, "orphan.spill"), "orphan");
        Directory.SetLastWriteTimeUtc(stale, DateTime.UtcNow - TimeSpan.FromHours(2));
        try
        {
            await using var session = new SpillSession(new SpillOptions
            {
                TemporaryDirectory = root,
                AbandonedSessionAge = TimeSpan.FromHours(1)
            });
            Assert.False(Directory.Exists(stale));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Encrypted_blocks_round_trip_and_reject_tampering()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 1024 * 1024,
            EncryptionKey = Enumerable.Range(0, 32).Select(value => (byte)value).ToArray()
        });
        var path = session.NewFile();
        await using (var writer = new PooledRecordWriter<int>(path, session))
            for (var value = 0; value < 1_000; value++)
                await writer.WriteAsync(value, CancellationToken.None);

        Assert.Equal(Enumerable.Range(0, 1_000), await CollectAsync(RecordFile.ReadAsync<int>(path, session)));
        var bytes = await File.ReadAllBytesAsync(path);
        bytes[^1] ^= 0x5A;
        await File.WriteAllBytesAsync(path, bytes);
        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(
            () => CollectAsync(RecordFile.ReadAsync<int>(path, session)));
    }

    [Fact]
    public async Task Plaintext_blocks_reject_checksum_tampering()
    {
        await using var session = new SpillSession(new SpillOptions { MemoryLimitBytes = 1024 * 1024 });
        var path = session.NewFile();
        await using (var writer = new PooledRecordWriter<int>(path, session))
            for (var value = 0; value < 100; value++)
                await writer.WriteAsync(value, CancellationToken.None);

        var bytes = await File.ReadAllBytesAsync(path);
        bytes[^1] ^= 0x01;
        await File.WriteAllBytesAsync(path, bytes);
        var error = await Assert.ThrowsAsync<InvalidDataException>(
            () => CollectAsync(RecordFile.ReadAsync<int>(path, session)));
        Assert.Contains("checksum", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Registered_codec_is_used_for_spilled_records()
    {
        await using var session = TinySession().RegisterCodec(new RowCodec());
        var input = Enumerable.Range(0, 10_000).Select(i => new Row(i, "ignored")).ToArray();
        var result = await CollectAsync(AsAsync(input).OrderBySpill(session, x => -x.Id));
        Assert.Equal(input.Length, result.Count);
        Assert.Equal(9_999, result[0].Id);
        Assert.All(result, row => Assert.Equal("decoded", row.Value));
    }

    [Fact]
    public async Task Block_rejects_a_different_registered_codec_schema()
    {
        await using var session = TinySession().RegisterCodec(new RowCodec());
        var path = session.NewFile();
        await using (var writer = new PooledRecordWriter<Row>(path, session))
            await writer.WriteAsync(new Row(42, "ignored"), CancellationToken.None);

        session.RegisterCodec(new ChangedRowCodec());
        var error = await Assert.ThrowsAsync<InvalidDataException>(
            () => CollectAsync(RecordFile.ReadAsync<Row>(path, session)));

        Assert.Contains("schema fingerprint", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Strict_codec_mode_rejects_an_unregistered_record_type()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 64 * 1024,
            AllowJsonFallback = false
        });
        var path = session.NewFile();

        var error = Assert.Throws<InvalidOperationException>(() => new PooledRecordWriter<Row>(path, session));

        Assert.Contains("JSON fallback is disabled", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GroupAggregateSpill_computes_count_and_sum_per_key()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 128 * 1024,
            PartitionCount = 32
        });
        var input = Enumerable.Range(0, 20_000)
            .Select(i => new Row(i % 250, (i % 17).ToString())).ToArray();

        var result = await CollectAsync(AsAsync(input).GroupAggregateSpill(
            session,
            keySelector: row => row.Id,
            seedFactory: () => (Count: 0, Sum: 0L),
            accumulate: (state, row) => (state.Count + 1, state.Sum + int.Parse(row.Value)),
            resultSelector: (key, state) => (Key: key, state.Count, state.Sum)));

        var expected = input.GroupBy(row => row.Id)
            .ToDictionary(group => group.Key, group => (group.Count(), group.Sum(row => (long)int.Parse(row.Value))));
        Assert.Equal(expected.Count, result.Count);
        foreach (var item in result)
            Assert.Equal(expected[item.Key], (item.Count, item.Sum));
    }

    [Fact]
    public async Task GroupAggregateSpill_empty_source_returns_no_groups()
    {
        await using var session = TinySession();
        var result = await CollectAsync(AsAsync(Array.Empty<int>()).GroupAggregateSpill(
            session, value => value, () => 0, (count, _) => count + 1, (key, count) => (key, count)));
        Assert.Empty(result);
    }

    [Fact]
    public async Task GroupAggregateSpill_stays_in_memory_when_state_fits_budget()
    {
        await using var session = new SpillSession(new SpillOptions
        {
            MemoryLimitBytes = 1024 * 1024,
            PartitionCount = 16
        });
        var result = await CollectAsync(AsAsync(Enumerable.Range(0, 1_000)).GroupAggregateSpill(
            session, value => value % 10, () => 0, (sum, value) => sum + value, (key, sum) => (key, sum)));
        Assert.Equal(10, result.Count);
        Assert.False(session.Statistics.UsedDisk);
    }

    private static SpillSession TinySession() => new(new SpillOptions
    {
        MemoryLimitBytes = 64 * 1024,
        PartitionCount = 16
    });

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (var item in source)
            result.Add(item);
        return result;
    }

    public sealed record Row(int Id, string Value);

    private sealed class RowCodec : ISpillCodec<Row>
    {
        public string SchemaId => "spillflow-tests:row:id-int32:v1";
        public byte[] Encode(Row value) => BitConverter.GetBytes(value.Id);
        public Row Decode(ReadOnlySpan<byte> bytes) => new(BitConverter.ToInt32(bytes), "decoded");
        public int GetEncodedSize(Row value) => sizeof(int);
    }

    private sealed class ChangedRowCodec : ISpillCodec<Row>
    {
        public string SchemaId => "spillflow-tests:row:id-int64:v2";
        public byte[] Encode(Row value) => BitConverter.GetBytes((long)value.Id);
        public Row Decode(ReadOnlySpan<byte> bytes) => new((int)BitConverter.ToInt64(bytes), "changed");
        public int GetEncodedSize(Row value) => sizeof(long);
    }
}
