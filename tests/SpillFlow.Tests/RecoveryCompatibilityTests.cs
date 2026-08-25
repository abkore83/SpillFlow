using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class RecoveryCompatibilityTests
{
    private const string FixtureVersion = "0.3.0-alpha.8";

    [Fact]
    public async Task Recovers_orderby_fixture_created_by_alpha_8_format()
    {
        var fixture = Path.Combine(AppContext.BaseDirectory, "Fixtures", FixtureVersion, "orderby-output");
        Assert.True(Directory.Exists(fixture), $"Recovery fixture is missing: {fixture}");
        var root = Path.Combine(Path.GetTempPath(), $"spillflow-compat-{Guid.NewGuid():N}");
        var working = Path.Combine(root, "session");
        CopyDirectory(fixture, working);
        try
        {
            await using var session = SpillSession.Recover(working, new SpillOptions
            {
                TemporaryDirectory = root,
                CleanupAbandonedSessions = false,
                ProtectTemporaryDirectory = false,
                MemoryLimitBytes = 64 * 1024,
                MergeFanIn = 2,
                PartitionCount = 8,
                RecursivePartitionCount = 4
            });
            var output = new List<int>();
            await foreach (var value in session.OrderByRecoverableSpill<int, int>(
                               (offset, token) => throw new InvalidOperationException(
                                   "An output-phase compatibility fixture must not reopen its source."),
                               "compat-orderby",
                               value => value,
                               outputCheckpointInterval: 10))
                output.Add(value);

            Assert.Equal(Enumerable.Range(10, 990), output);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public Task Recovers_distinct_fixture_created_by_alpha_8_format() => AssertFixtureRecoversAsync(
        "distinct-output",
        session => session.DistinctRecoverableSpill(
            (offset, token) => AscendingSource(offset, 10_000, token),
            "hard-crash", value => value % 1_000, outputCheckpointInterval: 10),
        session => session.DistinctRecoverableSpill<int, int>(
            UnavailableSource<int>, "hard-crash", value => value % 1_000, outputCheckpointInterval: 10));

    [Fact]
    public Task Recovers_exceptby_fixture_created_by_alpha_8_format() => AssertFixtureRecoversAsync(
        "exceptby-output",
        session => session.ExceptByRecoverableSpill(
            (offset, token) => AscendingSource(offset, 10_000, token),
            ExcludedKeys,
            "hard-crash", value => value % 1_000, outputCheckpointInterval: 10),
        session => session.ExceptByRecoverableSpill<int, int>(
            UnavailableSource<int>, UnavailableSource<int>,
            "hard-crash", value => value % 1_000, outputCheckpointInterval: 10));

    [Fact]
    public Task Recovers_group_aggregate_fixture_created_by_alpha_8_format() => AssertFixtureRecoversAsync(
        "aggregate-output",
        session => session.GroupAggregateRecoverableSpill(
            (offset, token) => AscendingSource(offset, 10_000, token),
            "hard-crash", value => value % 1_000, () => 0L, (sum, value) => sum + value,
            (key, sum) => key * 10_000_000L + sum, outputCheckpointInterval: 10),
        session => session.GroupAggregateRecoverableSpill<int, int, long, long>(
            UnavailableSource<int>, "hard-crash", value => value % 1_000, () => 0L,
            (sum, value) => sum + value, (key, sum) => key * 10_000_000L + sum,
            outputCheckpointInterval: 10));

    [Fact]
    public Task Recovers_group_join_fixture_created_by_alpha_8_format() => AssertFixtureRecoversAsync(
        "groupjoin-output",
        session => session.GroupJoinStreamRecoverableSpill(
            (offset, token) => AscendingSource(offset, 10_000, token),
            ReferenceKeys,
            "hard-crash", left => left % 1_000, right => right, SelectJoinResult,
            outputCheckpointInterval: 10),
        session => session.GroupJoinStreamRecoverableSpill<int, int, int, long>(
            UnavailableSource<int>, UnavailableSource<int>,
            "hard-crash", left => left % 1_000, right => right, SelectJoinResult,
            outputCheckpointInterval: 10));

    private static async Task AssertFixtureRecoversAsync<T>(
        string fixtureName,
        Func<SpillSession, IAsyncEnumerable<T>> currentOperation,
        Func<SpillSession, IAsyncEnumerable<T>> recoveryOperation)
    {
        var fixture = Path.Combine(AppContext.BaseDirectory, "Fixtures", FixtureVersion, fixtureName);
        Assert.True(Directory.Exists(fixture), $"Recovery fixture is missing: {fixture}");
        var root = Path.Combine(Path.GetTempPath(), $"spillflow-compat-{Guid.NewGuid():N}");
        var working = Path.Combine(root, "fixture-session");
        Directory.CreateDirectory(root);
        try
        {
            List<T> expected;
            await using (var current = new SpillSession(Options(root)))
                expected = await CollectAsync(currentOperation(current));

            CopyDirectory(fixture, working);
            await using var recovered = SpillSession.Recover(working, Options(root));
            var actual = await CollectAsync(recoveryOperation(recovered));

            Assert.Equal(expected.Skip(10), actual);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    private static SpillOptions Options(string root) => new()
    {
        TemporaryDirectory = root,
        CleanupAbandonedSessions = false,
        ProtectTemporaryDirectory = false,
        MemoryLimitBytes = 64 * 1024,
        MergeFanIn = 2,
        PartitionCount = 8,
        RecursivePartitionCount = 4
    };

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var values = new List<T>();
        await foreach (var value in source) values.Add(value);
        return values;
    }

    private static async IAsyncEnumerable<int> UnavailableSource<T>(
        long offset,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Yield();
        throw new InvalidOperationException("An output-phase compatibility fixture must not reopen its source.");
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }

    private static async IAsyncEnumerable<int> AscendingSource(
        long offset,
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var position = offset; position < count; position++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return (int)position;
            if ((position & 1_023) == 0) await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> ExcludedKeys(
        long offset,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var position = offset; position < 334; position++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return (int)position * 3;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> ReferenceKeys(
        long offset,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var position = offset; position < 1_000; position++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return (int)position;
            await Task.Yield();
        }
    }

    private static async ValueTask<long> SelectJoinResult(
        int left,
        IAsyncEnumerable<int> matches,
        CancellationToken cancellationToken)
    {
        var count = 0;
        await foreach (var _ in matches.WithCancellation(cancellationToken)) count++;
        return left * 10L + count;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var path in Directory.EnumerateFiles(source))
            File.Copy(path, Path.Combine(destination, Path.GetFileName(path)));
        foreach (var path in Directory.EnumerateDirectories(source))
            CopyDirectory(path, Path.Combine(destination, Path.GetFileName(path)));
    }
}
