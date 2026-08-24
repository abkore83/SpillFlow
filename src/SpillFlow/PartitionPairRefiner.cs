namespace SpillFlow;

internal sealed record PartitionPair(string? Left, string? Right);

internal static class PartitionPairRefiner
{
    public static async ValueTask<IReadOnlyList<PartitionPair>> RefineAsync<TLeft, TRight, TKey>(
        string? leftPath,
        string? rightPath,
        SpillSession session,
        Func<TLeft, TKey> leftKeySelector,
        Func<TRight, TKey> rightKeySelector,
        IEqualityComparer<TKey> comparer,
        string operation,
        bool materializeLeft,
        CancellationToken cancellationToken,
        bool allowOversizedLeaf = false,
        int depth = 0,
        string leftExtension = ".spill",
        string rightExtension = ".spill")
        where TKey : notnull
    {
        var checkedPaths = materializeLeft ? new[] { leftPath, rightPath } : new[] { rightPath };
        if (session.CanPartitionsFit(checkedPaths))
            return [new PartitionPair(Existing(leftPath), Existing(rightPath))];

        var originalRightLength = rightPath is not null && File.Exists(rightPath)
            ? new FileInfo(rightPath).Length
            : 0;

        if (depth >= session.Options.MaxPartitionDepth)
        {
            if (allowOversizedLeaf)
                return [new PartitionPair(Existing(leftPath), Existing(rightPath))];
            throw new SpillResourceLimitException(
                $"{operation} cannot split paired partitions enough to fit the memory lease after {depth} " +
                "refinement levels. Increase MemoryLimitBytes or investigate a hot key.");
        }

        var count = session.Options.RecursivePartitionCount;
        await using var leftWriter = new PartitionWriter<TLeft>(session, count, leftExtension);
        await using var rightWriter = new PartitionWriter<TRight>(session, count, rightExtension);

        if (leftPath is not null && File.Exists(leftPath))
        {
            await foreach (var item in RecordFile.ReadAsync<TLeft>(leftPath, session, cancellationToken).ConfigureAwait(false))
            {
                var partition = StableHash.Partition(leftKeySelector(item), count, comparer, depth + 1);
                await leftWriter.WriteAsync(partition, item, cancellationToken).ConfigureAwait(false);
            }
        }
        if (rightPath is not null && File.Exists(rightPath))
        {
            await foreach (var item in RecordFile.ReadAsync<TRight>(rightPath, session, cancellationToken).ConfigureAwait(false))
            {
                var partition = StableHash.Partition(rightKeySelector(item), count, comparer, depth + 1);
                await rightWriter.WriteAsync(partition, item, cancellationToken).ConfigureAwait(false);
            }
        }
        await leftWriter.DisposeAsync().ConfigureAwait(false);
        await rightWriter.DisposeAsync().ConfigureAwait(false);
        Delete(leftPath, session);
        Delete(rightPath, session);

        var result = new List<PartitionPair>();
        for (var i = 0; i < count; i++)
        {
            var left = Existing(leftWriter.Paths[i]);
            var right = Existing(rightWriter.Paths[i]);
            if (left is null && right is null)
                continue;
            if (allowOversizedLeaf && right is not null && originalRightLength > 0 &&
                !session.CanPartitionFit(right) && new FileInfo(right).Length >= originalRightLength)
            {
                result.Add(new PartitionPair(left, right));
                continue;
            }
            var leaves = await RefineAsync<TLeft, TRight, TKey>(left, right, session, leftKeySelector,
                rightKeySelector, comparer, operation, materializeLeft, cancellationToken, allowOversizedLeaf,
                depth + 1, leftExtension, rightExtension)
                .ConfigureAwait(false);
            result.AddRange(leaves);
        }
        return result;
    }

    private static string? Existing(string? path) => path is not null && File.Exists(path) ? path : null;

    private static void Delete(string? path, SpillSession session)
    {
        if (path is not null && File.Exists(path))
            session.DeleteFile(path);
    }
}
