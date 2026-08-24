namespace SpillFlow;

internal static class PartitionRefiner
{
    public static async ValueTask<IReadOnlyList<string>> RefineAsync<TSource, TKey>(
        string path,
        SpillSession session,
        Func<TSource, TKey> keySelector,
        IEqualityComparer<TKey> comparer,
        string operation,
        CancellationToken cancellationToken,
        int depth = 0,
        string extension = ".spill")
        where TKey : notnull
    {
        if (session.CanPartitionFit(path))
            return [path];

        if (depth >= session.Options.MaxPartitionDepth)
        {
            throw new SpillResourceLimitException(
                $"{operation} cannot split partition '{Path.GetFileName(path)}' enough to fit the memory lease " +
                $"after {depth} refinement levels. Increase MemoryLimitBytes or investigate a hot key.");
        }

        await using var writer = new PartitionWriter<TSource>(session, session.Options.RecursivePartitionCount, extension);
        await foreach (var item in RecordFile.ReadAsync<TSource>(path, session, cancellationToken).ConfigureAwait(false))
        {
            var key = keySelector(item);
            var partition = StableHash.Partition(key, session.Options.RecursivePartitionCount, comparer, depth + 1);
            await writer.WriteAsync(partition, item, cancellationToken).ConfigureAwait(false);
        }
        await writer.DisposeAsync().ConfigureAwait(false);
        session.DeleteFile(path);

        var result = new List<string>();
        foreach (var child in writer.Paths)
        {
            if (!File.Exists(child))
                continue;
            var leaves = await RefineAsync<TSource, TKey>(child, session, keySelector, comparer, operation,
                cancellationToken, depth + 1, extension).ConfigureAwait(false);
            result.AddRange(leaves);
        }
        return result;
    }
}
