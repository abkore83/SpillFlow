namespace SpillFlow;

internal static class StableHash
{
    public static int Partition<T>(T value, int count, IEqualityComparer<T> comparer)
    {
        ArgumentNullException.ThrowIfNull(comparer);
        return (comparer.GetHashCode(value!) & int.MaxValue) % count;
    }

    public static int Partition<T>(T value, int count, IEqualityComparer<T> comparer, int depth)
    {
        ArgumentNullException.ThrowIfNull(comparer);
        var hash = unchecked((uint)comparer.GetHashCode(value!));
        hash ^= unchecked((uint)depth * 0x9E3779B9u);
        hash ^= hash >> 16;
        hash *= 0x7FEB352Du;
        hash ^= hash >> 15;
        return (int)(hash % (uint)count);
    }

    public static int Partition<T>(T value, int count)
    {
        if (value is null)
            return 0;
        if (value is int intValue)
            return (int)((uint)intValue % (uint)count);
        if (value is long longValue)
            return (int)((ulong)longValue % (uint)count);
        if (value is Guid guid)
            return HashBytes(guid.ToByteArray(), count);
        if (value is string text)
            return HashChars(text, count);

        // Equality-compatible within one process; custom keys can provide a stable hash code.
        return (EqualityComparer<T>.Default.GetHashCode(value) & int.MaxValue) % count;
    }

    private static int HashBytes(ReadOnlySpan<byte> bytes, int count)
    {
        uint hash = 2166136261;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= 16777619;
        }
        return (int)(hash % (uint)count);
    }

    private static int HashChars(string value, int count)
    {
        uint hash = 2166136261;
        foreach (var character in value)
        {
            hash ^= character;
            hash *= 16777619;
        }
        return (int)(hash % (uint)count);
    }
}
