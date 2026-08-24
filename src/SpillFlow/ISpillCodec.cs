namespace SpillFlow;

/// <summary>Encodes values into deterministic spill-record bytes and decodes them again.</summary>
/// <typeparam name="T">The value type handled by the codec.</typeparam>
public interface ISpillCodec<T>
{
    /// <summary>Encodes a value.</summary>
    byte[] Encode(T value);
    /// <summary>Decodes one complete encoded value.</summary>
    T Decode(ReadOnlySpan<byte> bytes);
    /// <summary>Returns the exact encoded byte count for a value.</summary>
    int GetEncodedSize(T value) => Encode(value).Length;
}

/// <summary>Adapts encode and decode delegates to <see cref="ISpillCodec{T}"/>.</summary>
/// <typeparam name="T">The value type handled by the delegates.</typeparam>
/// <param name="encode">Encodes a value.</param>
/// <param name="decode">Decodes a byte array.</param>
public sealed class DelegateSpillCodec<T>(Func<T, byte[]> encode, Func<byte[], T> decode) : ISpillCodec<T>
{
    /// <inheritdoc />
    public byte[] Encode(T value) => encode(value);
    /// <inheritdoc />
    public T Decode(ReadOnlySpan<byte> bytes) => decode(bytes.ToArray());
}
