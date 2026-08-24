using System.Security.Cryptography;

namespace SpillFlow;

internal static class SpillBlockFormat
{
    public const uint Magic = 0x424C5053; // SPLB
    public const byte Version = 1;
    public const int HeaderSize = 48;
    public const int NonceSize = 12;
    public const int TagSize = 16;
    public const byte EncryptedFlag = 1;

    public static uint ComputeCrc32(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var value in data)
        {
            crc ^= value;
            for (var bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ (0xEDB88320u & (uint)-(int)(crc & 1));
        }
        return ~crc;
    }

    public static byte[] CreateHeader(int payloadLength, int plainLength, uint checksum,
        ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> tag, bool encrypted)
    {
        var header = new byte[HeaderSize];
        BitConverter.TryWriteBytes(header.AsSpan(0, 4), Magic);
        header[4] = Version;
        header[5] = encrypted ? EncryptedFlag : (byte)0;
        BitConverter.TryWriteBytes(header.AsSpan(8, 4), payloadLength);
        BitConverter.TryWriteBytes(header.AsSpan(12, 4), plainLength);
        BitConverter.TryWriteBytes(header.AsSpan(16, 4), checksum);
        nonce.CopyTo(header.AsSpan(20, NonceSize));
        tag.CopyTo(header.AsSpan(32, TagSize));
        return header;
    }

    public static BlockHeader ParseHeader(ReadOnlySpan<byte> header)
    {
        if (header.Length != HeaderSize || BitConverter.ToUInt32(header[..4]) != Magic)
            throw new InvalidDataException("Invalid SpillFlow block magic.");
        if (header[4] != Version)
            throw new InvalidDataException($"Unsupported SpillFlow block version {header[4]}.");
        var flags = header[5];
        if ((flags & ~EncryptedFlag) != 0)
            throw new InvalidDataException($"Unsupported SpillFlow block flags {flags}.");
        var payloadLength = BitConverter.ToInt32(header.Slice(8, 4));
        var plainLength = BitConverter.ToInt32(header.Slice(12, 4));
        if (payloadLength <= 0 || plainLength <= 0 || payloadLength > 512 * 1024 * 1024 || plainLength > 512 * 1024 * 1024)
            throw new InvalidDataException("Invalid SpillFlow block length.");
        return new BlockHeader(payloadLength, plainLength, BitConverter.ToUInt32(header.Slice(16, 4)),
            flags == EncryptedFlag, header.Slice(20, NonceSize).ToArray(), header.Slice(32, TagSize).ToArray());
    }
}

internal sealed record BlockHeader(int PayloadLength, int PlainLength, uint Checksum,
    bool Encrypted, byte[] Nonce, byte[] Tag);
