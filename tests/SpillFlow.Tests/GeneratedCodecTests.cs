namespace SpillFlow.Tests;

public sealed class GeneratedCodecTests
{
    [Fact]
    public void Generated_codec_round_trips_supported_fields()
    {
        var codec = new GeneratedBankRecordSpillCodec();
        var expected = new GeneratedBankRecord
        {
            Id = 42,
            Balance = 9_876_543_210,
            Active = true,
            CorrelationId = Guid.Parse("d85b1407-351d-4694-9392-03acc5870eb1"),
            Name = "Miami checking",
            Note = null
        };

        var bytes = codec.Encode(expected);
        var actual = codec.Decode(bytes);

        Assert.Equal(codec.GetEncodedSize(expected), bytes.Length);
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Balance, actual.Balance);
        Assert.Equal(expected.Active, actual.Active);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.Name, actual.Name);
        Assert.Null(actual.Note);
    }

    [Fact]
    public void Generated_codec_rejects_truncated_data()
    {
        var codec = new GeneratedBankRecordSpillCodec();
        var bytes = codec.Encode(new GeneratedBankRecord { Name = "test" });

        Assert.Throws<InvalidDataException>(() => codec.Decode(bytes.AsSpan(0, bytes.Length - 1)));
    }

    [Fact]
    public void Generated_codec_rejects_null_for_non_nullable_string()
    {
        var codec = new GeneratedBankRecordSpillCodec();
        var value = new GeneratedBankRecord { Name = null! };

        Assert.Throws<InvalidDataException>(() => codec.Encode(value));
    }
}

[GenerateSpillCodec]
public sealed class GeneratedBankRecord
{
    public int Id { get; set; }
    public long Balance { get; set; }
    public bool Active { get; set; }
    public Guid CorrelationId { get; set; }
    public string Name { get; set; } = "";
    public string? Note { get; set; }
}
