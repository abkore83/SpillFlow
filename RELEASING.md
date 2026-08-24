# Release Process

1. Choose a Semantic Versioning-compatible prerelease version.
2. Update both project versions and `CHANGELOG.md`.
3. Run the Release build and complete test suite.
4. Pack `SpillFlow`; it embeds the codec generator as an analyzer.
5. Install the local package into a clean sample and verify generated codecs.
6. Inspect package contents, dependency versions, README, license, and symbols.
7. Create a signed repository tag matching the package version.
8. Publish from CI using a scoped NuGet API key and trusted publishing when available.

Do not publish `1.0.0` until the remaining production limitations in
`DRAWBACKS.md` are resolved or explicitly accepted as stable API behavior.
