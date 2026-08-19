# Releasing

CoActor is published from GitHub Actions after a version tag is pushed.

## Prerequisites

Configure crates.io Trusted Publishing for both crates:

- Repository: `link-duan/coactor`
- Workflow: `.github/workflows/release.yml`
- GitHub environment: `crates-io`
- Provider: GitHub Actions

The first release must be bootstrapped manually before Trusted Publishing can be
configured:

```bash
cargo login
cargo publish --locked -p coactor-macros
cargo publish --locked -p coactor
```

After the bootstrap release, configure the `crates-io` environment in GitHub
repository settings and add a required reviewer if releases should be manually
approved. No long-lived crates.io token is stored in GitHub Actions.

## Release a version

Update the version of both workspace crates and the changelog, then run the
normal verification checks and commit the changes:

```bash
cargo test --locked --workspace --all-targets --all-features
cargo package --locked -p coactor-macros
```

Create a matching annotated tag:

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

The release workflow validates that the tag, `coactor`, and `coactor-macros`
all use the same version. It then:

1. runs formatting, lint, tests, documentation, MSRV, and packaging checks;
2. authenticates to crates.io using GitHub OIDC;
3. publishes `coactor-macros`;
4. waits for its crates.io index entry; and
5. publishes `coactor`.

Do not reuse a published version. If a release fails after
`coactor-macros` is published, fix the workflow or metadata and rerun the
remaining release steps with a new patch version when necessary.
