# Personal HKSV fork maintenance

This is a personal-fork operations guide. Keep it and the added personal
`AGENTS.md` section out of contributions to the original projects.

## Sources and branches

- Home Assistant: `origin` = `samhologram/home-assistant-hksv`, `upstream` = `home-assistant/core`.
- HAP-python: `origin` = `samhologram/HAP-python-hksv`, `upstream` = `ikalchev/HAP-python`.
- The tested HA branch is `feature/homekit-secure-video`; HAP's downstream branch is `feature/hksv-session`.
- Both projects use `feature/homekit-secure-video-upstream` for their separate contribution branches. Do not deploy them just because a PR exists.
- This guide lives on the personal HA branch `maintenance/hksv`; never include its documentation commit in an upstream feature branch.
- Linked drafts: [Home Assistant #190234](https://github.com/home-assistant/core/pull/190234) and [HAP-python #497](https://github.com/ikalchev/HAP-python/pull/497).

## Updating while retaining HKSV

For this HKSV work, the owner has requested that each pushed PR code fix also
be ported, validated and deployed on the maintained instance, with backup and
rollback. Make the corrected stable version available promptly for owner
testing, then finish broader regression checks. This standing instruction does
not authorize unrelated upgrades or package publication.

1. Read the personal Nodum Homelab notes and verify the actual running host, image, source refs and configuration. vmp4 is a coordination/documentation host; do not assume it runs HA.
2. Require a clean checkout. Inspect `git remote -v`, fetch `origin` and `upstream`, and review incoming changes. A plain `git pull` follows the configured personal fork branch, not necessarily original HA.
3. Create an isolated worktree from the appropriate maintained HKSV branch. Merge the selected upstream stable tag for production, or upstream `dev` for contribution work. Keep published PR history: add commits or merge upstream instead of rewriting it.
4. Resolve conflicts by preserving both updated upstream APIs and HKSV. Never select an entire file with `ours`/`theirs` blindly. Verify shared-source leases, native H.264 copy and pacing, independent audio fallback, timestamp handling, prebuffering and privacy controls remain present.
5. Update HAP-python separately. Preserve the authenticated connection context, HomeKit Data Stream and Secure Video APIs. A newer official version is not proof that these APIs are included.
6. Keep HA's dependency declaration and the built HAP wheel compatible. The downstream `.post1` pin is a local-wheel identity, not a published dependency. Do not replace it with an incompatible official package or automatically publish a release to PyPI. Upstream HA must eventually depend on a compatible published HAP release.
7. Port the build recipe to the selected HA release; never copy the integration wholesale into another release. Keep recipes, manifests and rollback material outside upstream source changes.
8. Run relevant HAP and HA tests, types/lints and dependency checks in an isolated environment. Never mount production authentication storage into test containers. Validate actual Apple Home live video, recording/playback, audio and resource use before calling a deployment successful.
9. Before deployment, verify a coherent configuration backup and preserve every current HomeKit pairing and key. Keep the previous image and a tested rollback procedure. Do not export sensitive backups to NAS/R2 without exact destination authorization.
10. Deploy only after validation; check health, logs, camera connection counts, codec choices and HomeKit behavior. Roll back on regression and record dated evidence in Nodum without secrets.

Do not add Tapo-specific patches, enable fork CI/CD, publish packages, open new
upstream PRs, or deploy as an incidental part of an update without authorization.
