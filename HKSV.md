# Home Assistant image with native HKSV (experimental)

This image layers the modified HomeKit integration, stream worker and HAP-python session fork on Home Assistant **2026.10.3**. Build it from a Home Assistant checkout based on the `2026.10.3` tag with the HKSV integration changes applied. Do not copy the integration from a different Core release without porting and testing it first. The HomeKit manifest must require `HAP-python==5.0.0.post1` so Home Assistant will not replace the fork with the upstream 5.0.0 package at startup.

Prerequisites: Docker BuildKit, `uv`, the Home Assistant translation development dependencies, a checked-out HAP-python fork at the exact reviewed commit, and the modified stable Home Assistant checkout. Build the wheel into a fresh temporary directory; use a local named build context so no virtual environment, Home Assistant configuration, or secrets are sent as part of the build.

```sh
HAP_FORK=/path/to/HAP-python
python3 -m script.translations develop --integration homekit
WHEEL_DIR=$(mktemp -d)
uv build --wheel --out-dir "$WHEEL_DIR" "$HAP_FORK"
docker buildx build --load \
  --build-context "hksv_wheel=$WHEEL_DIR" \
  -f Dockerfile.hksv -t home-assistant-hksv:2026.10.3 .
rm -rf "$WHEEL_DIR"
```

Use the recorded Git commits for both checkouts and record the resulting image ID. The base image digest pins the production 2026.10.3 image inspected for amd64. The Dockerfile-specific ignore file limits the main build context to the HomeKit integration and the patched stream worker. Review the wheel directory before building: the command passes every file in it to BuildKit.

Before deployment, run the integration tests and verify inside the built image that Home Assistant imports the modified HomeKit integration and HAP-python reports `5.0.0.post1`. Preserve the existing configuration and have a rollback image available. An image build or passing automated tests alone does not demonstrate Apple Home recording, notifications, iCloud recording, or interoperability with a real camera and Home hub; these require separate end-to-end tests.

For each camera, Secure Video offers a 4- or 8-second prebuffer, defaulting to 4 seconds. The buffer is maintained while recording is active and remains subject to its memory limit. Audio transcoding normalizes overlapping source timestamps before AAC encoding to prevent invalid mux timestamps; verify audio/video synchronization with the actual camera after deployment.
