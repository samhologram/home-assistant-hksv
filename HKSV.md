# Native HomeKit Secure Video

This image layers the modified HomeKit integration, stream worker and HAP-python session fork on Home Assistant **2026.10.3**. Build it from a Home Assistant checkout based on the `2026.10.3` tag with the HKSV integration changes applied. Do not copy the integration from a different Core release without porting and testing it first. The HomeKit manifest must require `HAP-python==5.0.0.post1` so Home Assistant will not replace the fork with the upstream 5.0.0 package at startup.

Prerequisites: Docker BuildKit, `uv`, the Home Assistant translation development dependencies, a checked-out HAP-python fork at the exact reviewed commit, and the modified stable Home Assistant checkout. The HAP dependency is [samhologram/HAP-python-hksv](https://github.com/samhologram/HAP-python-hksv) at commit `e6bd1a45c10f2b6710d3e0fa2e8b5e27b5ddd6c6` (`5.0.0.post1`); upstream HAP-python 5.0.0 does not contain these Secure Video services and data-stream changes. Build the wheel into a fresh temporary directory; use a local named build context so no virtual environment, Home Assistant configuration, or secrets are sent as part of the build.

```sh
HAP_FORK=/path/to/HAP-python
test "$(git -C "$HAP_FORK" rev-parse HEAD)" = e6bd1a45c10f2b6710d3e0fa2e8b5e27b5ddd6c6
python3 -m script.translations develop --integration homekit
WHEEL_DIR=$(mktemp -d)
uv build --wheel --out-dir "$WHEEL_DIR" "$HAP_FORK"
docker buildx build --load \
  --build-context "hksv_wheel=$WHEEL_DIR" \
  -f Dockerfile.hksv -t home-assistant-hksv:2026.10.3 .
rm -rf "$WHEEL_DIR"
```

Use the recorded Git commits for both checkouts and record the resulting image ID. The base image digest pins the production 2026.10.3 image inspected for amd64. The Dockerfile-specific ignore file limits the main build context to the HomeKit integration and the patched stream package's `__init__.py`, `core.py`, and `worker.py`. Review the wheel directory before building: the command passes every file in it to BuildKit.

Enable Secure Video in the HomeKit integration options for each camera and select a linked motion sensor that reports real events. In Apple Home, enable streaming and recording for the relevant home/away modes. Enabling the Home Assistant option exposes recording support; Apple Home controls recording permission and clip retention.

Before deployment, run the integration tests and verify inside the built image that Home Assistant imports the modified HomeKit integration and HAP-python reports `5.0.0.post1`. Preserve the existing configuration and have a rollback image available. An image build or passing automated tests alone does not demonstrate Apple Home recording, notifications, iCloud recording, or interoperability with a real camera and Home hub; these require separate end-to-end tests.

For each camera, Secure Video offers a 4- or 8-second prebuffer, defaulting to 4 seconds. The buffer is maintained while recording is active and remains subject to its memory limit. Audio transcoding normalizes overlapping source timestamps before AAC encoding to prevent invalid mux timestamps; verify audio/video synchronization with the actual camera after deployment.

HomeKit live video, Secure Video recording, and HomeKit snapshots share the camera entity's Home Assistant stream. Each consumer holds its own reference; stopping a recording or live session releases that reference without stopping the others. Live output reads fragmented MP4 parts from the shared worker and sends them to FFmpeg over a local pipe. Snapshots decode a fresh keyframe from that same worker and fail if a fresh image cannot be decoded. Per-slot and camera privacy settings continue to apply.

The shared provider requests AAC audio at startup so a later audio consumer does not restart the camera connection; sources with unsupported audio such as PCM A-law incur an AAC conversion even when the current consumer is silent. HomeKit live advertises Opus, so even a configured audio copy is encoded to Opus when the shared stream carries AAC or MP3. An explicit HomeKit `stream_source` override cannot be combined with this shared-source path and is rejected; configure the camera entity's stream source instead. Other integrations that open their own camera connections are outside this HomeKit sharing guarantee. Hardware encoding, camera-specific frame compatibility, snapshots and real Home hub behavior require separate validation on the target system.

The image build checks that `Stream.async_get_image` exposes `use_existing_stream` and `require_fresh`, that `KeyFrameConverter.async_get_image` exposes `require_fresh`, and that `Stream.is_running` is available. Repeat those signature checks in the built image before deployment; they guard against accidentally building with only the worker patch.
