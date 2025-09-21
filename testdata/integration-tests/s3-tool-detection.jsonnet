/*
   this flow spec contains tasks that test whether we have
   a working s3 download client in different
   configurations. to use, execute each task using sdflow
   and see if they "PASS"
*/


local PREAMBLE = |||
  filter_path() {
      local exclude="$$1"
      echo $$PATH | tr ':' '\n' | grep -v "$$exclude" | paste -sd':'
  }

  source testdata/minio-test.env
|||;

{
  // No tools available - should use built-in downloader
  'test-no-tools': {
    run: std.join('\n', [
      'echo "=== Testing: No tools available ==="',
      PREAMBLE,
      'export PATH="$(filter_path nix-profile)"',
      'export PATH="$(filter_path awscli2)"',
      'export PATH="$(filter_path minio-client)"',
      'which aws && exit 1 || echo "✓ aws not found (expected)"',
      'which mc && exit 1 || echo "✓ mc not found (expected)"',
      'DEBUG_LEVEL=3 sdflow example-minio-loader-to-stdout 2>&1 | grep -E "built-in s3 downloader" && echo PASS || echo FAIL',
    ]),
  },

  // Decoy tools - should detect fakes and use built-in
  'test-decoy-tools': {
    run: std.join('\n', [
      'echo "=== Testing: Decoy tools (fake aws/mc) ==="',
      PREAMBLE,
      'export PATH="$(filter_path nix-profile)"',
      'export PATH="$(filter_path awscli2)"',
      'export PATH="$(filter_path minio-client)"',
      'export PATH="$$PWD/testdata/fake-awscli2:$$PWD/testdata/fake-minio-client:$$PATH"',
      'aws --version | grep "fake awscli" || echo FAIL',
      'mc --version | grep "fake mc" || echo FAIL',
      'DEBUG_LEVEL=3 sdflow example-minio-loader-to-stdout 2>&1 | grep -E "built-in s3 downloader" && echo PASS || echo FAIL',
    ]),
  },

  // Valid AWS only - should use aws CLI
  'test-valid-aws-only': {
    run: std.join('\n', [
      'echo "=== Testing: Valid AWS only ==="',
      PREAMBLE,
      'export PATH="$(filter_path nix-profile)"',
      'export PATH="$(filter_path minio-client)"',
      'which aws && echo "✓ aws found" || exit 1',
      'aws --version | grep "aws-cli/" || echo FAIL',
      'which mc && exit 1 || echo "✓ mc correctly hidden"',
      'DEBUG_LEVEL=3 sdflow example-minio-loader-to-stdout 2>&1 | grep -E "downloading using aws" && echo PASS || echo FAIL',
    ]),
  },

  // Valid MC only - should use mc
  'test-valid-mc-only': {
    run: std.join('\n', [
      'echo "=== Testing: Valid MC only ==="',
      PREAMBLE,
      'export PATH="$(filter_path nix-profile)"',
      'export PATH="$(filter_path awscli2)"',
      'which mc && echo "✓ mc found" || exit 1',
      'mc --version | grep -i "minio" || echo FAIL',
      'which aws && exit 1 || echo "✓ aws correctly hidden"',
      'DEBUG_LEVEL=3 sdflow example-minio-loader-to-stdout 2>&1 | grep -E "downloading using mc" && echo PASS || echo FAIL',
    ]),
  },

  // Both tools available - AWS should have priority
  'test-both-tools-aws-priority': {
    run: std.join('\n', [
      'echo "=== Testing: Both tools available (AWS priority) ==="',
      PREAMBLE,
      'which aws && echo "✓ aws found" || exit 1',
      'which mc && echo "✓ mc found" || exit 1',
      'aws --version | grep "aws-cli/" || echo FAIL',
      'mc --version | grep -i "minio" || echo FAIL',
      'DEBUG_LEVEL=3 sdflow example-minio-loader-to-stdout 2>&1 | grep -E "downloading using aws" && echo PASS || echo FAIL',
    ]),
  },

  // Decoy AWS + real MC - should use mc
  'test-decoy-aws-real-mc': {
    run: std.join('\n', [
      'echo "=== Testing: Decoy AWS + real MC ==="',
      PREAMBLE,
      'export PATH="$(filter_path nix-profile)"',
      'export PATH="$(filter_path awscli2)"',
      'export PATH="$$PWD/testdata/fake-awscli2:$$PATH"',
      'aws --version | grep "fake awscli" || echo FAIL',
      'mc --version | grep -i "minio" || echo FAIL',
      'DEBUG_LEVEL=3 sdflow example-minio-loader-to-stdout 2>&1 | grep -E "downloading using mc" && echo PASS || echo FAIL',
    ]),
  },
}
