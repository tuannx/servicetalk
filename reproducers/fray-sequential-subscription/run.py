#!/usr/bin/env python3
# Copyright © 2026 Apple Inc. and the ServiceTalk project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Run a pinned production source with Fray; preserve replay and native results."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

PROJECT = Path(__file__).resolve().parent
ROOT = PROJECT.parent.parent
SOURCE = 'servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java'
TEST = 'io/servicetalk/concurrent/api/SequentialSubscriptionTest.java'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('revision', help='Full 40-character production Git commit')
    parser.add_argument('label', help='Unique output label')
    parser.add_argument('--replay', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch('[0-9a-f]{40}', args.revision):
        parser.error('revision must be a full commit SHA')
    if not re.fullmatch('[a-z0-9-]+', args.label):
        parser.error('label must contain lowercase letters, digits, or hyphens')
    out = PROJECT / 'build/runs' / args.label
    out.mkdir(parents=True, exist_ok=False)
    source = subprocess.check_output(['git', 'show', args.revision + ':' + SOURCE], cwd=ROOT)
    subject = PROJECT / 'build/subject/io/servicetalk/concurrent/api/SequentialSubscription.java'
    subject.parent.mkdir(parents=True, exist_ok=True)
    subject.write_bytes(source)
    template = (PROJECT / 'src/test/java' / TEST).read_text()
    generated = template
    if args.replay:
        generated = template.replace('@FrayTest(iterations = 10000)',
                                     '@FrayTest(iterations = 1, replay = "evidence/recording")')
        assert generated != template
    test = PROJECT / 'build/generated/src/test/java' / TEST
    test.parent.mkdir(parents=True, exist_ok=True)
    test.write_text(generated)
    report = PROJECT / 'build/fray/fray-report'
    if report.exists():
        shutil.rmtree(report)
    xml_dir = PROJECT / 'build/test-results/frayTest'
    if xml_dir.exists():
        shutil.rmtree(xml_dir)
    command = [str(ROOT / 'gradlew'), '-p', str(PROJECT), 'frayTest', 'writeRuntimeClasspath',
               '--console=plain', '--no-build-cache', '--continue']
    started = time.monotonic()
    with (out / 'gradle.log').open('w') as log:
        process = subprocess.run(command, cwd=PROJECT, stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - started
    log = (out / 'gradle.log').read_text()
    if report.exists():
        shutil.copytree(report, out / 'fray-report')
    if xml_dir.exists():
        shutil.copytree(xml_dir, out / 'junit')
    failures = []
    junit = {'tests': 0, 'failures': 0, 'errors': 0, 'skipped': 0}
    for xml in xml_dir.glob('TEST-*.xml'):
        suite = ET.parse(xml).getroot()
        for key in junit:
            junit[key] += int(suite.get(key, '0'))
        failures.extend(f.get('message') for f in suite.findall('.//failure'))
    progress = [int(n) for n in re.findall(r'Iterations: (\d+)', log)]
    finished = re.search(r'Run finished. Total iter: (\d+)', log)
    error_iteration = re.search(r'Error found at iter: (\d+)', log)
    java = Path(os.environ['JAVA_HOME']) / 'bin/java'
    version = subprocess.run([str(java), '-version'], capture_output=True, text=True, check=True)
    replay_files = {p.name: digest(p.read_bytes()) for p in sorted((PROJECT / 'evidence/recording').iterdir())}
    result = {'revision': args.revision, 'subjectSha256': digest(source),
              'testSha256': digest(generated.encode()), 'frayVersion': '0.9.0',
              'javaVersion': version.stderr.strip(), 'mode': 'replay' if args.replay else 'explore',
              'requestedIterations': 1 if args.replay else 10000,
              'reportedTotalIter': int(finished.group(1)) if finished else None,
              'lastProgressIteration': max(progress) if progress else None,
              'errorIteration': int(error_iteration.group(1)) if error_iteration else None,
              'exitCode': process.returncode, 'junit': junit, 'failures': failures,
              'replayInputSha256': replay_files if args.replay else None,
              'wallSeconds': elapsed}
    (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    normalized = {k: v for k, v in result.items() if k != 'wallSeconds'}
    (out / 'normalized.sha256').write_text(digest(json.dumps(normalized, sort_keys=True).encode()) + '\n')
    print(json.dumps(result, indent=2))
    return process.returncode


if __name__ == '__main__':
    raise SystemExit(main())
