#!/bin/bash
PACKAGES="master worker"

# Create a test binary for each package which will be used to run each test individually.
for pkg in ${PACKAGES}; do
    cd ${pkg}
    go test -c -o tests
    cd ..
done

# Run each test individually, printint "." for successful tests, or the test name for
# failing tests.
for pkg in ${PACKAGES}; do
    cd ${pkg}
    for test in $(go test -list . | grep -E "^(Test|Example)"); do
        (./tests -test.run "^$test\$" &>/dev/null && echo "\n.") || echo -e "\n$test failed";
    done
    cd ..
done

# Cleanup tests binary
for pkg in ${PACKAGES}; do
    cd ${pkg}
    rm tests
    cd ..
done