#!/bin/bash
# Update version number across all SIMP files

if [ $# -ne 1 ]; then
    echo "Usage: $0 <new-version>"
    echo "Example: $0 1.13.0"
    exit 1
fi

NEW_VERSION="$1"

# Validate version format (e.g., 1.12.0)
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version must be in format X.Y.Z (e.g., 1.13.0)"
    exit 1
fi

echo "Updating SIMP version to $NEW_VERSION..."

# Update Makefile
sed -i "s/^VERSION=.*/VERSION=$NEW_VERSION/" Makefile
echo "✓ Updated Makefile"

# Update spec files
for spec in spec/*.spec; do
    sed -i "s/^Version:.*/Version: $NEW_VERSION/" "$spec"
    # Update simp-env dependency version in other specs
    sed -i "s/Requires: simp-env == .*/Requires: simp-env == $NEW_VERSION/" "$spec"
done
echo "✓ Updated spec files"

# Update Perl module versions
for pm in lib/GRNOC/Simp/*.pm; do
    sed -i "s/our \$VERSION = '[^']*';/our \$VERSION = '$NEW_VERSION';/" "$pm"
done
echo "✓ Updated Perl modules"

echo ""
echo "Version updated to $NEW_VERSION successfully!"
echo "Files modified:"
echo "  - Makefile"
echo "  - spec/*.spec"
echo "  - lib/GRNOC/Simp/*.pm"
echo ""
echo "Please review changes with: git diff"
